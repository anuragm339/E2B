package com.messaging.broker.compaction;

import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.StorageEngine;
import com.messaging.storage.segment.Segment;
import com.messaging.storage.segment.SegmentAccess;
import com.messaging.storage.segment.SegmentManager;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

/**
 * Scheduled background job that drives Kafka-style incremental log compaction.
 *
 * <p>Each run iterates all known topics, loads the last checkpoint, selects the next
 * dirty window via {@link CompactionPlanner}, rewrites it via {@link CompactionRewriter},
 * advances the checkpoint, and records metrics. Per-topic failures are caught so one bad
 * topic cannot block compaction of the rest.
 */
@Singleton
public class CompactionScheduler {

    private static final Logger log = LoggerFactory.getLogger(CompactionScheduler.class);

    private final StorageEngine storage;
    private final SegmentAccess segmentAccess;
    private final CompactionCheckpointStore checkpointStore;
    private final CompactionPlanner planner;
    private final CompactionRewriter rewriter;
    private final RocksDbCompactionIndex compactionIndex;
    private final BrokerMetrics metrics;
    private final com.messaging.broker.monitoring.MemoryMonitor memoryMonitor;
    private final boolean enabled;
    private final int tombstoneRetentionDays;
    private final int windowSize;
    private final int maxTopicsPerRun;
    private final int minSegmentsPerTopic;
    private final double maxProcessCpuUsage;
    private final double maxHeapUsage;
    private final com.sun.management.OperatingSystemMXBean osBean;

    @Inject
    public CompactionScheduler(
            StorageEngine storage,
            SegmentAccess segmentAccess,
            CompactionCheckpointStore checkpointStore,
            CompactionPlanner planner,
            CompactionRewriter rewriter,
            RocksDbCompactionIndex compactionIndex,
            BrokerMetrics metrics,
            com.messaging.broker.monitoring.MemoryMonitor memoryMonitor,
            @Value("${compaction.enabled:true}") boolean enabled,
            @Value("${compaction.tombstone-retention-days:7}") int tombstoneRetentionDays,
            @Value("${compaction.window-size:10}") int windowSize,
            @Value("${compaction.max-topics-per-run:2147483647}") int maxTopicsPerRun,
            @Value("${compaction.min-segments-per-topic:1}") int minSegmentsPerTopic,
            @Value("${compaction.max-process-cpu-usage:1.0}") double maxProcessCpuUsage,
            @Value("${compaction.max-heap-usage:1.0}") double maxHeapUsage) {
        this.storage               = storage;
        this.segmentAccess         = segmentAccess;
        this.checkpointStore       = checkpointStore;
        this.planner               = planner;
        this.rewriter              = rewriter;
        this.compactionIndex       = compactionIndex;
        this.metrics               = metrics;
        this.memoryMonitor         = memoryMonitor;
        this.enabled               = enabled;
        this.tombstoneRetentionDays = tombstoneRetentionDays;
        this.windowSize            = windowSize;
        this.maxTopicsPerRun       = maxTopicsPerRun;
        this.minSegmentsPerTopic   = minSegmentsPerTopic;
        this.maxProcessCpuUsage    = maxProcessCpuUsage;
        this.maxHeapUsage          = maxHeapUsage;
        java.lang.management.OperatingSystemMXBean rawBean = ManagementFactory.getOperatingSystemMXBean();
        this.osBean = rawBean instanceof com.sun.management.OperatingSystemMXBean
                ? (com.sun.management.OperatingSystemMXBean) rawBean
                : null;
    }

    @Scheduled(
        fixedDelay   = "${compaction.schedule.interval:24h}",
        initialDelay = "${compaction.schedule.initial-delay:5m}")
    public void compact() {
        if (!enabled) {
            log.debug("Compaction disabled, skipping");
            metrics.recordCompactionSkipped("disabled");
            return;
        }

        if (!canRunCompaction("run_start", null)) {
            return;
        }

        Timer.Sample runTimer = metrics.startCompactionTimer();

        Set<String> topics = storage.getTopicNames();
        List<String> orderedTopics = new ArrayList<>(topics);
        Collections.sort(orderedTopics);
        Runtime rt = Runtime.getRuntime();
        long heapUsedMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long heapMaxMB  = rt.maxMemory() / (1024 * 1024);
        log.info("event=compaction_run_start topics={} heap={}/{}MB", topics.size(), heapUsedMB, heapMaxMB);

        int compactedTopics = 0;
        for (String topic : orderedTopics) {
            if (compactedTopics >= maxTopicsPerRun) {
                log.info("event=compaction_run_budget_exhausted compactedTopics={} maxTopicsPerRun={}",
                        compactedTopics, maxTopicsPerRun);
                break;
            }
            if (!canRunCompaction("before_topic", topic)) {
                break;
            }
            try {
                if (compactTopic(topic, 0)) {
                    compactedTopics++;
                }
            } catch (Exception e) {
                log.error("Compaction failed for topic={}", topic, e);
                metrics.markCompactionComplete(topic);   // ensure active flag is cleared on error
                metrics.recordCompactionError(topic);
            }
        }

        if (compactedTopics > 0) {
            metrics.stopCompactionTimer(runTimer);
            metrics.recordCompactionRun();
        }
        long heapUsedAfterMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        log.info("event=compaction_run_finish compactedTopics={} heap={}/{}MB", compactedTopics, heapUsedAfterMB, heapMaxMB);
    }

    private boolean compactTopic(String topic, int partition) throws Exception {
        SegmentManager segmentManager = segmentAccess.getSegmentManager(topic, partition);
        if (segmentManager == null) {
            log.debug("No segment manager for topic={}, skipping", topic);
            return false;
        }

        long lastCheckpoint = checkpointStore.loadCheckpoint(topic, partition);
        List<Segment> sealedSegments = segmentManager.getInactiveSegments();
        if (sealedSegments.size() < minSegmentsPerTopic) {
            log.debug("Compaction skipped for topic={} partition={} sealedSegments={} minSegmentsPerTopic={}",
                    topic, partition, sealedSegments.size(), minSegmentsPerTopic);
            metrics.recordCompactionSkipped("not_enough_sealed_segments");
            return false;
        }
        List<Segment> window = planner.selectDirtyWindow(sealedSegments, lastCheckpoint, windowSize);

        if (window.isEmpty()) {
            log.debug("No dirty segments for topic={} since checkpoint={}", topic, lastCheckpoint);
            metrics.recordCompactionSkipped("no_dirty_segments");
            return false;
        }

        String windowOffsets = window.stream()
                .map(s -> String.valueOf(s.getBaseOffset()))
                .reduce((a, b) -> a + "," + b).orElse("none");
        log.info("event=compaction_topic_start topic={} partition={} segments={} " +
                 "checkpoint={} windowOffsets=[{}]",
                 topic, partition, window.size(), lastCheckpoint, windowOffsets);

        long topicStartMs = System.currentTimeMillis();
        long compactedThroughOffset = window.stream()
                .mapToLong(s -> s.getNextOffset() - 1)
                .max()
                .orElse(-1L);
        metrics.markCompactionActive(topic);
        CompactionRewriter.CompactionResult result;
        try {
            result = rewriter.rewrite(
                    window, topic, partition, segmentManager, compactionIndex, tombstoneRetentionDays);
        } finally {
            metrics.markCompactionComplete(topic);
        }

        compactionIndex.markCompactedThrough(topic, compactedThroughOffset);

        // Only advance the checkpoint when no live tombstones were left behind.
        // If tombstones are still within their retention window, the same window must be
        // re-selected on the next run so they can be removed once they age out.
        long newCheckpoint = lastCheckpoint;
        if (!result.hadUnexpiredTombstones) {
            newCheckpoint = window.stream()
                    .mapToLong(Segment::getBaseOffset)
                    .max()
                    .orElse(lastCheckpoint);
            checkpointStore.saveCheckpoint(topic, partition, newCheckpoint);
        } else {
            log.debug("Skipping checkpoint advancement for topic={} partition={}: unexpired tombstones remain",
                    topic, partition);
        }

        metrics.recordCompactionTopicRun(
                topic,
                result.recordsRemoved,
                result.tombstonesRemoved,
                result.bytesRead,
                result.bytesWritten,
                result.bytesReclaimed,
                result.segmentsReplaced);

        long topicElapsedMs = System.currentTimeMillis() - topicStartMs;
        log.info("event=compaction_topic_finish topic={} removed={} records (tombstones={}) " +
                 "bytesRead={} bytesWritten={} reclaimed={}B segments={} newCheckpoint={} elapsedMs={}",
                topic, result.recordsRemoved, result.tombstonesRemoved,
                result.bytesRead, result.bytesWritten, result.bytesReclaimed,
                result.segmentsReplaced, newCheckpoint, topicElapsedMs);
        return true;
    }

    private boolean canRunCompaction(String phase, String topic) {
        double heapUsage = memoryMonitor.getHeapUsagePercent();
        double processCpu = getProcessCpuUsage();

        if (heapUsage >= maxHeapUsage || memoryMonitor.isMemoryPressureHigh()) {
            metrics.recordCompactionSkipped("memory_pressure");
            log.info("event=compaction_run_skipped phase={} topic={} reason=memory_pressure heapUsage={} maxHeapUsage={} warning={}",
                    phase, topic, heapUsage, maxHeapUsage, memoryMonitor.isMemoryPressureHigh());
            return false;
        }

        if (processCpu >= 0 && processCpu >= maxProcessCpuUsage) {
            metrics.recordCompactionSkipped("cpu_pressure");
            log.info("event=compaction_run_skipped phase={} topic={} reason=cpu_pressure processCpuUsage={} maxProcessCpuUsage={}",
                    phase, topic, processCpu, maxProcessCpuUsage);
            return false;
        }

        return true;
    }

    private double getProcessCpuUsage() {
        if (osBean == null) {
            return -1;
        }
        double load = osBean.getProcessCpuLoad();
        return load >= 0 ? load : -1;
    }
}
