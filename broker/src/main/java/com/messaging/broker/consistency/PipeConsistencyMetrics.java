package com.messaging.broker.consistency;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prometheus metrics for the PipeConsistency feature. Metric names and labels follow
 * the same pattern as BrokerMetrics ({@code pipe_messages_received_total} style).
 *
 * <p>All metric names are part of the operator-facing contract and must not change
 * silently — update {@code provider/docs/PIPE_CONSISTENCY.md} alongside any rename.
 */
@Singleton
public class PipeConsistencyMetrics {

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, Counter> auditTotals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> auditSeconds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> lastTs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> statusGauge = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> throttled = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> firstDivergentHop = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> mismatchSegments = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> missingRecords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> extraRecords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> dataMismatchRecords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> lineageStale = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> deepWalkAborted = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> headClamped = new ConcurrentHashMap<>();

    // Latest-audit gauges — reset on every check so dashboards can show
    // "what does the most recent audit say is broken right now?" without summing counters.
    private final ConcurrentHashMap<String, AtomicLong> latestMissing = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> latestExtra = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> latestDataMismatch = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> latestMismatchSegments = new ConcurrentHashMap<>();

    private final Counter hashCacheHits;
    private final Counter hashCacheMisses;

    public PipeConsistencyMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.hashCacheHits = Counter.builder("pipe_consistency_hash_cache_hit_total").register(registry);
        this.hashCacheMisses = Counter.builder("pipe_consistency_hash_cache_miss_total").register(registry);
    }

    public void recordAuditResult(String result, String mode, String parentNodeId) {
        String key = result + "|" + mode + "|" + (parentNodeId == null ? "" : parentNodeId);
        auditTotals.computeIfAbsent(key, k -> Counter.builder("pipe_consistency_check_total")
                .tags(Tags.of("result", result, "mode", mode, "parent_node_id", parentNodeId == null ? "" : parentNodeId))
                .register(registry)).increment();
    }

    public Timer auditTimer(String topic, String mode) {
        return auditSeconds.computeIfAbsent(topic + "|" + mode, k -> Timer.builder("pipe_consistency_check_seconds")
                .tags(Tags.of("topic", topic, "mode", mode))
                .register(registry));
    }

    public void recordCacheHit() { hashCacheHits.increment(); }
    public void recordCacheMiss() { hashCacheMisses.increment(); }

    public void recordThrottled(String parentNodeId) {
        throttled.computeIfAbsent(parentNodeId == null ? "" : parentNodeId,
                k -> Counter.builder("pipe_consistency_throttled_total")
                        .tags(Tags.of("parent_node_id", k))
                        .register(registry)).increment();
    }

    public void setLastCheckTimestamp(String topic, String mode, long epochSeconds) {
        String key = topic + "|" + mode;
        AtomicLong holder = lastTs.computeIfAbsent(key, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_last_check_timestamp_seconds",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        });
        holder.set(epochSeconds);
    }

    /** Records the first-divergent hop nodeId hash code (a stable but opaque identifier per nodeId). */
    public void setFirstDivergentHop(String topic, String nodeId) {
        AtomicLong holder = firstDivergentHop.computeIfAbsent(topic, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_chain_first_divergent_hop",
                    Tags.of("topic", topic), al, AtomicLong::doubleValue);
            return al;
        });
        holder.set(nodeId == null ? 0L : (long) nodeId.hashCode());
    }

    /**
     * Numeric status per (topic, mode) for dashboard panels. 0=CONSISTENT, 1=MISMATCH,
     * 2=ERROR, 3=LINEAGE_STALE, 4=DEEP_WALK_ABORTED, 5=UPSTREAM_UNSUPPORTED.
     */
    public void setStatus(String topic, String mode, int statusCode) {
        AtomicLong holder = statusGauge.computeIfAbsent(topic + "|" + mode, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_status",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        });
        holder.set(statusCode);
    }

    public void addMismatchSegments(String topic, String mode, long count) {
        // Register the counter even on count=0 so Prometheus has a series to query
        // (otherwise dashboards show "No data" instead of a flat zero line until
        // the first non-zero audit ever).
        Counter c = mismatchSegments.computeIfAbsent(topic + "|" + mode,
                k -> Counter.builder("pipe_consistency_mismatch_segments_total")
                        .tags(Tags.of("topic", topic, "mode", mode))
                        .register(registry));
        if (count > 0) c.increment(count);
    }

    public void addMissingRecords(String topic, String mode, long count) {
        Counter c = missingRecords.computeIfAbsent(topic + "|" + mode,
                k -> Counter.builder("pipe_consistency_missing_records_total")
                        .tags(Tags.of("topic", topic, "mode", mode))
                        .register(registry));
        if (count > 0) c.increment(count);
    }

    public void addExtraRecords(String topic, String mode, long count) {
        Counter c = extraRecords.computeIfAbsent(topic + "|" + mode,
                k -> Counter.builder("pipe_consistency_extra_records_total")
                        .tags(Tags.of("topic", topic, "mode", mode))
                        .register(registry));
        if (count > 0) c.increment(count);
    }

    public void addDataMismatchRecords(String topic, String mode, long count) {
        Counter c = dataMismatchRecords.computeIfAbsent(topic + "|" + mode,
                k -> Counter.builder("pipe_consistency_data_mismatch_records_total")
                        .tags(Tags.of("topic", topic, "mode", mode))
                        .register(registry));
        if (count > 0) c.increment(count);
    }

    public void incrementLineageStale(String topic) {
        lineageStale.computeIfAbsent(topic,
                k -> Counter.builder("pipe_consistency_lineage_stale_total")
                        .tags(Tags.of("topic", topic))
                        .register(registry)).increment();
    }

    public void incrementDeepWalkAborted(String topic) {
        deepWalkAborted.computeIfAbsent(topic,
                k -> Counter.builder("pipe_consistency_deep_walk_aborted_total")
                        .tags(Tags.of("topic", topic))
                        .register(registry)).increment();
    }

    public void setHeadClampedOffsets(String topic, String mode, long offsets) {
        AtomicLong holder = headClamped.computeIfAbsent(topic + "|" + mode, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_head_clamped_offsets",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        });
        holder.set(offsets);
    }

    /**
     * Set the "what was the most recent audit's breakdown" gauges — overwritten
     * on every check so dashboards can show the current mismatch shape without
     * having to do rate/delta math on counters.
     */
    public void setLatestBreakdown(String topic, String mode,
                                    long missingOnBroker,
                                    long extraOnBroker,
                                    long dataMismatch,
                                    long mismatchSegments) {
        String key = topic + "|" + mode;
        latestMissing.computeIfAbsent(key, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_missing_records_latest",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        }).set(missingOnBroker);

        latestExtra.computeIfAbsent(key, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_extra_records_latest",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        }).set(extraOnBroker);

        latestDataMismatch.computeIfAbsent(key, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_data_mismatch_records_latest",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        }).set(dataMismatch);

        latestMismatchSegments.computeIfAbsent(key, k -> {
            AtomicLong al = new AtomicLong();
            registry.gauge("pipe_consistency_mismatch_segments_latest",
                    Tags.of("topic", topic, "mode", mode), al, AtomicLong::doubleValue);
            return al;
        }).set(mismatchSegments);
    }
}
