package com.messaging.broker.monitoring;

import com.messaging.broker.consumer.RefreshContext;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics for refresh workflow monitoring.
 *
 * Tracks refresh lifecycle, ACK timings, transfer metrics, and
 * per-topic/per-consumer detail.
 */
@Singleton
public class DataRefreshMetrics {
    private static final Logger log = LoggerFactory.getLogger(DataRefreshMetrics.class);
    private final MeterRegistry registry;

    // Counters
    private final Counter refreshStartedTotal;
    private final Map<String, Counter> refreshCompletedCounters;

    // Gauges for resettable metrics (bytes and messages transferred)
    private final Map<String, Gauge> bytesTransferredGauges;
    private final Map<String, AtomicLong> bytesTransferredValues;
    private final Map<String, Gauge> messagesTransferredGauges;
    private final Map<String, AtomicLong> messagesTransferredValues;

    // Timers (Histograms)
    private final Map<String, Timer> refreshDurationTimers;
    private final Map<String, Timer> resetAckDurationTimers;
    private final Map<String, Timer> readyAckDurationTimers;
    private final Map<String, Timer> replayDurationTimers;

    // Gauges
    private final Map<String, Gauge> transferRateGauges;
    private final Map<String, AtomicDouble> transferRates;
    private final Map<String, Gauge> refreshStartTimeGauges;
    private final Map<String, AtomicDouble> refreshStartTimeValues;
    private final Map<String, Gauge> refreshEndTimeGauges;
    private final Map<String, AtomicDouble> refreshEndTimeValues;
    private final Map<String, Gauge> downtimeGauges;
    private final Map<String, AtomicDouble> downtimeValues;
    private final Map<String, Gauge> activeProcessingTimeGauges;
    private final Map<String, AtomicDouble> activeProcessingTimeValues;

    // State tracking
    private final Map<String, Long> refreshStartTimes;
    private final Map<String, Long> resetSentTimes;
    private final Map<String, Long> resetAckTimes;
    private final Map<String, Long> readySentTimes;
    private final Map<String, Long> replayStartTimes;

    public DataRefreshMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.refreshStartedTotal = Counter.builder("data_refresh_started_total")
                .description("Total number of data refresh workflows started")
                .register(registry);

        this.refreshCompletedCounters = new ConcurrentHashMap<>();
        this.bytesTransferredGauges = new ConcurrentHashMap<>();
        this.bytesTransferredValues = new ConcurrentHashMap<>();
        this.messagesTransferredGauges = new ConcurrentHashMap<>();
        this.messagesTransferredValues = new ConcurrentHashMap<>();
        this.refreshDurationTimers = new ConcurrentHashMap<>();
        this.resetAckDurationTimers = new ConcurrentHashMap<>();
        this.readyAckDurationTimers = new ConcurrentHashMap<>();
        this.replayDurationTimers = new ConcurrentHashMap<>();
        this.transferRateGauges = new ConcurrentHashMap<>();
        this.transferRates = new ConcurrentHashMap<>();
        this.refreshStartTimeGauges = new ConcurrentHashMap<>();
        this.refreshStartTimeValues = new ConcurrentHashMap<>();
        this.refreshEndTimeGauges = new ConcurrentHashMap<>();
        this.refreshEndTimeValues = new ConcurrentHashMap<>();
        this.downtimeGauges = new ConcurrentHashMap<>();
        this.downtimeValues = new ConcurrentHashMap<>();
        this.activeProcessingTimeGauges = new ConcurrentHashMap<>();
        this.activeProcessingTimeValues = new ConcurrentHashMap<>();

        this.refreshStartTimes = new ConcurrentHashMap<>();
        this.resetSentTimes = new ConcurrentHashMap<>();
        this.resetAckTimes = new ConcurrentHashMap<>();
        this.readySentTimes = new ConcurrentHashMap<>();
        this.replayStartTimes = new ConcurrentHashMap<>();
    }

    /**
     * Record refresh workflow started
     */
    public void recordRefreshStarted(String topic, String refreshType, String refreshId) {
        refreshStartedTotal.increment();
        long startTimeMs = System.currentTimeMillis();
        String stateKey = topic + ":" + refreshId;
        refreshStartTimes.put(stateKey, startTimeMs);

        // Use refresh_id to preserve per-refresh history in Prometheus.
        String key = topic + ":" + refreshType + ":" + refreshId;
        AtomicDouble startTimeValue = refreshStartTimeValues.computeIfAbsent(key, k -> {
            AtomicDouble atomicTime = new AtomicDouble(0.0);
            refreshStartTimeGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_start_time_seconds", atomicTime, AtomicDouble::get)
                            .description("Timestamp when refresh started (seconds since epoch)")
                            .tag("topic", topic)
                            .tag("refresh_type", refreshType)
                            .tag("refresh_id", refreshId)
                            .register(registry)
            );
            return atomicTime;
        });
        startTimeValue.set(startTimeMs / 1000.0);
    }

    /**
     * Record refresh workflow completed
     */
    public void recordRefreshCompleted(String topic, String refreshType, String status, String refreshId, RefreshContext context) {
        // Use refresh_id to preserve per-refresh history in Prometheus.
        String key = topic + ":" + refreshType + ":" + status + ":" + refreshId;

        Counter counter = refreshCompletedCounters.computeIfAbsent(key, k ->
                Counter.builder("data_refresh_completed_total")
                        .description("Total number of data refresh workflows completed")
                        .tag("topic", topic)
                        .tag("refresh_type", refreshType)
                        .tag("status", status)
                        .tag("refresh_id", refreshId)
                        .register(registry)
        );
        counter.increment();

        // Record end timestamp as gauge (in seconds since epoch)
        long endTimeMs = System.currentTimeMillis();
        // Include refresh_id to preserve per-refresh history
        String gaugeKey = topic + ":" + refreshType + ":" + refreshId;
        AtomicDouble endTimeValue = refreshEndTimeValues.computeIfAbsent(gaugeKey, k -> {
            AtomicDouble atomicTime = new AtomicDouble(0.0);
            refreshEndTimeGauges.computeIfAbsent(gaugeKey, gk ->
                    Gauge.builder("data_refresh_end_time_seconds", atomicTime, AtomicDouble::get)
                            .description("Timestamp when refresh ended (seconds since epoch)")
                            .tag("topic", topic)
                            .tag("refresh_type", refreshType)
                            .tag("refresh_id", refreshId)
                            .register(registry)
            );
            return atomicTime;
        });
        endTimeValue.set(endTimeMs / 1000.0);

        // Calculate downtime
        long totalDowntimeSeconds = context.getTotalDowntimeSeconds();

        // Record downtime gauge
        AtomicDouble downtimeValue = downtimeValues.computeIfAbsent(gaugeKey, k -> {
            AtomicDouble atomicDowntime = new AtomicDouble(0.0);
            downtimeGauges.computeIfAbsent(gaugeKey, gk ->
                Gauge.builder("data_refresh_downtime_seconds", atomicDowntime, AtomicDouble::get)
                    .description("Total downtime during refresh (broker offline)")
                    .tag("topic", topic)
                    .tag("refresh_type", refreshType)
                    .tag("refresh_id", refreshId)
                    .register(registry)
            );
            return atomicDowntime;
        });
        downtimeValue.set(totalDowntimeSeconds);

        // Calculate active processing time using actual end time (not "now")
        long endTimeSeconds = endTimeMs / 1000;
        long startTimeSeconds = context.getStartTime().getEpochSecond();
        long totalDurationSeconds = endTimeSeconds - startTimeSeconds;
        long activeProcessingSeconds = totalDurationSeconds - totalDowntimeSeconds;

        // Record active processing time gauge
        AtomicDouble activeTimeValue = activeProcessingTimeValues.computeIfAbsent(gaugeKey, k -> {
            AtomicDouble atomicTime = new AtomicDouble(0.0);
            activeProcessingTimeGauges.computeIfAbsent(gaugeKey, gk ->
                Gauge.builder("data_refresh_active_processing_seconds", atomicTime, AtomicDouble::get)
                    .description("Active processing time during refresh (excluding downtime)")
                    .tag("topic", topic)
                    .tag("refresh_type", refreshType)
                    .tag("refresh_id", refreshId)
                    .register(registry)
            );
            return atomicTime;
        });
        activeTimeValue.set(activeProcessingSeconds);

        log.info("Refresh metrics for {}: total={}s, downtime={}s, active={}s",
                 topic, totalDurationSeconds, totalDowntimeSeconds, activeProcessingSeconds);

        // Record total duration
        String stateKey = topic + ":" + refreshId;
        Long startTime = refreshStartTimes.remove(stateKey);
        if (startTime != null) {
            long durationMs = endTimeMs - startTime;
            recordRefreshDuration(topic, refreshType, refreshId, durationMs);
        }
    }

    /**
     * Record total refresh duration
     */
    private void recordRefreshDuration(String topic, String refreshType, String refreshId, long durationMs) {
        String key = topic + ":" + refreshType + ":" + refreshId;

        Timer timer = refreshDurationTimers.computeIfAbsent(key, k ->
                Timer.builder("data_refresh_duration_seconds")
                        .description("Duration of data refresh workflow")
                        .tag("topic", topic)
                        .tag("refresh_type", refreshType)
                        .tag("refresh_id", refreshId)
                        .publishPercentileHistogram()
                        .serviceLevelObjectives(
                            Duration.ofMillis(100),
                            Duration.ofMillis(500),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(10),
                            Duration.ofSeconds(30),
                            Duration.ofSeconds(60)
                        )
                        .register(registry)
        );
        timer.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Record RESET message sent to consumer
     */
    public void recordResetSent(String topic, String consumer, String refreshId) {
        String key = topic + ":" + consumer + ":" + refreshId;
        resetSentTimes.put(key, System.currentTimeMillis());
    }

    /**
     * Record RESET message sent at a specific time (for resume-after-restart).
     * B9-1 fix: resumeRefresh() must record the original RESET sent time, not System.currentTimeMillis(),
     * so that ACK duration measured after resume reflects actual consumer response time.
     */
    public void recordResetSentAt(String topic, String consumer, String refreshId, long sentTimeMs) {
        String key = topic + ":" + consumer + ":" + refreshId;
        resetSentTimes.put(key, sentTimeMs);
    }

    /**
     * Record READY message sent to consumer
     */
    public void recordReadySent(String topic, String consumer, String refreshId) {
        String key = topic + ":" + consumer + ":" + refreshId;
        readySentTimes.put(key, System.currentTimeMillis());
    }

    /**
     * Record READY message sent at a specific time (for resume-after-restart).
     * B9-1 fix: analogous to recordResetSentAt — preserves original READY sent time.
     */
    public void recordReadySentAt(String topic, String consumer, String refreshId, long sentTimeMs) {
        String key = topic + ":" + consumer + ":" + refreshId;
        readySentTimes.put(key, sentTimeMs);
    }

    /**
     * Record RESET ACK received from consumer
     */
    public void recordResetAckReceived(String topic, String consumer, String refreshId) {
        // Internal state key still uses refreshId to distinguish concurrent refreshes
        String stateKey = topic + ":" + consumer + ":" + refreshId;
        String timerKey = topic + ":" + consumer + ":" + refreshId;

        Long resetSentTime = resetSentTimes.get(stateKey);
        if (resetSentTime != null) {
            long durationMs = System.currentTimeMillis() - resetSentTime;

            Timer timer = resetAckDurationTimers.computeIfAbsent(timerKey, k ->
                    Timer.builder("data_refresh_reset_ack_duration_seconds")
                            .description("Time taken for consumer to ACK RESET message")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_id", refreshId)
                            .publishPercentileHistogram()
                            .serviceLevelObjectives(
                                Duration.ofMillis(10),
                                Duration.ofMillis(50),
                                Duration.ofMillis(100),
                                Duration.ofMillis(500),
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(2)
                            )
                            .register(registry)
            );
            timer.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            // Track when RESET ACK was received for replay duration
            resetAckTimes.put(stateKey, System.currentTimeMillis());
        }
    }

    /**
     * Record RESET ACK duration from persisted timestamps (for resume after restart)
     */
    public void recordResetAckDuration(String topic, String consumer, String refreshId, long durationMs) {
        String timerKey = topic + ":" + consumer + ":" + refreshId;

        Timer timer = resetAckDurationTimers.computeIfAbsent(timerKey, k ->
                Timer.builder("data_refresh_reset_ack_duration_seconds")
                        .description("Time taken for consumer to ACK RESET message")
                        .tag("topic", topic)
                        .tag("consumer", consumer)
                        .tag("refresh_id", refreshId)
                        .publishPercentileHistogram()
                        .serviceLevelObjectives(
                            Duration.ofMillis(10),
                            Duration.ofMillis(50),
                            Duration.ofMillis(100),
                            Duration.ofMillis(500),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(2)
                        )
                        .register(registry)
        );
        timer.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Record replay started for consumer
     */
    public void recordReplayStarted(String topic, String consumer, String refreshId) {
        String key = topic + ":" + consumer + ":" + refreshId;
        replayStartTimes.put(key, System.currentTimeMillis());
    }

    /**
     * Record READY ACK received from consumer
     */
    public void recordReadyAckReceived(String topic, String consumer, String refreshId) {
        // Internal state key still uses refreshId to distinguish concurrent refreshes
        String stateKey = topic + ":" + consumer + ":" + refreshId;
        String timerKey = topic + ":" + consumer + ":" + refreshId;

        // Measure time from READY sent to READY ACK received
        Long readySentTime = readySentTimes.get(stateKey);
        if (readySentTime != null) {
            long durationMs = System.currentTimeMillis() - readySentTime;

            Timer timer = readyAckDurationTimers.computeIfAbsent(timerKey, k ->
                    Timer.builder("data_refresh_ready_ack_duration_seconds")
                            .description("Time taken from READY sent to READY ACK received")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_id", refreshId)
                            .publishPercentileHistogram()
                            .serviceLevelObjectives(
                                Duration.ofMillis(100),
                                Duration.ofMillis(500),
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(60),
                                Duration.ofSeconds(120)
                            )
                            .register(registry)
            );
            timer.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        // Measure pure replay duration (from replay start to READY ACK)
        Long replayStartTime = replayStartTimes.remove(stateKey);
        if (replayStartTime != null) {
            long durationMs = System.currentTimeMillis() - replayStartTime;

            Timer timer = replayDurationTimers.computeIfAbsent(timerKey, k ->
                    Timer.builder("data_refresh_replay_duration_seconds")
                            .description("Pure replay duration (from replay start to READY ACK)")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_id", refreshId)
                            .publishPercentileHistogram()
                            .serviceLevelObjectives(
                                Duration.ofMillis(100),
                                Duration.ofMillis(500),
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(60),
                                Duration.ofSeconds(120)
                            )
                            .register(registry)
            );
            timer.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        // Cleanup — B5-3 fix: also remove readySentTimes entry to prevent memory leak.
        // Previously only resetSentTimes and resetAckTimes were cleaned up here;
        // readySentTimes accumulated one entry per refresh run indefinitely.
        resetSentTimes.remove(stateKey);
        resetAckTimes.remove(stateKey);
        readySentTimes.remove(stateKey);
    }

    /**
     * Record READY ACK duration from persisted timestamps (for resume after restart)
     */
    public void recordReadyAckDuration(String topic, String consumer, String refreshId, long durationMs) {
        String timerKey = topic + ":" + consumer + ":" + refreshId;

        Timer timer = readyAckDurationTimers.computeIfAbsent(timerKey, k ->
                Timer.builder("data_refresh_ready_ack_duration_seconds")
                        .description("Time taken from READY sent to READY ACK received")
                        .tag("topic", topic)
                        .tag("consumer", consumer)
                        .tag("refresh_id", refreshId)
                        .publishPercentileHistogram()
                        .serviceLevelObjectives(
                            Duration.ofMillis(100),
                            Duration.ofMillis(500),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(10),
                            Duration.ofSeconds(30),
                            Duration.ofSeconds(60),
                            Duration.ofSeconds(120)
                        )
                        .register(registry)
        );
        timer.record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Initialize transfer metrics to 0 for a consumer at the start of replay.
     *
     * Called when a consumer ACKs RESET so the gauge exists immediately — even
     * when the topic has no data to replay. Without this, the gauge is never
     * created for empty topics and Prometheus/Grafana shows stale values from
     * prior refreshes instead of 0.
     */
    public void initializeTransferMetrics(String topic, String consumer, String refreshType) {
        String key = topic + ":" + consumer + ":" + refreshType;

        AtomicLong bytesValue = bytesTransferredValues.computeIfAbsent(key, k -> {
            AtomicLong atomicBytes = new AtomicLong(0);
            bytesTransferredGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_bytes_transferred_total", atomicBytes, AtomicLong::get)
                            .description("Total bytes transferred during current data refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_type", refreshType)
                            .baseUnit("bytes")
                            .register(registry)
            );
            return atomicBytes;
        });
        bytesValue.set(0);

        AtomicLong messagesValue = messagesTransferredValues.computeIfAbsent(key, k -> {
            AtomicLong atomicMessages = new AtomicLong(0);
            messagesTransferredGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_messages_transferred_total", atomicMessages, AtomicLong::get)
                            .description("Total messages transferred during current data refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_type", refreshType)
                            .register(registry)
            );
            return atomicMessages;
        });
        messagesValue.set(0);
    }

    /**
     * Record data transferred during replay.
     *
     * Uses a stable gauge key (topic:consumer:refreshType) rather than including
     * refreshId. Including refreshId created a new Prometheus time series per
     * refresh run — old zeroed series persisted alongside the current one,
     * causing dashboards to show incorrect aggregated values.
     */
    public void recordDataTransferred(String topic, String consumer, long bytes, long messages, String refreshId, String refreshType) {
        // Stable key: no refreshId — one gauge per topic/consumer/type, reset each refresh
        String key = topic + ":" + consumer + ":" + refreshType;

        // Bytes gauge (resettable)
        AtomicLong bytesValue = bytesTransferredValues.computeIfAbsent(key, k -> {
            AtomicLong atomicBytes = new AtomicLong(0);
            bytesTransferredGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_bytes_transferred_total", atomicBytes, AtomicLong::get)
                            .description("Total bytes transferred during current data refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_type", refreshType)
                            .baseUnit("bytes")
                            .register(registry)
            );
            return atomicBytes;
        });
        bytesValue.addAndGet(bytes);

        // Messages gauge (resettable)
        AtomicLong messagesValue = messagesTransferredValues.computeIfAbsent(key, k -> {
            AtomicLong atomicMessages = new AtomicLong(0);
            messagesTransferredGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_messages_transferred_total", atomicMessages, AtomicLong::get)
                            .description("Total messages transferred during current data refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_type", refreshType)
                            .register(registry)
            );
            return atomicMessages;
        });
        messagesValue.addAndGet(messages);
    }

    /**
     * Update transfer rate (bytes per second)
     */
    public void updateTransferRate(String topic, String consumer, double bytesPerSecond, String refreshId) {
        String key = topic + ":" + consumer + ":" + refreshId;

        AtomicDouble rate = transferRates.computeIfAbsent(key, k -> {
            AtomicDouble atomicRate = new AtomicDouble(0.0);

            // Create gauge only once
            transferRateGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_transfer_rate_bytes_per_second", atomicRate, AtomicDouble::get)
                            .description("Current data transfer rate during refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
                            .tag("refresh_id", refreshId)
                            .baseUnit("bytes_per_second")
                            .register(registry)
            );

            return atomicRate;
        });

        rate.set(bytesPerSecond);
    }

    /**
     * Helper class for atomic double gauge
     */
    private static class AtomicDouble {
        private volatile double value;

        AtomicDouble(double initialValue) {
            this.value = initialValue;
        }

        void set(double newValue) {
            this.value = newValue;
        }

        double get() {
            return value;
        }
    }

    /**
     * Helper class for atomic long gauge.
     * B2-5 fix: was using volatile long + non-atomic compound += which loses concurrent increments.
     * Now backed by java.util.concurrent.atomic.AtomicLong for correct CAS-based addAndGet().
     */
    private static class AtomicLong {
        private final java.util.concurrent.atomic.AtomicLong value;

        AtomicLong(long initialValue) {
            this.value = new java.util.concurrent.atomic.AtomicLong(initialValue);
        }

        void set(long newValue) {
            this.value.set(newValue);
        }

        long addAndGet(long delta) {
            return this.value.addAndGet(delta);
        }

        double get() {
            return value.get();
        }
    }

    /**
     * Reset all metrics for a new refresh operation
     * This allows each refresh to show independent metrics in the dashboard
     * while Prometheus retains historical data for time-series queries
     */
    public void resetMetricsForNewRefresh() {
        // Reset all bytes transferred gauges to 0
        bytesTransferredValues.values().forEach(v -> v.set(0));

        // Reset all messages transferred gauges to 0
        messagesTransferredValues.values().forEach(v -> v.set(0));

        // Reset all start time gauges to 0
        refreshStartTimeValues.values().forEach(v -> v.set(0.0));

        // Reset all end time gauges to 0
        refreshEndTimeValues.values().forEach(v -> v.set(0.0));

        // Reset transfer rates to 0
        transferRates.values().forEach(v -> v.set(0.0));

        // Reset downtime gauges to 0
        downtimeValues.values().forEach(v -> v.set(0.0));

        // Reset active processing time gauges to 0
        activeProcessingTimeValues.values().forEach(v -> v.set(0.0));

        // Unregister and clear all Timer instances to reset histogram data
        // Timers must be unregistered from the registry because Micrometer
        // doesn't provide a way to reset accumulated histogram samples

        // Unregister RESET ACK duration timers
        resetAckDurationTimers.values().forEach(timer -> registry.remove(timer));
        resetAckDurationTimers.clear();

        // Unregister READY ACK duration timers
        readyAckDurationTimers.values().forEach(timer -> registry.remove(timer));
        readyAckDurationTimers.clear();

        // Unregister replay duration timers
        replayDurationTimers.values().forEach(timer -> registry.remove(timer));
        replayDurationTimers.clear();

        // Unregister refresh duration timers
        refreshDurationTimers.values().forEach(timer -> registry.remove(timer));
        refreshDurationTimers.clear();

        // Clear state tracking maps
        refreshStartTimes.clear();
        resetSentTimes.clear();
        resetAckTimes.clear();
        replayStartTimes.clear();
    }

    /**
     * Clear all state for a topic (called when refresh completes)
     */
    public void clearTopicState(String topic) {
        refreshStartTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));

        // Clear consumer-specific state — B5-3 fix: include readySentTimes
        resetSentTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
        resetAckTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
        readySentTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
        replayStartTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
    }

    /**
     * OOM FIX: Remove all timing data for a specific consumer on disconnect
     *
     * Prevents memory leak from accumulating per-consumer timing maps when
     * consumers reconnect with different ephemeral ports.
     *
     * @param topic Topic name
     * @param consumer Consumer identifier (may include ephemeral port)
     */
    public void removeConsumerTimingData(String topic, String consumer) {
        String keyPrefix = topic + ":" + consumer;
        int removedCount = 0;

        // Remove timing state maps (keyed by topic:consumer or topic:consumer:refreshId)
        removedCount += resetSentTimes.keySet().removeIf(k -> k.startsWith(keyPrefix)) ? 1 : 0;
        removedCount += resetAckTimes.keySet().removeIf(k -> k.startsWith(keyPrefix)) ? 1 : 0;
        removedCount += readySentTimes.keySet().removeIf(k -> k.startsWith(keyPrefix)) ? 1 : 0;
        removedCount += replayStartTimes.keySet().removeIf(k -> k.startsWith(keyPrefix)) ? 1 : 0;

        // Remove Timer objects (keys are topic:consumer:refreshId)
        removedCount += resetAckDurationTimers.keySet().removeIf(k -> k.startsWith(keyPrefix)) ? 1 : 0;
        removedCount += readyAckDurationTimers.keySet().removeIf(k -> k.startsWith(keyPrefix)) ? 1 : 0;

        // Note: transferRateGauges, bytesTransferredGauges use group:topic keys, not consumer-specific
        // So we don't remove them here (they're cleaned by BrokerMetrics.removeConsumerMetrics)

        if (removedCount > 0) {
            log.info("OOM FIX: Removed {} timing data entries for disconnected consumer: topic={}, consumer={}",
                    removedCount, topic, consumer);
        }
    }
}
