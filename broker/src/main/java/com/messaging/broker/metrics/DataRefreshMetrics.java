package com.messaging.broker.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics for DataRefresh workflow monitoring
 *
 * Tracks:
 * - Refresh lifecycle (start, complete, duration)
 * - Consumer ACK timings (RESET, READY)
 * - Data transfer metrics (bytes, messages, speed)
 * - Per-topic and per-consumer granularity
 */
@Singleton
public class DataRefreshMetrics {
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

    // State tracking
    private final Map<String, Long> refreshStartTimes;
    private final Map<String, Long> resetSentTimes;
    private final Map<String, Long> resetAckTimes;
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

        this.refreshStartTimes = new ConcurrentHashMap<>();
        this.resetSentTimes = new ConcurrentHashMap<>();
        this.resetAckTimes = new ConcurrentHashMap<>();
        this.replayStartTimes = new ConcurrentHashMap<>();
    }

    /**
     * Record refresh workflow started
     */
    public void recordRefreshStarted(String topic, String refreshType) {
        refreshStartedTotal.increment();
        long startTimeMs = System.currentTimeMillis();
        refreshStartTimes.put(topic, startTimeMs);

        // Record start timestamp as gauge (in seconds since epoch)
        String key = topic + ":" + refreshType;
        AtomicDouble startTimeValue = refreshStartTimeValues.computeIfAbsent(key, k -> {
            AtomicDouble atomicTime = new AtomicDouble(0.0);
            refreshStartTimeGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_start_time_seconds", atomicTime, AtomicDouble::get)
                            .description("Timestamp when refresh started (seconds since epoch)")
                            .tag("topic", topic)
                            .tag("refresh_type", refreshType)
                            .register(registry)
            );
            return atomicTime;
        });
        startTimeValue.set(startTimeMs / 1000.0);
    }

    /**
     * Record refresh workflow completed
     */
    public void recordRefreshCompleted(String topic, String refreshType, String status) {
        String key = topic + ":" + refreshType + ":" + status;

        Counter counter = refreshCompletedCounters.computeIfAbsent(key, k ->
                Counter.builder("data_refresh_completed_total")
                        .description("Total number of data refresh workflows completed")
                        .tag("topic", topic)
                        .tag("refresh_type", refreshType)
                        .tag("status", status)
                        .register(registry)
        );
        counter.increment();

        // Record end timestamp as gauge (in seconds since epoch)
        long endTimeMs = System.currentTimeMillis();
        String gaugeKey = topic + ":" + refreshType;
        AtomicDouble endTimeValue = refreshEndTimeValues.computeIfAbsent(gaugeKey, k -> {
            AtomicDouble atomicTime = new AtomicDouble(0.0);
            refreshEndTimeGauges.computeIfAbsent(gaugeKey, gk ->
                    Gauge.builder("data_refresh_end_time_seconds", atomicTime, AtomicDouble::get)
                            .description("Timestamp when refresh ended (seconds since epoch)")
                            .tag("topic", topic)
                            .tag("refresh_type", refreshType)
                            .register(registry)
            );
            return atomicTime;
        });
        endTimeValue.set(endTimeMs / 1000.0);

        // Record total duration
        Long startTime = refreshStartTimes.remove(topic);
        if (startTime != null) {
            long durationMs = endTimeMs - startTime;
            recordRefreshDuration(topic, refreshType, durationMs);
        }
    }

    /**
     * Record total refresh duration
     */
    private void recordRefreshDuration(String topic, String refreshType, long durationMs) {
        String key = topic + ":" + refreshType;

        Timer timer = refreshDurationTimers.computeIfAbsent(key, k ->
                Timer.builder("data_refresh_duration_seconds")
                        .description("Duration of data refresh workflow")
                        .tag("topic", topic)
                        .tag("refresh_type", refreshType)
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
    public void recordResetSent(String topic, String consumer) {
        String key = topic + ":" + consumer;
        resetSentTimes.put(key, System.currentTimeMillis());
    }

    /**
     * Record RESET ACK received from consumer
     */
    public void recordResetAckReceived(String topic, String consumer) {
        String key = topic + ":" + consumer;

        Long resetSentTime = resetSentTimes.get(key);
        if (resetSentTime != null) {
            long durationMs = System.currentTimeMillis() - resetSentTime;

            Timer timer = resetAckDurationTimers.computeIfAbsent(key, k ->
                    Timer.builder("data_refresh_reset_ack_duration_seconds")
                            .description("Time taken for consumer to ACK RESET message")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
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
            resetAckTimes.put(key, System.currentTimeMillis());
        }
    }

    /**
     * Record replay started for consumer
     */
    public void recordReplayStarted(String topic, String consumer) {
        String key = topic + ":" + consumer;
        replayStartTimes.put(key, System.currentTimeMillis());
    }

    /**
     * Record READY ACK received from consumer
     */
    public void recordReadyAckReceived(String topic, String consumer) {
        String key = topic + ":" + consumer;

        // Measure time from RESET ACK to READY ACK (total replay time)
        Long resetAckTime = resetAckTimes.get(key);
        if (resetAckTime != null) {
            long durationMs = System.currentTimeMillis() - resetAckTime;

            Timer timer = readyAckDurationTimers.computeIfAbsent(key, k ->
                    Timer.builder("data_refresh_ready_ack_duration_seconds")
                            .description("Time taken from RESET ACK to READY ACK (replay duration)")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
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
        Long replayStartTime = replayStartTimes.remove(key);
        if (replayStartTime != null) {
            long durationMs = System.currentTimeMillis() - replayStartTime;

            Timer timer = replayDurationTimers.computeIfAbsent(key, k ->
                    Timer.builder("data_refresh_replay_duration_seconds")
                            .description("Pure replay duration (from replay start to READY ACK)")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
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

        // Cleanup
        resetSentTimes.remove(key);
        resetAckTimes.remove(key);
    }

    /**
     * Record data transferred during replay
     */
    public void recordDataTransferred(String topic, String consumer, long bytes, long messages) {
        String key = topic + ":" + consumer;

        // Bytes gauge (resettable)
        AtomicLong bytesValue = bytesTransferredValues.computeIfAbsent(key, k -> {
            AtomicLong atomicBytes = new AtomicLong(0);
            bytesTransferredGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_bytes_transferred_total", atomicBytes, AtomicLong::get)
                            .description("Total bytes transferred during current data refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
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
                            .register(registry)
            );
            return atomicMessages;
        });
        messagesValue.addAndGet(messages);
    }

    /**
     * Update transfer rate (bytes per second)
     */
    public void updateTransferRate(String topic, String consumer, double bytesPerSecond) {
        String key = topic + ":" + consumer;

        AtomicDouble rate = transferRates.computeIfAbsent(key, k -> {
            AtomicDouble atomicRate = new AtomicDouble(0.0);

            // Create gauge only once
            transferRateGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("data_refresh_transfer_rate_bytes_per_second", atomicRate, AtomicDouble::get)
                            .description("Current data transfer rate during refresh")
                            .tag("topic", topic)
                            .tag("consumer", consumer)
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
     * Helper class for atomic long gauge
     */
    private static class AtomicLong {
        private volatile long value;

        AtomicLong(long initialValue) {
            this.value = initialValue;
        }

        void set(long newValue) {
            this.value = newValue;
        }

        long addAndGet(long delta) {
            this.value += delta;
            return this.value;
        }

        double get() {
            return value;
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
        refreshStartTimes.remove(topic);

        // Clear consumer-specific state
        resetSentTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
        resetAckTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
        replayStartTimes.keySet().removeIf(k -> k.startsWith(topic + ":"));
    }
}
