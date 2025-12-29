package com.messaging.pipe.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Metrics for pipe performance monitoring
 * Tracks latency and throughput of data fetching from upstream broker
 */
@Singleton
public class PipeMetrics {
    private static final Logger log = LoggerFactory.getLogger(PipeMetrics.class);

    private final Timer pipeFetchLatency;
    private final Counter pipeMessagesReceived;
    private final Counter pipeFetchErrors;
    private final Counter pipeFetchEmpty;

    public PipeMetrics(MeterRegistry registry) {
        // Pipe fetch latency - how long does it take to fetch from upstream
        this.pipeFetchLatency = Timer.builder("pipe_fetch_latency_seconds")
                .description("HTTP latency when fetching from upstream broker")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofSeconds(5))
                .register(registry);

        // Count of messages received from pipe
        this.pipeMessagesReceived = Counter.builder("pipe_messages_received_total")
                .description("Total number of messages received from upstream pipe")
                .register(registry);

        // Count of fetch errors
        this.pipeFetchErrors = Counter.builder("pipe_fetch_errors_total")
                .description("Total number of errors when fetching from pipe")
                .register(registry);

        // Count of empty fetches (no data available)
        this.pipeFetchEmpty = Counter.builder("pipe_fetch_empty_total")
                .description("Total number of fetch calls that returned no data")
                .register(registry);

        log.info("PipeMetrics initialized");
    }

    /**
     * Start timer for pipe fetch operation
     */
    public Timer.Sample startFetchTimer() {
        return Timer.start();
    }

    /**
     * Stop timer and record pipe fetch latency
     */
    public void recordFetchLatency(Timer.Sample sample) {
        sample.stop(pipeFetchLatency);
    }

    /**
     * Record messages received from pipe
     */
    public void recordMessagesReceived(int count) {
        pipeMessagesReceived.increment(count);
    }

    /**
     * Record fetch error
     */
    public void recordFetchError() {
        pipeFetchErrors.increment();
    }

    /**
     * Record empty fetch (no data available)
     */
    public void recordEmptyFetch() {
        pipeFetchEmpty.increment();
    }
}
