package com.messaging.network.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer-side decode error metrics backed by Micrometer.
 * This is injected explicitly via client library wiring.
 */
public final class ConsumerDecodeMetrics implements DecodeErrorRecorder {
    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, Counter> decodeErrors = new ConcurrentHashMap<>();

    public ConsumerDecodeMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void record(String topic, String reason) {
        if (registry == null) {
            return;
        }
        String safeTopic = (topic == null || topic.isBlank()) ? "unknown" : topic;
        String safeReason = (reason == null || reason.isBlank()) ? "unknown" : reason;
        String key = safeTopic + ":" + safeReason;

        Counter counter = decodeErrors.computeIfAbsent(key, k ->
                Counter.builder("consumer_decode_errors_total")
                        .description("Total consumer-side decode errors")
                        .tag("topic", safeTopic)
                        .tag("reason", safeReason)
                        .register(registry)
        );
        counter.increment();
    }
}
