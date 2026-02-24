package com.messaging.network.metrics;

/**
 * Records consumer-side decode errors.
 * Implementations may be no-op or backed by a metrics registry.
 */
@FunctionalInterface
public interface DecodeErrorRecorder {
    void record(String topic, String reason);

    static DecodeErrorRecorder noop() {
        return (topic, reason) -> {};
    }
}
