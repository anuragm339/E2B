package com.messaging.broker.consumer;

import com.messaging.broker.consumer.DeliveryRetryPolicy;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;

/**
 * Adaptive exponential backoff policy.
 *
 * - Data found: Reset to minimum delay (1ms) for near-push latency
 * - No data: Exponential backoff (double delay) up to maximum (1s)
 */
@Singleton
public class AdaptiveBackoffPolicy implements DeliveryRetryPolicy {
    private final long minDelayMs;
    private final long maxDelayMs;

    public AdaptiveBackoffPolicy(
            @Value("${broker.consumer.adaptive-polling.min-delay-ms}") long minDelayMs,
            @Value("${broker.consumer.adaptive-polling.max-delay-ms}") long maxDelayMs) {
        this.minDelayMs = Math.max(1, minDelayMs);
        this.maxDelayMs = maxDelayMs;
    }

    @Override
    public long calculateNextDelay(long currentDelayMs, boolean dataFound) {
        if (dataFound) {
            // Data found → reset to minimum delay for immediate poll
            return minDelayMs;
        } else {
            // No data → exponential backoff (double delay, cap at max)
            return Math.min(currentDelayMs * 2, maxDelayMs);
        }
    }

    @Override
    public long getInitialDelay() {
        return minDelayMs;
    }
}
