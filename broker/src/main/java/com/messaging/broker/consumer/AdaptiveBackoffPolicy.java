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
            @Value("${broker.delivery.min-poll-delay-ms:1}") long minDelayMs,
            @Value("${broker.delivery.max-poll-delay-ms:1000}") long maxDelayMs) {
        this.minDelayMs = minDelayMs;
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
