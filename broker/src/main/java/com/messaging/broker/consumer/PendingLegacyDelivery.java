package com.messaging.broker.consumer;

import com.messaging.broker.legacy.MergedBatch;
import io.micrometer.core.instrument.Timer;

/**
 * Immutable state for one legacy batch awaiting an ACK.
 */
public record PendingLegacyDelivery(
        long generation,
        MergedBatch batch,
        Timer.Sample timer,
        long sendTime) {
}
