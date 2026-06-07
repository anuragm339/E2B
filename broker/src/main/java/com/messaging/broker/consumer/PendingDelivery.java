package com.messaging.broker.consumer;

/**
 * Immutable snapshot claimed by either the ACK path or the timeout path.
 */
public record PendingDelivery(
        long generation,
        long originalOffset,
        long pendingOffset,
        Long fromOffset,
        Long sendTime,
        String traceId) {
}
