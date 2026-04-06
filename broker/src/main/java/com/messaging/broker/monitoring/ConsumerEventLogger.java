package com.messaging.broker.monitoring;

/**
 * Semantic logging interface for consumer events.
 */
public interface ConsumerEventLogger {
    /**
     * Log consumer registered successfully.
     *
     * @param context Consumer context (clientId, topic, group, offset)
     */
    void logConsumerRegistered(LogContext context);

    /**
     * Log consumer registration duplicate (already registered).
     *
     * @param context Consumer context
     */
    void logConsumerRegistrationDuplicate(LogContext context);

    /**
     * Log consumer unregistered.
     *
     * @param context Consumer context
     */
    void logConsumerUnregistered(LogContext context);

    /**
     * Log batch delivery started.
     *
     * @param context Delivery context (clientId, topic, offset, batchSize)
     */
    void logBatchDeliveryStarted(LogContext context);

    /**
     * Log batch delivery succeeded.
     *
     * @param context Delivery context (clientId, topic, offset, messageCount, durationMs)
     */
    void logBatchDeliverySucceeded(LogContext context);

    /**
     * Log batch delivery failed.
     *
     * @param context Delivery context (clientId, topic, offset, error)
     */
    void logBatchDeliveryFailed(LogContext context);

    /**
     * Log batch ACK received.
     *
     * @param context ACK context (clientId, topic, offset, ackedCount)
     */
    void logBatchAckReceived(LogContext context);

    /**
     * Log consumer offset updated.
     *
     * @param context Offset context (clientId, topic, group, oldOffset, newOffset)
     */
    void logConsumerOffsetUpdated(LogContext context);

    /**
     * Log consumer lag calculated.
     *
     * @param context Lag context (clientId, topic, group, consumerOffset, storageOffset, lag)
     */
    void logConsumerLag(LogContext context);

    /**
     * Log consumer offset clamped due to validation.
     *
     * @param context Offset context (topic, group, offset, storageHead, reason)
     */
    void logConsumerOffsetClamped(LogContext context);
}
