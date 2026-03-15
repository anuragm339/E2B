package com.messaging.broker.monitoring;

/**
 * Semantic logging interface for refresh events.
 */
public interface RefreshEventLogger {
    /**
     * Log refresh started.
     *
     * @param context Refresh context (topic, refreshId, consumerCount)
     */
    void logRefreshStarted(LogContext context);

    /**
     * Log RESET sent to consumers.
     *
     * @param context RESET context (topic, refreshId, consumerCount)
     */
    void logResetSent(LogContext context);

    /**
     * Log RESET ACK received from consumer.
     *
     * @param context ACK context (topic, refreshId, consumer, ackedCount, expectedCount)
     */
    void logResetAckReceived(LogContext context);

    /**
     * Log transition to REPLAYING state.
     *
     * @param context State context (topic, refreshId, state)
     */
    void logStateTransition(LogContext context);

    /**
     * Log replay progress check.
     *
     * @param context Progress context (topic, refreshId, caughtUpCount, totalCount)
     */
    void logReplayProgress(LogContext context);

    /**
     * Log READY sent to consumers.
     *
     * @param context READY context (topic, refreshId, consumerCount)
     */
    void logReadySent(LogContext context);

    /**
     * Log READY ACK received from consumer.
     *
     * @param context ACK context (topic, refreshId, consumer, ackedCount, expectedCount)
     */
    void logReadyAckReceived(LogContext context);

    /**
     * Log refresh completed successfully.
     *
     * @param context Completion context (topic, refreshId, durationMs, consumerCount)
     */
    void logRefreshCompleted(LogContext context);

    /**
     * Log refresh aborted due to error.
     *
     * @param context Error context (topic, refreshId, state, error)
     */
    void logRefreshAborted(LogContext context);

    /**
     * Log pipe calls paused for refresh.
     *
     * @param context Pipe context (topic, refreshId)
     */
    void logPipePaused(LogContext context);

    /**
     * Log pipe calls resumed after refresh.
     *
     * @param context Pipe context (topic, refreshId)
     */
    void logPipeResumed(LogContext context);
}
