package com.messaging.broker.consumer;

/**
 * Handles the REPLAY phase of the refresh workflow.
 */
public interface ReplayPhase {
    /**
     * Check replay progress for a topic.
     * Triggers replay for consumers that haven't caught up.
     *
     * @param topic Topic name
     * @param context Refresh context
     * @return true if all consumers caught up and ready for READY phase
     */
    boolean checkReplayProgress(String topic, RefreshContext context);

    /**
     * Start replay for a specific consumer.
     *
     * @param clientId Client socket ID
     * @param topic Topic name
     * @param context Refresh context
     */
    void startReplayForConsumer(String clientId, String topic, RefreshContext context);

    /**
     * Check if all consumers have caught up to latest offset.
     *
     * @param topic Topic name
     * @param ackedConsumers Set of consumers that have ACKed RESET
     * @return true if all ACKed consumers have caught up
     */
    boolean allConsumersCaughtUp(String topic, java.util.Set<String> ackedConsumers);
}
