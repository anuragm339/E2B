package com.messaging.broker.consumer;

import java.util.Set;

/**
 * Handles the RESET phase of the refresh workflow.
 */
public interface ResetPhase {
    /**
     * Send RESET to all consumers for a topic.
     *
     * @param topic Topic name
     * @param context Refresh context
     */
    void sendReset(String topic, RefreshContext context);

    /**
     * Handle RESET ACK from a consumer.
     *
     * @param consumerGroupTopic Consumer identifier ("group:topic")
     * @param clientId Client socket ID
     * @param topic Topic name
     * @param context Refresh context
     * @return true if this was the first RESET ACK (should transition to REPLAYING)
     */
    boolean handleResetAck(String consumerGroupTopic, String clientId, String topic, RefreshContext context, String traceId);

    /**
     * Retry RESET broadcast for consumers that haven't ACKed.
     *
     * @param topic Topic name
     * @param context Refresh context
     * @return Set of consumers still missing RESET ACKs
     */
    Set<String> retryResetBroadcast(String topic, RefreshContext context);

    /**
     * Get consumers that haven't sent RESET ACK yet.
     *
     * @param context Refresh context
     * @return Set of missing consumer identifiers
     */
    Set<String> getMissingResetAcks(RefreshContext context);

    /**
     * Check if all RESET ACKs have been received.
     *
     * @param context Refresh context
     * @return true if all expected consumers have ACKed
     */
    boolean allResetAcksReceived(RefreshContext context);
}
