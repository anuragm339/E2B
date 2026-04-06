package com.messaging.broker.consumer;

import java.util.Set;

/**
 * Handles the READY phase of the refresh workflow.
 */
public interface ReadyPhase {
    /**
     * Send READY message to consumers that ACKed RESET.
     *
     * @param topic Topic name
     * @param context Refresh context
     */
    void sendReady(String topic, RefreshContext context);

    /**
     * Handle READY ACK from a consumer.
     *
     * @param consumerGroupTopic Consumer identifier ("group:topic")
     * @param topic Topic name
     * @param context Refresh context
     * @return true if all READY ACKs received (should complete refresh)
     */
    boolean handleReadyAck(String consumerGroupTopic, String topic, RefreshContext context, String traceId);

    /**
     * Check for READY ACK timeout and retry if needed.
     *
     * @param topic Topic name
     * @param context Refresh context
     */
    void checkReadyAckTimeout(String topic, RefreshContext context);

    /**
     * Get consumers that haven't sent READY ACK yet.
     *
     * @param context Refresh context
     * @return Set of missing consumer identifiers
     */
    Set<String> getMissingReadyAcks(RefreshContext context);

    /**
     * Check if all READY ACKs have been received.
     *
     * @param context Refresh context
     * @return true if all expected consumers have ACKed
     */
    boolean allReadyAcksReceived(RefreshContext context);

    /**
     * Complete the refresh workflow.
     *
     * @param topic Topic name
     * @param context Refresh context
     */
    void completeRefresh(String topic, RefreshContext context);
}
