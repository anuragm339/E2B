package com.messaging.broker.consumer;

/**
 * Service for consumer batch acknowledgment processing.
 *
 * Handles ACK workflow for both modern and legacy consumers.
 */
public interface ConsumerAckService {

    /**
     * Handle BATCH_ACK from modern consumer (single topic).
     *
     * Workflow:
     * 1. Calculate ACK latency
     * 2. Remove pending state (offset, timeout, timestamps)
     * 3. Cancel timeout future
     * 4. Update consumer offset tracker
     * 5. Record metrics (ACK count, offset, lag, latency)
     * 6. Clear in-flight status
     *
     * @param clientId Client connection identifier
     * @param topic Topic that was acknowledged
     * @param group Consumer group
     */
    void handleModernBatchAck(String clientId, String topic, String group);

    /**
     * Handle BATCH_ACK from legacy consumer (multi-topic merge).
     *
     * Workflow:
     * 1. Calculate ACK latency
     * 2. Remove pending state (offset, timeout, batch metadata)
     * 3. Cancel timeout future
     * 4. Commit offsets for all topics in merged batch
     * 5. Record metrics for each topic
     * 6. Clear in-flight status
     *
     * @param clientId Client connection identifier
     * @param group Consumer group (service name for legacy)
     */
    void handleLegacyBatchAck(String clientId, String group);

    /**
     * Check if ACK is pending for a consumer topic.
     *
     * @param topic Topic name
     * @param topic Topic name
     * @param group Consumer group
     * @return true if ACK is pending, false otherwise
     */
    boolean isAckPending(String topic, String group);

    /**
     * Clear all pending ACKs for a client (on disconnect).
     *
     * @param clientId Client connection identifier
     */
    void clearPendingAcks(String clientId);
}
