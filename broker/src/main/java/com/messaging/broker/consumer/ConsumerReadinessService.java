package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;

import java.util.Set;

/**
 * Service for consumer readiness state management.
 *
 * Handles READY_ACK workflow for both modern and legacy consumers.
 */
public interface ConsumerReadinessService {

    /**
     * Mark legacy consumer as ready (received READY_ACK).
     *
     * @param clientId Client connection identifier
     */
    void markLegacyConsumerReady(String clientId);

    /**
     * Mark modern consumer subscription as ready (received READY_ACK for topic/group).
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     */
    void markModernConsumerTopicReady(String clientId, String topic, String group);

    /**
     * Check if legacy consumer is ready.
     *
     * @param clientId Client connection identifier
     * @return True if ready
     */
    boolean isLegacyConsumerReady(String clientId);

    /**
     * Check if modern consumer subscription is ready.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     * @return True if topic/group is ready
     */
    boolean isModernConsumerTopicReady(String clientId, String topic, String group);

    /**
     * Get all ready subscriptions for a modern consumer.
     *
     * @param clientId Client connection identifier
     * @return Set of ready delivery keys
     */
    Set<DeliveryKey> getModernConsumerReadyTopics(String clientId);

    /**
     * Schedule READY retry for a consumer.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name (null for legacy consumers)
     * @param group Consumer group (null for legacy consumers)
     * @param retryCount Current retry count
     */
    void scheduleReadyRetry(String clientId, String topic, String group, int retryCount);

    /**
     * Cancel READY retry for a consumer.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name (null for legacy consumers)
     * @param group Consumer group (null for legacy consumers)
     */
    void cancelReadyRetry(String clientId, String topic, String group);

    /**
     * Remove all readiness state for a client.
     *
     * @param clientId Client connection identifier
     */
    void removeClient(String clientId);
}
