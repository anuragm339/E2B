package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;

import java.util.Set;
import java.util.concurrent.ScheduledFuture;

/**
 * Store for consumer READY_ACK state tracking.
 *
 * Manages readiness signals from consumers during startup and data refresh.
 */
public interface ReadyStateStore {

    /**
     * Mark legacy consumer as ready.
     *
     * @param clientId Client connection identifier
     */
    void markLegacyConsumerReady(String clientId);

    /**
     * Check if legacy consumer is ready.
     *
     * @param clientId Client connection identifier
     * @return True if ready
     */
    boolean isLegacyConsumerReady(String clientId);

    /**
     * Mark modern consumer subscription as ready.
     *
     * @param clientId Client connection identifier
     * @param key Delivery key
     */
    void markModernConsumerTopicReady(String clientId, DeliveryKey key);

    /**
     * Check if modern consumer subscription is ready.
     *
     * @param clientId Client connection identifier
     * @param key Delivery key
     * @return True if ready
     */
    boolean isModernConsumerTopicReady(String clientId, DeliveryKey key);

    /**
     * Get all ready subscriptions for a modern consumer.
     *
     * @param clientId Client connection identifier
     * @return Set of ready delivery keys
     */
    Set<DeliveryKey> getModernConsumerReadyTopics(String clientId);

    /**
     * Schedule a READY retry task.
     *
     * @param key Retry key (clientId for legacy, or clientId:group:topic for modern)
     * @param task Scheduled task
     */
    void scheduleReadyRetry(String key, ScheduledFuture<?> task);

    /**
     * Cancel READY retry task.
     *
     * @param key Retry key
     */
    void cancelReadyRetry(String key);

    /**
     * Remove all state for a client.
     *
     * @param clientId Client connection identifier
     */
    void removeClient(String clientId);

    /**
     * Clear all ready state.
     */
    void clear();
}
