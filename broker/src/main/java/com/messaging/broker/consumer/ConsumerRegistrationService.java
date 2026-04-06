package com.messaging.broker.consumer;

import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.model.ConsumerKey;

import java.util.Collection;
import java.util.Optional;

/**
 * Service for consumer registration and lifecycle management.
 *
 * Handles consumer registration, unregistration, and lookup operations.
 */
public interface ConsumerRegistrationService {

    /**
     * Registration result containing consumer and metadata.
     */
    record RegistrationResult(
        RemoteConsumer consumer,
        boolean isNew,
        long restoredOffset
    ) {
        public static RegistrationResult newRegistration(RemoteConsumer consumer, long restoredOffset) {
            return new RegistrationResult(consumer, true, restoredOffset);
        }

        public static RegistrationResult duplicate(RemoteConsumer consumer) {
            return new RegistrationResult(consumer, false, consumer.getCurrentOffset());
        }

        public boolean isDuplicate() {
            return !isNew;
        }
    }

    /**
     * Register a consumer for message delivery.
     *
     * Restores offset from persistent storage and validates against storage head.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     * @param isLegacy Whether this is a legacy protocol consumer
     * @return Registration result (new vs duplicate, consumer instance, restored offset)
     */
    RegistrationResult registerConsumer(String clientId, String topic, String group, boolean isLegacy, String traceId);

    /**
     * Unregister all consumers for a client (connection closed).
     *
     * @param clientId Client connection identifier
     * @return Number of consumers unregistered
     */
    int unregisterConsumer(String clientId);

    /**
     * Get consumer by key.
     *
     * @param key Consumer key
     * @return Consumer if present
     */
    Optional<RemoteConsumer> getConsumer(ConsumerKey key);

    /**
     * Get all consumers for a client.
     *
     * @param clientId Client connection identifier
     * @return Consumers for this client
     */
    Collection<RemoteConsumer> getConsumersByClient(String clientId);

    /**
     * Get all consumers subscribed to a topic.
     *
     * @param topic Topic name
     * @return Consumers subscribed to this topic
     */
    Collection<RemoteConsumer> getConsumersByTopic(String topic);

    /**
     * Get all registered consumers.
     *
     * @return All consumers
     */
    Collection<RemoteConsumer> getAllConsumers();
}
