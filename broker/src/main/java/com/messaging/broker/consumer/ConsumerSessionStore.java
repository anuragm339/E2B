package com.messaging.broker.consumer;

import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.model.ConsumerKey;

import java.util.Collection;
import java.util.Optional;

/**
 * Store for active consumer sessions.
 *
 * Manages the registry of connected consumers and their state.
 */
public interface ConsumerSessionStore {

    /**
     * Register a consumer.
     *
     * @param key Consumer key
     * @param consumer Consumer instance
     */
    void put(ConsumerKey key, RemoteConsumer consumer);

    /**
     * Get consumer by key.
     *
     * @param key Consumer key
     * @return Consumer if present
     */
    Optional<RemoteConsumer> get(ConsumerKey key);

    /**
     * Remove consumer.
     *
     * @param key Consumer key
     */
    void remove(ConsumerKey key);

    /**
     * Get all consumers.
     *
     * @return All registered consumers
     */
    Collection<RemoteConsumer> getAll();

    /**
     * Get all consumers for a specific client.
     *
     * @param clientId Client connection identifier
     * @return Consumers for this client
     */
    Collection<RemoteConsumer> getByClientId(String clientId);

    /**
     * Get all consumers subscribed to a topic.
     *
     * @param topic Topic name
     * @return Consumers subscribed to this topic
     */
    Collection<RemoteConsumer> getByTopic(String topic);

    /**
     * Check if consumer exists.
     *
     * @param key Consumer key
     * @return True if consumer is registered
     */
    boolean contains(ConsumerKey key);

    /**
     * Get number of registered consumers.
     *
     * @return Consumer count
     */
    int size();

    /**
     * Remove all consumers for a client.
     *
     * @param clientId Client connection identifier
     * @return Number of consumers removed
     */
    int removeByClientId(String clientId);
}
