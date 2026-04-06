package com.messaging.broker.model;

import java.util.Objects;

/**
 * Value object representing a unique consumer identifier.
 * Format: clientId:topic:group
 *
 * This replaces String-based keys throughout the codebase to provide type safety
 * and prevent bugs from incorrect key construction.
 */
public record ConsumerKey(String clientId, String topic, String group) {

    public ConsumerKey {
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(topic, "topic cannot be null");
        Objects.requireNonNull(group, "group cannot be null");
    }

    /**
     * Create a ConsumerKey from components.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     * @return ConsumerKey instance
     */
    public static ConsumerKey of(String clientId, String topic, String group) {
        return new ConsumerKey(clientId, topic, group);
    }

    /**
     * Parse a ConsumerKey from string format "clientId:topic:group".
     *
     * @param key String in format "clientId:topic:group"
     * @return ConsumerKey instance
     * @throws IllegalArgumentException if format is invalid
     */
    public static ConsumerKey parse(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("ConsumerKey string cannot be null or empty");
        }

        String[] parts = key.split(":", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid ConsumerKey format: " + key + " (expected clientId:topic:group)");
        }

        return of(parts[0], parts[1], parts[2]);
    }

    /**
     * Convert to string representation "clientId:topic:group".
     *
     * @return String representation
     */
    @Override
    public String toString() {
        return clientId + ":" + topic + ":" + group;
    }

    /**
     * Convert to DeliveryKey (group:topic).
     *
     * @return DeliveryKey for this consumer
     */
    public DeliveryKey toDeliveryKey() {
        return DeliveryKey.of(group, topic);
    }
}
