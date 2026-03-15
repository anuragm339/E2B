package com.messaging.broker.model;

import java.util.Objects;

/**
 * Value object representing the canonical delivery identity.
 * Format: group:topic
 *
 * Delivery, ACK, timeout, and flow-control state are tracked per consumer
 * group on a topic rather than per client connection.
 */
public record DeliveryKey(String group, String topic) {

    public DeliveryKey {
        Objects.requireNonNull(group, "group cannot be null");
        Objects.requireNonNull(topic, "topic cannot be null");
    }

    /**
     * Create a DeliveryKey from components.
     *
     * @param group Consumer group name
     * @param topic Topic name
     * @return DeliveryKey instance
     */
    public static DeliveryKey of(String group, String topic) {
        return new DeliveryKey(group, topic);
    }

    /**
     * Parse a DeliveryKey from string format "group:topic".
     *
     * @param key String in format "group:topic"
     * @return DeliveryKey instance
     * @throws IllegalArgumentException if format is invalid
     */
    public static DeliveryKey parse(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("DeliveryKey string cannot be null or empty");
        }

        String[] parts = key.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid DeliveryKey format: " + key + " (expected group:topic)");
        }

        return of(parts[0], parts[1]);
    }

    /**
     * Convert to string representation "group:topic".
     *
     * @return String representation
     */
    @Override
    public String toString() {
        return group + ":" + topic;
    }
}
