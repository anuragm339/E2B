package com.messaging.broker.model;

import java.util.List;
import java.util.Objects;

/**
 * Sealed interface representing a subscription request.
 * Supports both modern (topic + group) and legacy (service name + multiple topics) protocols.
 */
public sealed interface SubscribeRequest {

    /**
     * @return Client connection identifier
     */
    String clientId();

    /**
     * @return True if this is a modern protocol request
     */
    boolean isModern();

    /**
     * @return True if this is a legacy protocol request
     */
    boolean isLegacy();

    /**
     * Modern protocol subscribe request: single topic with consumer group.
     * Format: {"topic": "...", "group": "..."}
     */
    record Modern(String clientId, String topic, String group) implements SubscribeRequest {

        public Modern {
            Objects.requireNonNull(clientId, "clientId cannot be null");
            Objects.requireNonNull(topic, "topic cannot be null");
            Objects.requireNonNull(group, "group cannot be null");
        }

        @Override
        public boolean isModern() {
            return true;
        }

        @Override
        public boolean isLegacy() {
            return false;
        }
    }

    /**
     * Legacy protocol subscribe request: multiple topics with service name.
     * Format: {"isLegacy": true, "serviceName": "...", "topics": [...]}
     */
    record Legacy(String clientId, String serviceName, List<String> topics) implements SubscribeRequest {

        public Legacy {
            Objects.requireNonNull(clientId, "clientId cannot be null");
            Objects.requireNonNull(serviceName, "serviceName cannot be null");
            Objects.requireNonNull(topics, "topics cannot be null");
            if (topics.isEmpty()) {
                throw new IllegalArgumentException("topics list cannot be empty");
            }
            // Make defensive copy to ensure immutability
            topics = List.copyOf(topics);
        }

        @Override
        public boolean isModern() {
            return false;
        }

        @Override
        public boolean isLegacy() {
            return true;
        }
    }

    /**
     * Create a modern protocol subscribe request.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     * @return Modern subscribe request
     */
    static SubscribeRequest modern(String clientId, String topic, String group) {
        return new Modern(clientId, topic, group);
    }

    /**
     * Create a legacy protocol subscribe request.
     *
     * @param clientId Client connection identifier
     * @param serviceName Service name
     * @param topics List of topics to subscribe to
     * @return Legacy subscribe request
     */
    static SubscribeRequest legacy(String clientId, String serviceName, List<String> topics) {
        return new Legacy(clientId, serviceName, topics);
    }
}
