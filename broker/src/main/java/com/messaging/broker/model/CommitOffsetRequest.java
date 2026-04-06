package com.messaging.broker.model;

import java.util.Objects;

/**
 * Value object representing a consumer offset commit request.
 *
 * Sent by consumers to persist their current offset after processing messages.
 * Format: {"offset": <long>}
 */
public record CommitOffsetRequest(String clientId, String topic, String group, long offset) {

    public CommitOffsetRequest {
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(topic, "topic cannot be null");
        Objects.requireNonNull(group, "group cannot be null");
    }

    /**
     * Create a CommitOffsetRequest.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     * @param offset Offset to commit
     * @return CommitOffsetRequest instance
     */
    public static CommitOffsetRequest of(String clientId, String topic, String group, long offset) {
        return new CommitOffsetRequest(clientId, topic, group, offset);
    }

    /**
     * Get the consumer key for this commit request.
     *
     * @return ConsumerKey representing the consumer
     */
    public ConsumerKey toConsumerKey() {
        return ConsumerKey.of(clientId, topic, group);
    }

    /**
     * Get the delivery key for this commit request.
     *
     * @return DeliveryKey for offset tracking
     */
    public DeliveryKey toDeliveryKey() {
        return DeliveryKey.of(group, topic);
    }
}
