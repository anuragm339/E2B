package com.messaging.broker.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Value object representing a data message publish request.
 *
 * Sent by producers to publish messages to a topic.
 * Format: {"topic": "...", "payload": <bytes>}
 */
public record DataMessageRequest(String topic, byte[] payload, int partition) {

    public DataMessageRequest {
        Objects.requireNonNull(topic, "topic cannot be null");
        Objects.requireNonNull(payload, "payload cannot be null");
        // Make defensive copy to ensure immutability
        payload = Arrays.copyOf(payload, payload.length);
    }

    /**
     * Create a DataMessageRequest with default partition (0).
     *
     * @param topic Topic name
     * @param payload Message payload
     * @return DataMessageRequest instance
     */
    public static DataMessageRequest of(String topic, byte[] payload) {
        return new DataMessageRequest(topic, payload, 0);
    }

    /**
     * Create a DataMessageRequest with specific partition.
     *
     * @param topic Topic name
     * @param payload Message payload
     * @param partition Partition number
     * @return DataMessageRequest instance
     */
    public static DataMessageRequest of(String topic, byte[] payload, int partition) {
        return new DataMessageRequest(topic, payload, partition);
    }

    /**
     * Get payload size in bytes.
     *
     * @return Payload size
     */
    public int payloadSize() {
        return payload.length;
    }

    /**
     * Get a copy of the payload.
     * Returns defensive copy to maintain immutability.
     *
     * @return Copy of payload bytes
     */
    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataMessageRequest that = (DataMessageRequest) o;
        return partition == that.partition &&
               Objects.equals(topic, that.topic) &&
               Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, partition);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "DataMessageRequest{" +
               "topic='" + topic + '\'' +
               ", payloadSize=" + payload.length +
               ", partition=" + partition +
               '}';
    }
}
