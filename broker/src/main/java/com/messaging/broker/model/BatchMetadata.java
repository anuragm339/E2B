package com.messaging.broker.model;

import java.util.Objects;

/**
 * Value object representing metadata for a message batch delivery.
 *
 * Contains information about the batch being delivered to a consumer,
 * including offset range, message count, and size.
 */
public record BatchMetadata(
    String topic,
    int partition,
    long startOffset,
    long endOffset,
    int messageCount,
    long totalBytes
) {

    public BatchMetadata {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset cannot be negative: " + startOffset);
        }
        if (endOffset < startOffset) {
            throw new IllegalArgumentException("endOffset (" + endOffset + ") cannot be less than startOffset (" + startOffset + ")");
        }
        if (messageCount < 0) {
            throw new IllegalArgumentException("messageCount cannot be negative: " + messageCount);
        }
        if (totalBytes < 0) {
            throw new IllegalArgumentException("totalBytes cannot be negative: " + totalBytes);
        }
    }

    /**
     * Create BatchMetadata for a single message.
     *
     * @param topic Topic name
     * @param partition Partition number
     * @param offset Message offset
     * @param messageSize Size in bytes
     * @return BatchMetadata instance
     */
    public static BatchMetadata singleMessage(String topic, int partition, long offset, long messageSize) {
        return new BatchMetadata(topic, partition, offset, offset, 1, messageSize);
    }

    /**
     * Create BatchMetadata for a batch of messages.
     *
     * @param topic Topic name
     * @param partition Partition number
     * @param startOffset First message offset
     * @param endOffset Last message offset
     * @param messageCount Number of messages in batch
     * @param totalBytes Total size in bytes
     * @return BatchMetadata instance
     */
    public static BatchMetadata batch(
        String topic,
        int partition,
        long startOffset,
        long endOffset,
        int messageCount,
        long totalBytes
    ) {
        return new BatchMetadata(topic, partition, startOffset, endOffset, messageCount, totalBytes);
    }

    /**
     * Check if this represents a single message.
     *
     * @return True if messageCount is 1
     */
    public boolean isSingleMessage() {
        return messageCount == 1;
    }

    /**
     * Check if this represents a batch of multiple messages.
     *
     * @return True if messageCount is greater than 1
     */
    public boolean isBatch() {
        return messageCount > 1;
    }

    /**
     * Get the offset range (endOffset - startOffset + 1).
     *
     * @return Number of offsets covered
     */
    public long offsetRange() {
        return endOffset - startOffset + 1;
    }

    /**
     * Get average message size in bytes.
     *
     * @return Average size per message
     */
    public long averageMessageSize() {
        return messageCount > 0 ? totalBytes / messageCount : 0;
    }
}
