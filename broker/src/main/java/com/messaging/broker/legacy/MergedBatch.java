package com.messaging.broker.legacy;

import com.messaging.common.model.MessageRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a batch of messages merged from multiple topics,
 * sorted by global offset. Tracks per-topic max offsets for ACK.
 *
 * Example:
 *   Messages: [
 *     {offset=10, topic="price-v1"},
 *     {offset=15, topic="price-v1"},
 *     {offset=20, topic="price-v2"},
 *     {offset=22, topic="price-v1"}
 *   ]
 *
 *   maxOffsetPerTopic: {
 *     "price-v1": 22,
 *     "price-v2": 20
 *   }
 */
public class MergedBatch {
    private final List<MessageRecord> messages;
    private final Map<String, Long> maxOffsetPerTopic;
    private long totalBytes;

    public MergedBatch() {
        this.messages = new ArrayList<>();
        this.maxOffsetPerTopic = new HashMap<>();
        this.totalBytes = 0;
    }

    /**
     * Add a message to the batch and track its topic's max offset
     */
    public void add(String topic, MessageRecord msg) {
        messages.add(msg);
        totalBytes += estimateMessageSize(msg);

        // Track highest offset per topic for ACK
        maxOffsetPerTopic.merge(topic, msg.getOffset(), Math::max);
    }

    /**
     * Estimate message size for batch size limit
     */
    private long estimateMessageSize(MessageRecord msg) {
        // Rough estimate: key length + data length + overhead
        return msg.getMsgKey().length() +
               (msg.getData() != null ? msg.getData().length() : 0) +
               50; // Overhead (offset, eventType, etc.)
    }

    // Getters
    public List<MessageRecord> getMessages() {
        return messages;
    }

    public Map<String, Long> getMaxOffsetPerTopic() {
        return maxOffsetPerTopic;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public int getMessageCount() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("MergedBatch{messages=%d, bytes=%d, topics=%s}",
                messages.size(), totalBytes, maxOffsetPerTopic);
    }
}
