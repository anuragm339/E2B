package com.messaging.common.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Internal message record stored in segments.
 * Contains all fields including internal offset.
 */
public class MessageRecord {
    private long offset;           // Internal, auto-assigned by broker
    private String topic;
    private int partition;
    private String msgKey;         // Business key for compaction
    private EventType eventType;   // MESSAGE or DELETE
    private String data;           // JSON payload (null for DELETE events)
    private Instant createdAt;
    private int crc32;             // Checksum

    public MessageRecord() {
    }

    public MessageRecord(String msgKey, EventType eventType, String data, Instant createdAt) {
        this.msgKey = msgKey;
        this.eventType = eventType;
        this.data = data;
        this.createdAt = createdAt;
    }

    // Getters and setters
    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public int getCrc32() {
        return crc32;
    }

    public void setCrc32(int crc32) {
        this.crc32 = crc32;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageRecord that = (MessageRecord) o;
        return offset == that.offset &&
               partition == that.partition &&
               Objects.equals(topic, that.topic) &&
               Objects.equals(msgKey, that.msgKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, topic, partition, msgKey);
    }

    @Override
    public String toString() {
        return "MessageRecord{" +
               "offset=" + offset +
               ", topic='" + topic + '\'' +
               ", partition=" + partition +
               ", msgKey='" + msgKey + '\'' +
               ", eventType=" + eventType +
               ", createdAt=" + createdAt +
               '}';
    }
}
