package com.messaging.common.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Consumer-facing record.
 * Internal offset is NOT exposed to consumers.
 */
public class ConsumerRecord {
    private String msgKey;
    private EventType eventType;
    private String data;           // null for DELETE events
    private Instant createdAt;

    // Internal - used by broker for tracking, not exposed to consumer code
    private long internalOffset;

    public ConsumerRecord() {

    }
    public ConsumerRecord(String msgKey, EventType eventType, String data, Instant createdAt) {
        this.msgKey = msgKey;
        this.eventType = eventType;
        this.data = data;
        this.createdAt = createdAt;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getData() {
        return data;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    // Package-private - only broker framework can access
    long getInternalOffset() {
        return internalOffset;
    }

    void setInternalOffset(long internalOffset) {
        this.internalOffset = internalOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerRecord that = (ConsumerRecord) o;
        return Objects.equals(msgKey, that.msgKey) &&
               eventType == that.eventType &&
               Objects.equals(createdAt, that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgKey, eventType, createdAt);
    }

    @Override
    public String toString() {
        return "ConsumerRecord{" +
               "msgKey='" + msgKey + '\'' +
               ", eventType=" + eventType +
               ", data=" + (data != null ? "..." : "null") +
               ", createdAt=" + createdAt +
               '}';
    }
}
