package com.messaging.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.time.Instant;
import java.util.Objects;

/**
 * Internal message record stored in segments.
 * Contains all fields including internal offset.
 *
 * Maps to external API response:
 * - API "type" → topic
 * - API "key" → msgKey
 * - API "offset" (String) → offset (long)
 * - API "created" → createdAt
 * - API "data" → data (if null, eventType = DELETE)
 * - API "contentType" → contentType
 */
public class MessageRecord {
    private long offset;           // Internal, auto-assigned by broker

    @JsonProperty("type")
    private String topic;

    private int partition;

    @JsonProperty("key")
    private String msgKey;         // Business key for compaction

    private EventType eventType;   // MESSAGE or DELETE (auto-detected from data field)

    @JsonSetter(nulls = Nulls.SKIP)  // Allow null data for DELETE events
    private String data;           // JSON payload (null for DELETE events)

    @JsonProperty("created")
    private Instant createdAt;

    @JsonProperty("contentType")
    private String contentType;    // e.g., "application/json"

    public MessageRecord() {
    }

    public MessageRecord(String msgKey, EventType eventType, String data, Instant createdAt) {
        this.msgKey = msgKey;
        this.eventType = eventType;
        this.data = data;
        this.createdAt = createdAt;
    }

    public MessageRecord(long offset, String topic, int partition, String msgKey, EventType eventType, String data, Instant createdAt) {
        this.offset = offset;
        this.topic = topic;
        this.partition = partition;
        this.msgKey = msgKey;
        this.eventType = eventType;
        this.data = data;
        this.createdAt = createdAt;
    }

    // Getters and setters
    public long getOffset() {
        return offset;
    }

    /**
     * Set offset - handles both long and String from API
     */
    @JsonProperty("offset")
    public void setOffset(Object offset) {
        if (offset instanceof String) {
            this.offset = Long.parseLong((String) offset);
        } else if (offset instanceof Number) {
            this.offset = ((Number) offset).longValue();
        } else {
            this.offset = 0;
        }
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

    /**
     * Set data - auto-detects DELETE events when data is null
     */
    public void setData(String data) {
        this.data = data;
        // Auto-detect event type: if data is null, it's a DELETE event
        if (this.eventType == null) {  // Only auto-detect if not already set
            this.eventType = (data == null || data.isEmpty()) ? EventType.DELETE : EventType.MESSAGE;
        }
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
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
               ", contentType='" + contentType + '\'' +
               ", createdAt=" + createdAt +
               ", hasData=" + (data != null && !data.isEmpty()) +
               '}';
    }
}
