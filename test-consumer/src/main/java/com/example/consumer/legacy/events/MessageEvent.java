package com.example.consumer.legacy.events;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public abstract class MessageEvent<T extends Message> extends EventWithOffset {
    static final long NO_OFFSET = Long.MIN_VALUE;
    static final long NO_PUBLISHED_TIME = Long.MIN_VALUE;

    private final T payload;
    private final long size;

    MessageEvent(EventType type, T payload, long offset, long publishedTime, long size, long createTime) {
        super(type, offset, publishedTime, createTime);
        this.payload = payload;
        this.size = size;
    }

    public T getPayload() {
        return payload;
    }

    public T getMessage() {
        return payload;
    }

    public String getEventIdentifier() {
        return getPayload().getKey();
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        // Subclasses will implement specific serialization
    }

    String payloadType() {
        return payload.getType();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MessageEvent<?> that = (MessageEvent<?>) o;
        return size == that.size && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), payload, size);
    }
}
