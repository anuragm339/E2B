package com.example.consumer.legacy.events;

import java.util.Objects;

public abstract class EventWithOffset extends Event {
    private final long offset;
    private final long publishedTime;
    private final long createTime;

    EventWithOffset(EventType type, long offset, long publishedTime, long createTime) {
        super(type);
        this.offset = offset;
        this.publishedTime = publishedTime;
        this.createTime = createTime;
    }

    public long getOffset() {
        return offset;
    }

    public long getPublishedTime() {
        return publishedTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public boolean isBatchEvent() {
        return getType().equals(EventType.BATCH);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventWithOffset that = (EventWithOffset) o;
        return offset == that.offset &&
               publishedTime == that.publishedTime &&
               createTime == that.createTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), offset, publishedTime, createTime);
    }
}
