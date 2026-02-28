package com.messaging.network.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class Event {
    public static final Class<? extends Event> NO_RESPONSE_EVENT = null;
    public final Long NO_SIZE = 1L;
    private final EventType type;

    Event(EventType type) {
        this.type = type;
    }

    public EventType getType() {
        return type;
    }

    public Class<? extends Event> getResponseEventType() {
        return NO_RESPONSE_EVENT;
    }

    public abstract void toWire(DataOutputStream dos) throws IOException;

    public Boolean isConsumable(String serviceName, List<String> messageTypes) {
        return false;
    }

    public static Event from(DataInputStream dataInputStream) throws IOException {
        return EventFactory.create(dataInputStream);
    }

    public long size() {
        return NO_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return type == event.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }
}
