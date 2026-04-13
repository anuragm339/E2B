package com.example.consumer.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BatchEvent extends EventWithOffset {
    private final List<Event> events;

    public BatchEvent(List<Event> events) {
        super(EventType.BATCH,
              lastEvent(events).getOffset(),
              lastEvent(events).getPublishedTime(),
              lastEvent(events).getCreateTime());
        this.events = new ArrayList<>(events);
    }

    private static EventWithOffset lastEvent(List<Event> events) {
        return (EventWithOffset) events.get(events.size() - 1);
    }

    public List<Long> getOffsets() {
        return events.stream()
                .filter(e -> e instanceof EventWithOffset)
                .map(e -> ((EventWithOffset) e).getOffset())
                .collect(Collectors.toList());
    }

    public String getEventIdentifier() {
        return firstEvent().getEventIdentifier();
    }

    @SuppressWarnings("unchecked")
    public MessageEvent<? extends Message> firstEvent() {
        return (MessageEvent<? extends Message>) events.get(0);
    }

    @Override
    public void toWire(DataOutputStream out) throws IOException {
        out.writeInt(events.size());
        for (Event event : events) {
            out.writeByte(event.getType().ordinal());
            event.toWire(out);
        }
    }

    public static BatchEvent from(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            events.add(Event.from(in));
        }
        return new BatchEvent(events);
    }

    @Override
    public Class<? extends Event> getResponseEventType() {
        return AckEvent.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Boolean isConsumable(String serviceName, List<String> messageTypes) {
        return events.stream()
                .filter(e -> e instanceof MessageEvent)
                .map(e -> ((MessageEvent<Message>) e).getPayload())
                .allMatch(m -> messageTypes.contains(m.getType()));
    }

    @SuppressWarnings("unchecked")
    public List<Message> getMessages() {
        return Collections.unmodifiableList(
                events.stream()
                        .filter(e -> e instanceof MessageEvent)
                        .map(e -> ((MessageEvent<Message>) e).getPayload())
                        .collect(Collectors.toList())
        );
    }

    public List<Event> getEvents() {
        return Collections.unmodifiableList(events);
    }

    public int count() {
        return events.size();
    }

    @Override
    public long size() {
        return events.stream()
                .mapToLong(Event::size)
                .sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        BatchEvent that = (BatchEvent) o;
        return Objects.equals(events, that.events);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), events);
    }
}
