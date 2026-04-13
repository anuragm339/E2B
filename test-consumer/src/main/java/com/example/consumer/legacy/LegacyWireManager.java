package com.example.consumer.legacy;

import com.example.consumer.legacy.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wire protocol manager for legacy Event protocol.
 * Handles serialization and deserialization of Events over the wire.
 */
public class LegacyWireManager {
    private static final Logger log = LoggerFactory.getLogger(LegacyWireManager.class);

    private final DataInputStream dataInputStream;
    private final DataOutputStream dataOutputStream;
    private final Object writeLock = new Object();

    public LegacyWireManager(DataInputStream dis, DataOutputStream dos) {
        this.dataInputStream = dis;
        this.dataOutputStream = dos;
    }

    /**
     * Read the next event from the wire.
     * Format: [EventType:1B][Payload:variable]
     */
    public Event nextEvent() throws IOException {
        Event event = Event.from(dataInputStream);
        log.trace("Received event: {}", event.getType());
        return event;
    }

    /**
     * Send an event over the wire.
     * Format: [EventType:1B][Payload:variable]
     */
    public void send(Event event) throws IOException {
        synchronized (writeLock) {
            dataOutputStream.writeByte(event.getType().ordinal());
            event.toWire(dataOutputStream);
            dataOutputStream.flush();
            log.trace("Sent event: {}", event.getType());
        }
    }
}
