package com.messaging.network.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ReadyEvent extends Event {
    public static final ReadyEvent INSTANCE = new ReadyEvent();

    private ReadyEvent() {
        super(EventType.READY);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        // Nothing to serialize
    }

    public static ReadyEvent from(DataInputStream dataInputStream) throws IOException {
        return INSTANCE;
    }

    @Override
    public Class<? extends Event> getResponseEventType() {
        return ReadyEvent.class;
    }
}
