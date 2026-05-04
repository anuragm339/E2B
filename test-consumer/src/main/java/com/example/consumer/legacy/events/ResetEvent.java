package com.example.consumer.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ResetEvent extends Event {
    public static final ResetEvent INSTANCE = new ResetEvent();

    private ResetEvent() {
        super(EventType.RESET);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        // Nothing to serialize
    }

    public static ResetEvent from(DataInputStream dataInputStream) throws IOException {
        return INSTANCE;
    }

    @Override
    public Class<? extends Event> getResponseEventType() {
        return ResetEvent.class;
    }
}
