package com.example.consumer.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EOFEvent extends Event {
    public static final EOFEvent INSTANCE = new EOFEvent();

    private EOFEvent() {
        super(EventType.EOF);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        // Nothing to serialize
    }

    public static EOFEvent from(DataInputStream dataInputStream) throws IOException {
        return INSTANCE;
    }
}
