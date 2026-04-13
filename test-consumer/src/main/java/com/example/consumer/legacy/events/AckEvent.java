package com.example.consumer.legacy.events;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AckEvent extends Event {
    public static final AckEvent INSTANCE = new AckEvent();

    private AckEvent() {
        super(EventType.ACK);
    }

    @Override
    public void toWire(DataOutputStream dos) throws IOException {
        // Nothing to serialize
    }

    public static AckEvent from(DataInputStream dataInputStream) throws IOException {
        return INSTANCE;
    }
}
