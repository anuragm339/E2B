package com.messaging.network.legacy.events;

public enum EventType {
    REGISTER,
    MESSAGE,
    RESET,
    READY,
    ACK,
    EOF,
    DELETE,
    BATCH;

    public static EventType get(int typeOrdinal) {
        if(typeOrdinal<0 || typeOrdinal>=values().length) {
            throw new IllegalArgumentException("Invalid event type ordinal: " + typeOrdinal);
        }
        return values()[typeOrdinal];
    }
}
