package com.messaging.common.model;

/**
 * Event types for messages
 */
public enum EventType {
    /**
     * Normal message event - consumer should INSERT or UPDATE
     */
    MESSAGE('M'),

    /**
     * Delete event (tombstone) - consumer should DELETE the record
     */
    DELETE('D');

    private final char code;

    EventType(char code) {
        this.code = code;
    }

    public char getCode() {
        return code;
    }

    public static EventType fromCode(char code) {
        for (EventType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type code: " + code);
    }
}
