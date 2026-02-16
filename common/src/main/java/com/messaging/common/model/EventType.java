package com.messaging.common.model;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;

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
        NetworkException ex = new NetworkException(ErrorCode.NETWORK_DECODING_ERROR,
            "Unknown event type code: " + code);
        ex.withContext("eventTypeCode", (int) code);
        // Wrap in RuntimeException since enum methods can't declare checked exceptions
        throw new RuntimeException("Invalid event type code: " + code, ex);
    }
}
