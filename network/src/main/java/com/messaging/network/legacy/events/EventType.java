package com.messaging.network.legacy.events;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;

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
            throw new NetworkException(ErrorCode.NETWORK_DECODING_ERROR,
                    "Invalid event type ordinal: " + typeOrdinal);
        }
        return values()[typeOrdinal];
    }
}
