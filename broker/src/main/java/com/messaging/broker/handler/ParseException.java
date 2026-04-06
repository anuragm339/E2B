package com.messaging.broker.handler;

/**
 * Exception thrown when message parsing fails.
 *
 * Indicates malformed payload, missing required fields, or invalid data types.
 */
public class ParseException extends Exception {

    public ParseException(String message) {
        super(message);
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
