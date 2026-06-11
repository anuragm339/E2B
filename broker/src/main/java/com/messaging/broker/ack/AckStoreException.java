package com.messaging.broker.ack;

/**
 * Signals that the durable ACK store could not complete an operation.
 */
public class AckStoreException extends RuntimeException {

    public AckStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
