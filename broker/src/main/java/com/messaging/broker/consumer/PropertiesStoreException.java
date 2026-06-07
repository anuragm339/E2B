package com.messaging.broker.consumer;

/**
 * Signals that a properties-backed state store could not load or persist data.
 */
public class PropertiesStoreException extends RuntimeException {

    public PropertiesStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
