package com.messaging.broker.handler;

/**
 * Functional interface for handling client disconnections.
 */
@FunctionalInterface
public interface DisconnectHandler {
    /**
     * Handle client disconnect.
     *
     * @param clientId Client connection identifier
     */
    void handleDisconnect(String clientId);
}
