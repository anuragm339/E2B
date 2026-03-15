package com.messaging.broker.handler;

import com.messaging.common.model.BrokerMessage;

/**
 * Base interface for message handlers.
 *
 * Each handler processes a specific message type from the wire protocol.
 */
public interface MessageHandler {

    /**
     * Handle incoming message from client.
     *
     * @param clientId Client connection identifier
     * @param message Message to process
     */
    void handle(String clientId, BrokerMessage message, String traceId);

    /**
     * Get the message type this handler processes.
     *
     * @return Message type
     */
    BrokerMessage.MessageType getMessageType();
}
