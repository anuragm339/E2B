package com.example.consumer.service;

import com.example.consumer.legacy.events.Event;
import com.messaging.common.model.BrokerMessage;

import java.io.IOException;

/**
 * Abstract broker connection supporting both legacy and modern protocols.
 *
 * Legacy mode: Uses Event protocol (ordinals 0-7)
 * - RegisterEvent, AckEvent, BatchEvent, etc.
 *
 * Modern mode: Uses BrokerMessage protocol (codes 0x01-0x0C)
 * - SUBSCRIBE, BATCH_ACK, DATA, etc.
 */
public interface BrokerConnection {
    /**
     * Connect to broker and perform handshake
     */
    void connect(String host, int port) throws IOException;

    /**
     * Send acknowledgment to broker
     */
    void sendAck() throws IOException;

    /**
     * Receive next event (legacy protocol only)
     * @throws UnsupportedOperationException if not legacy
     */
    Event nextEvent() throws IOException;

    /**
     * Receive next message (modern protocol only)
     * @throws UnsupportedOperationException if legacy
     */
    BrokerMessage nextMessage() throws IOException;

    /**
     * Close connection
     */
    void close() throws IOException;

    /**
     * Check if this is a legacy protocol connection
     */
    boolean isLegacy();
}
