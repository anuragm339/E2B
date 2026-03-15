package com.messaging.broker.handler;

import com.messaging.broker.model.AckPayload;

/**
 * Parser for BATCH_ACK message payloads.
 *
 * Converts raw JSON bytes into typed AckPayload objects.
 */
public interface AckPayloadParser {

    /**
     * Parse BATCH_ACK message payload into typed ack payload object.
     *
     * @param payload Raw message payload (JSON)
     * @return Parsed ack payload
     * @throws ParseException if payload is malformed or missing required fields
     */
    AckPayload parse(byte[] payload) throws ParseException;
}
