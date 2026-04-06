package com.messaging.broker.handler;

import com.messaging.broker.model.SubscribeRequest;

/**
 * Parser for SUBSCRIBE message payloads.
 *
 * Converts raw JSON bytes into typed SubscribeRequest objects.
 * Supports both modern (topic + group) and legacy (service name + topics) protocols.
 */
public interface SubscribeRequestParser {

    /**
     * Parse SUBSCRIBE message payload into typed request object.
     *
     * @param clientId Client connection identifier
     * @param payload Raw message payload (JSON)
     * @return Parsed subscribe request (Modern or Legacy)
     * @throws ParseException if payload is malformed or missing required fields
     */
    SubscribeRequest parse(String clientId, byte[] payload) throws ParseException;
}
