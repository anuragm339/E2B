package com.messaging.broker.handler;

import com.messaging.broker.model.DataMessageRequest;

/**
 * Parser for DATA message payloads.
 *
 * Converts raw JSON bytes into typed DataMessageRequest objects.
 */
public interface DataMessageRequestParser {

    /**
     * Parse DATA message payload into typed request object.
     *
     * @param payload Raw message payload (JSON with topic and nested payload)
     * @return Parsed data message request
     * @throws ParseException if payload is malformed or missing required fields
     */
    DataMessageRequest parse(byte[] payload) throws ParseException;
}
