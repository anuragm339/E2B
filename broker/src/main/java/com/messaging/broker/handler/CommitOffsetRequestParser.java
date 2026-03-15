package com.messaging.broker.handler;

import com.messaging.broker.model.CommitOffsetRequest;

/**
 * Parser for COMMIT_OFFSET message payloads.
 *
 * Converts raw JSON bytes into typed CommitOffsetRequest objects.
 */
public interface CommitOffsetRequestParser {

    /**
     * Parse COMMIT_OFFSET message payload into typed request object.
     *
     * @param clientId Client connection identifier
     * @param topic Topic name
     * @param group Consumer group
     * @param payload Raw message payload (JSON)
     * @return Parsed commit offset request
     * @throws ParseException if payload is malformed or missing required fields
     */
    CommitOffsetRequest parse(String clientId, String topic, String group, byte[] payload) throws ParseException;
}
