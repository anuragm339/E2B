package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.CommitOffsetRequestParser;
import com.messaging.broker.handler.ParseException;
import com.messaging.broker.model.CommitOffsetRequest;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Parser for COMMIT_OFFSET requests.
 * Format: {"offset": <long>}
 */
@Singleton
public class JsonCommitOffsetRequestParser implements CommitOffsetRequestParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CommitOffsetRequest parse(String clientId, String topic, String group, byte[] payload) throws ParseException {
        if (payload == null || payload.length == 0) {
            throw new ParseException("COMMIT_OFFSET payload cannot be null or empty");
        }

        try {
            String json = new String(payload, StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(json);

            if (!node.has("offset")) {
                throw new ParseException("Missing required field: offset");
            }

            JsonNode offsetNode = node.get("offset");
            if (offsetNode == null || offsetNode.isNull()) {
                throw new ParseException("Field cannot be null: offset");
            }

            if (!offsetNode.isNumber()) {
                throw new ParseException("Field must be numeric: offset");
            }

            long offset = offsetNode.asLong();

            return CommitOffsetRequest.of(clientId, topic, group, offset);

        } catch (IOException e) {
            throw new ParseException("Failed to parse COMMIT_OFFSET JSON: " + e.getMessage(), e);
        }
    }
}
