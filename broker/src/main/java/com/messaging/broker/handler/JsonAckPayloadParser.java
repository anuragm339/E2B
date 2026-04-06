package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.AckPayloadParser;
import com.messaging.broker.handler.ParseException;
import com.messaging.broker.model.AckPayload;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Parser for BATCH_ACK acknowledgment payloads.
 * Format: {"offset": <long>, "success": <boolean>, "errorMessage": <string>}
 */
@Singleton
public class JsonAckPayloadParser implements AckPayloadParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public AckPayload parse(byte[] payload) throws ParseException {
        if (payload == null || payload.length == 0) {
            throw new ParseException("ACK payload cannot be null or empty");
        }

        try {
            String json = new String(payload, StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(json);

            // Extract offset
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

            // Extract success
            if (!node.has("success")) {
                throw new ParseException("Missing required field: success");
            }

            JsonNode successNode = node.get("success");
            if (successNode == null || successNode.isNull()) {
                throw new ParseException("Field cannot be null: success");
            }

            if (!successNode.isBoolean()) {
                throw new ParseException("Field must be boolean: success");
            }

            boolean success = successNode.asBoolean();

            // Extract optional errorMessage
            String errorMessage = null;
            if (node.has("errorMessage")) {
                JsonNode errorNode = node.get("errorMessage");
                if (errorNode != null && !errorNode.isNull()) {
                    errorMessage = errorNode.asText();
                }
            }

            return new AckPayload(offset, success, errorMessage);

        } catch (IOException e) {
            throw new ParseException("Failed to parse ACK JSON: " + e.getMessage(), e);
        }
    }
}
