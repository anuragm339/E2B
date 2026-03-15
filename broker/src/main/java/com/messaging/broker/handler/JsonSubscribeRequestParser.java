package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.ParseException;
import com.messaging.broker.handler.SubscribeRequestParser;
import com.messaging.broker.model.SubscribeRequest;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Parser for modern protocol SUBSCRIBE requests.
 * Format: {"topic": "...", "group": "..."}
 */
@Singleton
public class JsonSubscribeRequestParser implements SubscribeRequestParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SubscribeRequest parse(String clientId, byte[] payload) throws ParseException {
        if (payload == null || payload.length == 0) {
            throw new ParseException("SUBSCRIBE payload cannot be null or empty");
        }

        try {
            String json = new String(payload, StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(json);

            // Modern protocol: {"topic": "...", "group": "..."}
            if (!node.has("topic") || !node.has("group")) {
                throw new ParseException("Invalid SUBSCRIBE format: missing topic or group");
            }

            String topic = getRequiredText(node, "topic");
            String group = getRequiredText(node, "group");

            if (topic.isEmpty()) {
                throw new ParseException("SUBSCRIBE topic cannot be empty");
            }

            if (group.isEmpty()) {
                throw new ParseException("SUBSCRIBE group cannot be empty");
            }

            return SubscribeRequest.modern(clientId, topic, group);

        } catch (IOException e) {
            throw new ParseException("Failed to parse SUBSCRIBE JSON: " + e.getMessage(), e);
        }
    }

    private String getRequiredText(JsonNode node, String field) throws ParseException {
        if (!node.has(field)) {
            throw new ParseException("Missing required field: " + field);
        }

        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new ParseException("Field cannot be null: " + field);
        }

        return value.asText();
    }
}
