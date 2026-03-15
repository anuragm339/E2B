package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.ParseException;
import com.messaging.broker.handler.SubscribeRequestParser;
import com.messaging.broker.model.SubscribeRequest;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for legacy protocol SUBSCRIBE requests.
 * Format: {"isLegacy": true, "serviceName": "...", "topics": [...]}
 */
@Singleton
public class LegacySubscribeRequestParser implements SubscribeRequestParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SubscribeRequest parse(String clientId, byte[] payload) throws ParseException {
        if (payload == null || payload.length == 0) {
            throw new ParseException("SUBSCRIBE payload cannot be null or empty");
        }

        try {
            String json = new String(payload, StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(json);

            // Legacy protocol: {"isLegacy": true, "serviceName": "...", "topics": [...]}
            boolean isLegacy = node.has("isLegacy") && node.get("isLegacy").asBoolean();
            if (!isLegacy) {
                throw new ParseException("Not a legacy SUBSCRIBE request (isLegacy must be true)");
            }

            String serviceName = getRequiredText(node, "serviceName");
            List<String> topics = parseTopics(node);

            return SubscribeRequest.legacy(clientId, serviceName, topics);

        } catch (IOException e) {
            throw new ParseException("Failed to parse legacy SUBSCRIBE JSON: " + e.getMessage(), e);
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

    private List<String> parseTopics(JsonNode node) throws ParseException {
        if (!node.has("topics")) {
            throw new ParseException("Missing required field: topics");
        }

        JsonNode topicsNode = node.get("topics");
        if (topicsNode == null || topicsNode.isNull()) {
            throw new ParseException("Field cannot be null: topics");
        }

        if (!topicsNode.isArray()) {
            throw new ParseException("Field must be an array: topics");
        }

        if (topicsNode.size() == 0) {
            throw new ParseException("topics array cannot be empty");
        }

        List<String> topics = new ArrayList<>();
        for (JsonNode topicNode : topicsNode) {
            topics.add(topicNode.asText());
        }

        return topics;
    }
}
