package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.DataMessageRequestParser;
import com.messaging.broker.handler.ParseException;
import com.messaging.broker.model.DataMessageRequest;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Parser for DATA message requests.
 * Format: {"topic": "...", "payload": "<base64>", "partition": <int>}
 */
@Singleton
public class JsonDataMessageRequestParser implements DataMessageRequestParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public DataMessageRequest parse(byte[] payload) throws ParseException {
        if (payload == null || payload.length == 0) {
            throw new ParseException("DATA payload cannot be null or empty");
        }

        try {
            String json = new String(payload, StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(json);

            // Extract topic
            if (!node.has("topic")) {
                throw new ParseException("Missing required field: topic");
            }

            JsonNode topicNode = node.get("topic");
            if (topicNode == null || topicNode.isNull()) {
                throw new ParseException("Field cannot be null: topic");
            }

            String topic = topicNode.asText();

            // Extract payload (base64 encoded)
            if (!node.has("payload")) {
                throw new ParseException("Missing required field: payload");
            }

            JsonNode payloadNode = node.get("payload");
            if (payloadNode == null || payloadNode.isNull()) {
                throw new ParseException("Field cannot be null: payload");
            }

            String payloadBase64 = payloadNode.asText();
            byte[] messagePayload;
            try {
                messagePayload = Base64.getDecoder().decode(payloadBase64);
            } catch (IllegalArgumentException e) {
                throw new ParseException("Invalid base64 encoding in payload field", e);
            }

            // Extract partition (optional, defaults to 0)
            int partition = 0;
            if (node.has("partition")) {
                JsonNode partitionNode = node.get("partition");
                if (partitionNode != null && !partitionNode.isNull() && partitionNode.isNumber()) {
                    partition = partitionNode.asInt();
                }
            }

            return DataMessageRequest.of(topic, messagePayload, partition);

        } catch (IOException e) {
            throw new ParseException("Failed to parse DATA JSON: " + e.getMessage(), e);
        }
    }
}
