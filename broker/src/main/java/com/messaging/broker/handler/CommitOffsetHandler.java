package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.ConsumerOffsetTracker;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Handles COMMIT_OFFSET messages - validates and persists consumer offsets.
 */
@Singleton
public class CommitOffsetHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(CommitOffsetHandler.class);

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerOffsetTracker offsetTracker;
    private final ObjectMapper objectMapper;

    @Inject
    public CommitOffsetHandler(
            StorageEngine storage,
            NetworkServer server,
            ConsumerOffsetTracker offsetTracker) {
        this.storage = storage;
        this.server = server;
        this.offsetTracker = offsetTracker;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public BrokerMessage.MessageType getMessageType() {
        return BrokerMessage.MessageType.COMMIT_OFFSET;
    }

    @Override
    public void handle(String clientId, BrokerMessage message, String traceId) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            // Extract fields with validation
            String topic = safeGetText(json, "topic", clientId);
            String group = safeGetText(json, "group", clientId);
            long offset = safeGetLong(json, "offset", clientId);

            log.debug("Client {} committed offset: topic={}, group={}, offset={}, traceId={}",
                     clientId, topic, group, offset, traceId);

            // Validate offset bounds against storage
            long storageHead = storage.getCurrentOffset(topic, 0);

            if (offset < 0) {
                log.error("COMMIT_OFFSET rejected: offset {} is negative (client={}, topic={}, group={}, traceId={})",
                         offset, clientId, topic, group, traceId);
                server.closeConnection(clientId);
                return;
            }

            if (offset > storageHead) {
                log.error("COMMIT_OFFSET clamped: offset {} exceeds storage head {} (client={}, topic={}, group={}). " +
                         "Clamping to storage head to prevent delivery stall. traceId={}",
                         offset, storageHead, clientId, topic, group, traceId);
                // Apply ceiling: clamp to storage head to prevent infinite "no new data" polling
                offset = storageHead;
            }

            // CRITICAL: Persist offset to property file - property file is ONLY source of truth
            String consumerId = group + ":" + topic;
            offsetTracker.updateOffset(consumerId, offset);

            log.debug("Persisted offset to property file: consumerId={}, offset={}, traceId={}", consumerId, offset, traceId);

            // Send ACK
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send COMMIT_OFFSET ACK to {}, traceId={}", clientId, traceId, ex);
                } else {
                    log.debug("Sent COMMIT_OFFSET ACK to {}, traceId={}", clientId, traceId);
                }
            });

        } catch (IllegalArgumentException e) {
            log.error("COMMIT_OFFSET validation failed for client {}: {}, traceId={}", clientId, e.getMessage(), traceId);
            server.closeConnection(clientId);
        } catch (Exception e) {
            log.error("Error handling COMMIT_OFFSET from {}, traceId={}", clientId, traceId, e);
            server.closeConnection(clientId);
        }
    }

    /**
     * Safely extract text field from JSON with validation.
     */
    private String safeGetText(JsonNode node, String fieldName, String clientId) {
        if (!node.has(fieldName)) {
            throw new IllegalArgumentException(
                String.format("Missing required field '%s' from client %s", fieldName, clientId)
            );
        }
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            throw new IllegalArgumentException(
                String.format("Field '%s' is null from client %s", fieldName, clientId)
            );
        }
        return field.asText();
    }

    /**
     * Safely extract long field from JSON with validation.
     */
    private long safeGetLong(JsonNode node, String fieldName, String clientId) {
        if (!node.has(fieldName)) {
            throw new IllegalArgumentException(
                String.format("Missing required field '%s' from client %s", fieldName, clientId)
            );
        }
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            throw new IllegalArgumentException(
                String.format("Field '%s' is null from client %s", fieldName, clientId)
            );
        }
        return field.asLong();
    }
}
