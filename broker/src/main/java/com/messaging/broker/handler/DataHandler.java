package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.compaction.CompactionIndex;
import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Handles DATA messages - stores message and sends ACK.
 */
@Singleton
public class DataHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(DataHandler.class);

    private final StorageEngine storage;
    private final NetworkServer server;
    private final BrokerMetrics metrics;
    private final CompactionIndex compactionIndex;
    private final ObjectMapper objectMapper;

    @Inject
    public DataHandler(
            StorageEngine storage,
            NetworkServer server,
            BrokerMetrics metrics,
            CompactionIndex compactionIndex) {
        this.storage = storage;
        this.server = server;
        this.metrics = metrics;
        this.compactionIndex = compactionIndex;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public BrokerMessage.MessageType getMessageType() {
        return BrokerMessage.MessageType.DATA;
    }

    @Override
    public void handle(String clientId, BrokerMessage message, String traceId) {
        Timer.Sample e2eSample = metrics.startE2ETimer();

        try {
            metrics.recordMessageReceived();
            metrics.recordMessageSize(message.getPayload().length);

            // Decode payload
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            // Extract required topic field, and reject path-traversal names early (P0 hardening).
            String topic = safeGetText(json, "topic", clientId);
            com.messaging.common.validation.TopicNames.validate(topic);

            // Extract fields
            String msgKey = json.has("msg_key") ? json.get("msg_key").asText() : "key_" + System.currentTimeMillis();
            String eventTypeStr = json.has("event_type") ? json.get("event_type").asText() : "MESSAGE";
            String data = json.has("data") ? json.get("data").toString() : payload;

            EventType eventType = eventTypeStr.equals("DELETE") ? EventType.DELETE : EventType.MESSAGE;

            // Create message record. created_time is authoritative and immutable: take it from the
            // payload ("created", legacy alias "createdAt") and store it verbatim so it survives
            // unchanged down the pipe (it gates refresh READY). Only stamp ingest time if the
            // producer omitted it.
            MessageRecord record = new MessageRecord(
                msgKey,
                eventType,
                eventType == EventType.DELETE ? null : data,
                extractCreatedAt(json)
            );

            // Store
            Timer.Sample storageSample = metrics.startStorageWriteTimer();
            long offset = storage.append(topic, 0, record);
            metrics.stopStorageWriteTimer(storageSample);

            metrics.recordMessageStored(topic);
            metrics.recordTopicLastMessageTime(topic);
            metrics.updateTopicHeadOffset(topic, offset);

            compactionIndex.updateKey(topic, msgKey, offset, record.getCreatedAt().toEpochMilli());

            log.debug("Stored message: topic={}, offset={}, key={}, type={}, traceId={}",
                     topic, offset, msgKey, eventType, traceId);

            // Broker will discover new messages via adaptive polling (watermark-based)
            // No notification needed

            metrics.stopE2ETimer(e2eSample);

            // Send ACK
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send ACK to {}, traceId={}", clientId, traceId, ex);
                } else {
                    log.debug("Sent ACK to {} for message {}, traceId={}", clientId, message.getMessageId(), traceId);
                }
            });

        } catch (MessagingException e) {
            log.error("DATA validation failed from client {}: {}, traceId={}", clientId, e.getMessage(), traceId);
            server.closeConnection(clientId);
        } catch (Exception e) {
            log.error("Error handling DATA message from {}, traceId={}", clientId, traceId, e);
            server.closeConnection(clientId);
        }
    }

    /**
     * Safely extract text field from JSON with validation.
     */
    private String safeGetText(JsonNode node, String fieldName, String clientId) {
        if (!node.has(fieldName)) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_MESSAGE_DATA,
                String.format("Missing required field '%s' from client %s", fieldName, clientId)
            );
        }
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_MESSAGE_DATA,
                String.format("Field '%s' is null from client %s", fieldName, clientId)
            );
        }
        return field.asText();
    }

    /**
     * Extract the authoritative {@code created_time} from the producer payload so it is stored
     * verbatim (the refresh READY gate compares it against the settle window). Accepts {@code created}
     * or the legacy alias {@code createdAt}, as either an ISO-8601 string or epoch-millis number.
     * Falls back to {@code Instant.now()} only when the producer omits it (or it is unparseable),
     * preserving the previous behaviour for callers that never sent a timestamp.
     */
    private static Instant extractCreatedAt(JsonNode json) {
        JsonNode node = json.hasNonNull("created") ? json.get("created")
                : json.hasNonNull("createdAt") ? json.get("createdAt")
                : null;
        if (node == null) {
            return Instant.now();
        }
        try {
            return node.isNumber() ? Instant.ofEpochMilli(node.asLong()) : Instant.parse(node.asText());
        } catch (Exception e) {
            log.warn("event=data.created_time_unparseable value={} — stamping ingest time", node.asText());
            return Instant.now();
        }
    }
}
