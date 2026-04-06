package com.messaging.broker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.consumer.RefreshContext;
import com.messaging.broker.consumer.RefreshState;
import com.messaging.broker.legacy.LegacyClientConfig;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.model.BrokerMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Handles SUBSCRIBE messages - supports both modern and legacy protocols.
 *
 * Modern: {"topic": "prices-v1", "group": "price-quote-group"}
 * Legacy: {"isLegacy": true, "serviceName": "price-quote-service"}
 */
@Singleton
public class SubscribeHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(SubscribeHandler.class);

    private final NetworkServer server;
    private final ConsumerRegistry remoteConsumers;
    private final BrokerMetrics metrics;
    private final LegacyClientConfig legacyClientConfig;
    private final RefreshCoordinator refreshCoordinator;
    private final ObjectMapper objectMapper;

    @Inject
    public SubscribeHandler(
            NetworkServer server,
            ConsumerRegistry remoteConsumers,
            BrokerMetrics metrics,
            LegacyClientConfig legacyClientConfig,
            RefreshCoordinator refreshCoordinator) {
        this.server = server;
        this.remoteConsumers = remoteConsumers;
        this.metrics = metrics;
        this.legacyClientConfig = legacyClientConfig;
        this.refreshCoordinator = refreshCoordinator;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public BrokerMessage.MessageType getMessageType() {
        return BrokerMessage.MessageType.SUBSCRIBE;
    }

    @Override
    public void handle(String clientId, BrokerMessage message, String traceId) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            // Check if this is a legacy client
            boolean isLegacy = json.has("isLegacy") && json.get("isLegacy").asBoolean();

            if (isLegacy) {
                handleLegacySubscribe(clientId, message, json, traceId);
            } else {
                handleModernSubscribe(clientId, message, json, traceId);
            }

        } catch (Exception e) {
            log.error("Error handling SUBSCRIBE from {}: {}, traceId={}", clientId, e.getMessage(), traceId, e);
            server.closeConnection(clientId);
        }
    }

    /**
     * Handle modern SUBSCRIBE: single topic, explicit group.
     */
    private void handleModernSubscribe(String clientId, BrokerMessage message, JsonNode json, String traceId) {
        try {
            String topic = safeGetText(json, "topic", clientId);
            String group = safeGetText(json, "group", clientId);

            log.info("event=subscribe.processed mode=modern clientId={} topic={} group={} traceId={}", clientId, topic, group, traceId);

            // Register consumer - only record connection metric for new registrations
            boolean isNew = remoteConsumers.registerConsumer(clientId, topic, group, false, traceId);
            if (isNew) {
                metrics.recordConsumerConnection();
            } else {
                log.warn("event=subscribe.duplicate clientId={} topic={} group={} traceId={}",
                        clientId, topic, group, traceId);
            }

            log.debug("Registered remote consumer {} for topic={}, group={}, traceId={}", clientId, topic, group, traceId);

            // Send ACK (modern client)
            sendSubscribeAck(clientId, message, false, traceId);

            // Send READY to start delivery (modern consumers deliver only after READY_ACK)
            if (isNew) {
                remoteConsumers.sendStartupReadyToModernConsumer(clientId, topic, group);
            }

        } catch (IllegalArgumentException e) {
            log.error("SUBSCRIBE validation failed for client {}: {}, traceId={}", clientId, e.getMessage(), traceId);
            server.closeConnection(clientId);
        } catch (Exception e) {
            log.error("Error handling SUBSCRIBE from {}, traceId={}", clientId, traceId, e);
            server.closeConnection(clientId);
        }
    }

    /**
     * Handle legacy SUBSCRIBE: multiple topics based on serviceName.
     */
    private void handleLegacySubscribe(String clientId, BrokerMessage message, JsonNode json, String traceId) {
        String serviceName = safeGetText(json, "serviceName", clientId);

        if (!legacyClientConfig.isEnabled()) {
            log.warn("event=subscribe.legacy_rejected service={} reason=legacy_disabled traceId={}", serviceName, traceId);
            return;
        }

        List<String> topics = legacyClientConfig.getTopicsForService(serviceName);
        if (topics.isEmpty()) {
            log.error("Unknown legacy service: {}. No topics configured. traceId={}", serviceName, traceId);
            return;
        }

        log.info("event=subscribe.processed mode=legacy clientId={} service={} topicCount={} traceId={}",
                clientId, serviceName, topics.size(), traceId);

        // Register consumer for ALL topics (serviceName is used as the consumer group)
        // Mark as legacy (isLegacy=true) so delivery pipeline uses multi-topic merge
        int newRegistrations = 0;
        for (String topic : topics) {
            boolean isNew = remoteConsumers.registerConsumer(clientId, topic, serviceName, true, traceId);
            if (isNew) {
                newRegistrations++;
            }
            log.debug("Registered legacy consumer {} for topic={}, group={}, traceId={}",
                    clientId, topic, serviceName, traceId);
        }

        // Record metrics only for new registrations
        for (int i = 0; i < newRegistrations; i++) {
            metrics.recordConsumerConnection();
        }

        log.info("event=subscribe.legacy_registered clientId={} service={} topicCount={} traceId={}",
                clientId, topics.size(), serviceName, traceId);

        // Send READY to start delivery (legacy consumers deliver only after READY_ACK)
        if (newRegistrations > 0) {
            // Check if any of this consumer's topics have an active refresh in progress.
            // If so, skip the startup READY handshake: mark the consumer ready immediately
            // (so data can flow) and let the refresh workflow send READY when replay is done.
            // Only send the startup READY when no refresh is active.
            boolean anyRefreshActive = topics.stream()
                    .anyMatch(t -> refreshCoordinator.isRefreshActive(t));

            if (anyRefreshActive) {
                // Refresh in progress — bypass startup READY, mark consumer ready directly
                remoteConsumers.markLegacyConsumerReady(clientId);
                log.info("event=subscribe.ready_bypass clientId={} reason=refresh_active traceId={}",
                        clientId, traceId);

                for (String topic : topics) {
                    RefreshContext refreshContext = refreshCoordinator.getRefreshStatus(topic);
                    if (refreshContext == null) continue;

                    String groupTopic = serviceName + ":" + topic;
                    RefreshState state = refreshContext.getState();

                    if (state == RefreshState.READY_SENT && !refreshContext.allReadyAcksReceived()) {
                        // Already past replay — send refresh READY directly
                        log.info("event=subscribe.refresh_ready_sent clientId={} topic={} traceId={}",
                                clientId, topic, traceId);
                        remoteConsumers.sendRefreshReadyToConsumer(clientId, topic);
                    } else if (state == RefreshState.RESET_SENT || state == RefreshState.REPLAYING) {
                        // Still replaying — register as late joiner so refresh READY is sent when replay completes
                        refreshCoordinator.registerLateJoiningConsumer(topic, groupTopic);
                    }
                }
            } else {
                // No refresh active — standard startup READY handshake
                remoteConsumers.sendStartupReadyToLegacyConsumer(clientId);
            }
        }

        // Send ACK
        sendSubscribeAck(clientId, message, true, traceId);
    }

    /**
     * Send SUBSCRIBE acknowledgment.
     *
     * @param isLegacy true if client is using legacy Event protocol
     */
    private void sendSubscribeAck(String clientId, BrokerMessage message, boolean isLegacy, String traceId) {
        // Legacy clients don't expect SUBSCRIBE ACK in their protocol
        if (isLegacy) {
            log.debug("Legacy client registered, no SUBSCRIBE ACK sent (legacy protocol doesn't expect it), traceId={}", traceId);
            return;
        }

        // Modern client: send BrokerMessage ACK
        BrokerMessage ack = new BrokerMessage(
            BrokerMessage.MessageType.ACK,
            message.getMessageId(),
            new byte[0]
        );

        log.debug("Sending ACK to {}: type={}, messageId={}, traceId={}",
                 clientId, ack.getType(), ack.getMessageId(), traceId);

        server.send(clientId, ack).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Failed to send ACK to {}, traceId={}", clientId, traceId, ex);
            } else {
                log.info("event=subscribe.ack_sent clientId={} traceId={}", clientId, traceId);
            }
        });
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
}
