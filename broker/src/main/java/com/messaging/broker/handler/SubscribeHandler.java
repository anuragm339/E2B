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
            RefreshCoordinator refreshCoordinator,
            ObjectMapper objectMapper) {
        this.server = server;
        this.remoteConsumers = remoteConsumers;
        this.metrics = metrics;
        this.legacyClientConfig = legacyClientConfig;
        this.refreshCoordinator = refreshCoordinator;
        this.objectMapper = objectMapper;
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
            boolean isLegacy = false;
            if (json.has("isLegacy")) {
                JsonNode isLegacyNode = json.get("isLegacy");
                if (!isLegacyNode.isBoolean()) {
                    log.warn("event=subscribe.invalid_field clientId={} field=isLegacy expected=boolean got={} traceId={}",
                            clientId, isLegacyNode.getNodeType(), traceId);
                }
                isLegacy = isLegacyNode.asBoolean();
            }

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

            // Send READY to start delivery (modern consumers deliver only after READY_ACK).
            // When a refresh is active we must NOT send startup READY — the consumer's READY_ACK
            // would be misidentified as a refresh READY_ACK by ReadyAckHandler (which checks
            // isRefreshActive at ACK time). Instead, mirror the legacy path: bypass the startup
            // handshake and mark the consumer ready directly.
            if (isNew) {
                RefreshContext refreshContext = refreshCoordinator.getRefreshStatus(topic);
                if (refreshContext != null) {
                    RefreshState state = refreshContext.getState();
                    String groupTopic = group + ":" + topic;
                    if (state == RefreshState.RESET_SENT || state == RefreshState.REPLAYING) {
                        // Register as late joiner BEFORE opening the delivery gate. This ensures
                        // that if the scheduler fires checkReplayProgress between these two calls,
                        // our consumer is already in receivedResetAcks and will receive READY from
                        // the scheduler's sendReady broadcast rather than missing it.
                        boolean registered = refreshCoordinator.registerLateJoiningConsumer(topic, groupTopic);
                        // Open delivery gate only after registration is complete.
                        remoteConsumers.markModernConsumerTopicReady(clientId, topic, group);
                        if (!registered) {
                            // State advanced past REPLAYING while registerLateJoiningConsumer ran
                            // (C2 post-recordResetAck re-read detected the race). Check current state:
                            //  • READY_SENT → send refresh READY directly; consumer is in receivedResetAcks
                            //    so checkReadyAckTimeout retries will also reach it.
                            //  • COMPLETED/ABORTED/null → refresh is done; send normal startup READY.
                            RefreshContext currentCtx = refreshCoordinator.getRefreshStatus(topic);
                            if (currentCtx != null && currentCtx.getState() == RefreshState.READY_SENT) {
                                remoteConsumers.sendRefreshReadyToConsumer(clientId, topic);
                                log.info("event=subscribe.late_joiner_ready_fallback mode=modern clientId={} topic={} group={} traceId={}",
                                        clientId, topic, group, traceId);
                            } else {
                                // Terminal state or context cleaned up — treat as no active refresh.
                                remoteConsumers.sendStartupReadyToModernConsumer(clientId, topic, group);
                                log.info("event=subscribe.startup_ready_after_completed mode=modern clientId={} topic={} group={} traceId={}",
                                        clientId, topic, group, traceId);
                            }
                        } else {
                            log.info("event=subscribe.ready_bypass mode=modern clientId={} topic={} group={} reason=refresh_active state={} traceId={}",
                                    clientId, topic, group, state, traceId);
                        }
                    } else if (state == RefreshState.READY_SENT && !refreshContext.allReadyAcksReceived()) {
                        // Replay is done, READY broadcast is in flight.
                        // C1: Register in receivedResetAcks BEFORE opening gate so checkReadyAckTimeout
                        // re-broadcasts include this consumer if the direct send below is lost in transit.
                        // registerLateJoiningConsumer returns false for READY_SENT (no-op on state machine)
                        // but still records the ack — that is all we need here.
                        refreshCoordinator.registerLateJoiningConsumer(topic, groupTopic);
                        remoteConsumers.markModernConsumerTopicReady(clientId, topic, group);
                        remoteConsumers.sendRefreshReadyToConsumer(clientId, topic);
                        log.info("event=subscribe.refresh_ready_sent mode=modern clientId={} topic={} group={} traceId={}",
                                clientId, topic, group, traceId);
                    } else {
                        // COMPLETED, ABORTED, or any future terminal state: the context still exists
                        // in activeRefreshes for up to 60 seconds after completion (cleanup delay).
                        // Treat the same as no active refresh — send the normal startup READY.
                        remoteConsumers.sendStartupReadyToModernConsumer(clientId, topic, group);
                    }
                } else {
                    // No refresh active — standard startup READY handshake
                    remoteConsumers.sendStartupReadyToModernConsumer(clientId, topic, group);
                }
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

        // Send ACK before READY logic — matches modern path ordering. Legacy protocol does not
        // send an ACK on the wire (sendSubscribeAck is a no-op for isLegacy=true), but keeping
        // the ordering consistent avoids surprises if the protocol is extended.
        sendSubscribeAck(clientId, message, true, traceId);

        // Send READY to start delivery (legacy consumers deliver only after READY_ACK)
        if (newRegistrations > 0) {
            // isRefreshActive returns true for up to 60s after COMPLETED (cleanup delay), so
            // we must inspect the per-topic state inside the loop rather than relying on this flag alone.
            boolean anyRefreshActive = topics.stream()
                    .anyMatch(t -> refreshCoordinator.isRefreshActive(t));

            if (anyRefreshActive) {
                // PASS 1: Register all late joiners BEFORE opening the delivery gate.
                // This ensures the scheduler's sendReady snapshot includes this consumer
                // if checkReplayProgress fires between registration and gate-open.
                // C1: also register for READY_SENT topics so checkReadyAckTimeout retries reach us.
                for (String topic : topics) {
                    RefreshContext refreshContext = refreshCoordinator.getRefreshStatus(topic);
                    if (refreshContext == null) continue;
                    RefreshState state = refreshContext.getState();
                    if (state == RefreshState.RESET_SENT || state == RefreshState.REPLAYING
                            || state == RefreshState.READY_SENT) {
                        // registerLateJoiningConsumer enforces !allReadyAcksReceived() internally
                        // for the READY_SENT branch — safe to call unconditionally here.
                        refreshCoordinator.registerLateJoiningConsumer(topic, serviceName + ":" + topic);
                    }
                }

                // Open delivery gate only after all registrations are complete.
                remoteConsumers.markLegacyConsumerReady(clientId);
                log.info("event=subscribe.ready_bypass clientId={} reason=refresh_active traceId={}",
                        clientId, traceId);

                // PASS 2: Send READY signals; detect if all contexts are in terminal states.
                boolean anyActiveRefreshHandled = false;
                for (String topic : topics) {
                    // Re-read state — may have advanced since PASS 1.
                    RefreshContext refreshContext = refreshCoordinator.getRefreshStatus(topic);
                    if (refreshContext == null) continue;
                    RefreshState state = refreshContext.getState();

                    if (state == RefreshState.READY_SENT && !refreshContext.allReadyAcksReceived()) {
                        // Already past replay — send refresh READY directly.
                        // C1: Consumer was registered in receivedResetAcks in PASS 1, so any future
                        // checkReadyAckTimeout re-broadcast will also reach it if this send is lost.
                        log.info("event=subscribe.refresh_ready_sent clientId={} topic={} traceId={}",
                                clientId, topic, traceId);
                        remoteConsumers.sendRefreshReadyToConsumer(clientId, topic);
                        anyActiveRefreshHandled = true;
                    } else if (state == RefreshState.RESET_SENT || state == RefreshState.REPLAYING) {
                        // Registered in PASS 1. Check for the C2 race where state advanced to
                        // READY_SENT during registerLateJoiningConsumer's post-recordResetAck re-read.
                        // Do NOT set anyActiveRefreshHandled=true if the re-read shows COMPLETED/ABORTED
                        // — that would block the startup READY fallback (M2 fix).
                        RefreshContext currentCtx = refreshCoordinator.getRefreshStatus(topic);
                        if (currentCtx != null && currentCtx.getState() == RefreshState.READY_SENT) {
                            remoteConsumers.sendRefreshReadyToConsumer(clientId, topic);
                            log.info("event=subscribe.late_joiner_ready_fallback clientId={} topic={} traceId={}",
                                    clientId, topic, traceId);
                            anyActiveRefreshHandled = true;
                        } else if (currentCtx != null
                                && (currentCtx.getState() == RefreshState.RESET_SENT
                                        || currentCtx.getState() == RefreshState.REPLAYING)) {
                            // Still in an active non-terminal state — READY will arrive via broadcast.
                            anyActiveRefreshHandled = true;
                        }
                        // COMPLETED/ABORTED: do NOT set anyActiveRefreshHandled — fall through to
                        // the startup READY below so the consumer is not left without a READY signal.
                    }
                    // COMPLETED/ABORTED seen in the PASS 2 initial read: also do not set the flag.
                }

                if (!anyActiveRefreshHandled) {
                    // All topics' contexts were COMPLETED or ABORTED (60s cleanup window).
                    // Treat identically to no active refresh — send normal startup READY.
                    log.info("event=subscribe.startup_ready_after_completed clientId={} traceId={}", clientId, traceId);
                    remoteConsumers.sendStartupReadyToLegacyConsumer(clientId);
                }
            } else {
                // No refresh active — standard startup READY handshake
                remoteConsumers.sendStartupReadyToLegacyConsumer(clientId);
            }
        }
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
        String value = field.asText();
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                String.format("Field '%s' is blank from client %s", fieldName, clientId)
            );
        }
        return value;
    }
}
