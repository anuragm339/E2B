package com.messaging.broker.handler;

import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.model.BrokerMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Handles READY_ACK messages during startup and data refresh workflows.
 *
 * Supports:
 * - Empty payload: Legacy startup READY_ACK
 * - Topic+Group payload: Modern startup or refresh READY_ACK
 */
@Singleton
public class ReadyAckHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(ReadyAckHandler.class);

    private final NetworkServer server;
    private final ConsumerRegistry remoteConsumers;
    private final RefreshCoordinator dataRefreshCoordinator;

    @Inject
    public ReadyAckHandler(
            NetworkServer server,
            ConsumerRegistry remoteConsumers,
            RefreshCoordinator dataRefreshCoordinator) {
        this.server = server;
        this.remoteConsumers = remoteConsumers;
        this.dataRefreshCoordinator = dataRefreshCoordinator;
    }

    @Override
    public BrokerMessage.MessageType getMessageType() {
        return BrokerMessage.MessageType.READY_ACK;
    }

    @Override
    public void handle(String clientId, BrokerMessage message, String traceId) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());

            // Handle empty payload for legacy startup READY_ACK
            if (buffer.remaining() == 0) {
                remoteConsumers.markLegacyConsumerReady(clientId);
                log.info("event=ready_ack.processed mode=legacy_startup clientId={} traceId={}", clientId, traceId);

                // Do NOT send ACK back - legacy clients disconnect when receiving unexpected ACK
                log.debug("Legacy client {} ready - no ACK sent (legacy protocol), traceId={}", clientId, traceId);
                return;
            }

            // Validate payload size
            if (buffer.remaining() < 8) {
                log.error("READY_ACK payload too small from {}: {} bytes (expected >= 8 or 0), traceId={}",
                         clientId, buffer.remaining(), traceId);
                server.closeConnection(clientId);
                return;
            }

            // Read topic
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 65535) {
                log.error("Invalid topicLen in READY_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, topicLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < topicLen + 4) {
                log.error("Not enough data in READY_ACK from {}: remaining={}, needed={}, traceId={}",
                         clientId, buffer.remaining(), topicLen + 4, traceId);
                server.closeConnection(clientId);
                return;
            }

            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            // Read group
            int groupLen = buffer.getInt();
            if (groupLen < 0 || groupLen > 65535) {
                log.error("Invalid groupLen in READY_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, groupLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            String group;
            if (groupLen == 0) {
                // Legacy client — group not included in payload; derive from consumer registry
                group = remoteConsumers.getLegacyConsumersForClient(clientId).stream()
                        .filter(c -> c.getTopic().equals(topic))
                        .map(c -> c.getGroup())
                        .findFirst()
                        .orElse(null);
                if (group == null) {
                    log.warn("event=ready_ack.group_resolution_failed clientId={} topic={} traceId={}",
                             clientId, topic, traceId);
                    return; // soft-fail, do not close connection
                }
                log.debug("READY_ACK legacy fallback: resolved group={} for clientId={}, topic={}",
                          group, clientId, topic);
            } else {
                if (buffer.remaining() < groupLen) {
                    log.error("Not enough data for group in READY_ACK from {}: remaining={}, needed={}, traceId={}",
                             clientId, buffer.remaining(), groupLen, traceId);
                    server.closeConnection(clientId);
                    return;
                }
                byte[] groupBytes = new byte[groupLen];
                buffer.get(groupBytes);
                group = new String(groupBytes, StandardCharsets.UTF_8);
            }

            // Construct consumerGroupTopic identifier
            String consumerGroupTopic = group + ":" + topic;

            log.debug("Mapped consumer {} to group:topic identifier: {}, traceId={}", clientId, consumerGroupTopic, traceId);

            // Check if this is startup READY_ACK or refresh READY_ACK
            boolean isRefreshActive = topic != null && !topic.isEmpty() &&
                    dataRefreshCoordinator.isRefreshActive(topic);

            if (isRefreshActive) {
                // Refresh READY_ACK - delegate to RefreshCoordinator
                log.info("event=ready_ack.processed mode=refresh clientId={} topic={} group={} traceId={}",
                        clientId, topic, group, traceId);
                dataRefreshCoordinator.handleReadyAck(consumerGroupTopic, topic, traceId);
            } else {
                // Startup READY_ACK - mark consumer as ready
                // Determine if this is a legacy or modern consumer
                String consumerKey = clientId + ":" + topic + ":" + group;
                boolean isLegacy = remoteConsumers.isLegacyConsumer(consumerKey);

                if (isLegacy || topic.isEmpty()) {
                    // Legacy consumer - mark entire client as ready
                    remoteConsumers.markLegacyConsumerReady(clientId);
                    log.info("event=ready_ack.processed mode=legacy clientId={} topic={} group={} traceId={}",
                            clientId, topic, group, traceId);
                } else {
                    // Modern consumer - mark specific topic as ready
                    remoteConsumers.markModernConsumerTopicReady(clientId, topic, group);
                    log.info("event=ready_ack.processed mode=modern clientId={} topic={} group={} traceId={}",
                            clientId, topic, group, traceId);
                }
            }

            // Do NOT send ACK back - legacy protocol compatibility
            log.debug("Consumer {} marked as READY - no ACK sent (legacy protocol compatibility), traceId={}", clientId, traceId);

        } catch (Exception e) {
            log.error("Error handling READY_ACK from {}, traceId={}", clientId, traceId, e);
        }
    }
}
