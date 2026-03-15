package com.messaging.broker.handler;

import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.ConsumerRegistrationService;
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
 * Handles RESET_ACK messages during data refresh workflow.
 */
@Singleton
public class ResetAckHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(ResetAckHandler.class);

    private final NetworkServer server;
    private final RefreshCoordinator dataRefreshCoordinator;
    private final ConsumerRegistrationService registrationService;

    @Inject
    public ResetAckHandler(
            NetworkServer server,
            RefreshCoordinator dataRefreshCoordinator,
            ConsumerRegistrationService registrationService) {
        this.server = server;
        this.dataRefreshCoordinator = dataRefreshCoordinator;
        this.registrationService = registrationService;
    }

    @Override
    public BrokerMessage.MessageType getMessageType() {
        return BrokerMessage.MessageType.RESET_ACK;
    }

    @Override
    public void handle(String clientId, BrokerMessage message, String traceId) {
        try {
            // Parse payload: [topicLen:4][topic:var][groupLen:4][group:var]
            ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());

            // Validate payload size
            if (buffer.remaining() < 8) {
                log.error("RESET_ACK payload too small from {}: {} bytes (expected >= 8), traceId={}",
                         clientId, buffer.remaining(), traceId);
                server.closeConnection(clientId);
                return;
            }

            // Read topic
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 65535) {
                log.error("Invalid topicLen in RESET_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, topicLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < topicLen + 4) {
                log.error("Not enough data in RESET_ACK from {}: remaining={}, needed={}, traceId={}",
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
                log.error("Invalid groupLen in RESET_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, groupLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            String group;
            if (groupLen == 0) {
                // Legacy client — group was not included in the ACK payload; derive it from the registry
                group = registrationService.getConsumersByClient(clientId).stream()
                        .filter(c -> c.getTopic().equals(topic))
                        .map(c -> c.getGroup())
                        .findFirst()
                        .orElse(null);
                if (group == null) {
                    log.warn("RESET_ACK from {}: topic={} found but no registered consumer group, traceId={}",
                             clientId, topic, traceId);
                    return; // soft-fail, do not close connection
                }
                log.debug("RESET_ACK legacy fallback: resolved group={} for clientId={}, topic={}",
                          group, clientId, topic);
            } else {
                if (buffer.remaining() < groupLen) {
                    log.error("Not enough data for group in RESET_ACK from {}: remaining={}, needed={}, traceId={}",
                             clientId, buffer.remaining(), groupLen, traceId);
                    server.closeConnection(clientId);
                    return;
                }
                byte[] groupBytes = new byte[groupLen];
                buffer.get(groupBytes);
                group = new String(groupBytes, StandardCharsets.UTF_8);
            }

            log.info("Received RESET ACK from client: {} for topic: {}, group: {}, traceId={}", clientId, topic, group, traceId);

            // Construct consumerGroupTopic identifier
            String consumerGroupTopic = group + ":" + topic;

            log.info("Mapped consumer {} to group:topic identifier: {}, traceId={}", clientId, consumerGroupTopic, traceId);

            // Delegate to RefreshCoordinator
            dataRefreshCoordinator.handleResetAck(consumerGroupTopic, clientId, topic, traceId);

            // Do NOT send ACK back - legacy protocol compatibility
            log.debug("RESET_ACK processed for {} - no ACK sent (legacy protocol compatibility), traceId={}", clientId, traceId);

        } catch (Exception e) {
            log.error("Error handling RESET_ACK from {}, traceId={}", clientId, traceId, e);
        }
    }
}
