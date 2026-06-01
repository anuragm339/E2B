package com.messaging.broker.handler;

import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.monitoring.LogMdc;
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
        try (LogMdc.Scope ignored = LogMdc.withTrace(traceId)) {
            ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());

            // Handle empty payload for legacy startup READY_ACK
            if (buffer.remaining() == 0) {
                try (LogMdc.Scope clientScope = LogMdc.with(traceId, null, null, clientId)) {
                    remoteConsumers.markLegacyConsumerReady(clientId);
                    log.info("event=ready_ack.processed mode=legacy_startup clientId={}", clientId);
                    log.debug("event=ready_ack.ack_suppressed mode=legacy_startup reason=legacy_protocol clientId={}", clientId);
                }
                return;
            }

            // Validate payload size
            if (buffer.remaining() < 8) {
                log.error("event=ready_ack.invalid_payload clientId={} reason=payload_too_small bytes={}",
                        clientId, buffer.remaining());
                server.closeConnection(clientId);
                return;
            }

            // Read topic
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 65535) {
                log.error("event=ready_ack.invalid_payload clientId={} reason=topic_length topicLen={}",
                        clientId, topicLen);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < topicLen + 4) {
                log.error("event=ready_ack.invalid_payload clientId={} reason=topic_bytes_missing remaining={} needed={}",
                        clientId, buffer.remaining(), topicLen + 4);
                server.closeConnection(clientId);
                return;
            }

            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            // Read group
            int groupLen = buffer.getInt();
            if (groupLen < 0 || groupLen > 65535) {
                log.error("event=ready_ack.invalid_payload clientId={} reason=group_length groupLen={}",
                        clientId, groupLen);
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
                    try (LogMdc.Scope scope = LogMdc.with(traceId, topic, null, clientId)) {
                        log.warn("event=ready_ack.group_resolution_failed clientId={} topic={}", clientId, topic);
                    }
                    return; // soft-fail, do not close connection
                }
                try (LogMdc.Scope scope = LogMdc.with(traceId, topic, group, clientId)) {
                    log.debug("event=ready_ack.group_resolved mode=legacy clientId={} topic={} group={}",
                            clientId, topic, group);
                }
            } else {
                if (buffer.remaining() < groupLen) {
                    log.error("event=ready_ack.invalid_payload clientId={} reason=group_bytes_missing remaining={} needed={}",
                            clientId, buffer.remaining(), groupLen);
                    server.closeConnection(clientId);
                    return;
                }
                byte[] groupBytes = new byte[groupLen];
                buffer.get(groupBytes);
                group = new String(groupBytes, StandardCharsets.UTF_8);
            }

            // Construct consumerGroupTopic identifier
            String consumerGroupTopic = group + ":" + topic;

            try (LogMdc.Scope scope = LogMdc.with(traceId, topic, group, clientId)) {
                log.debug("event=ready_ack.consumer_resolved clientId={} consumerKey={}", clientId, consumerGroupTopic);

                // Check if this is startup READY_ACK or refresh READY_ACK
                boolean isRefreshActive = topic != null && !topic.isEmpty() &&
                        dataRefreshCoordinator.isRefreshActive(topic);

                if (isRefreshActive) {
                    log.info("event=ready_ack.processed mode=refresh clientId={} topic={} group={}",
                            clientId, topic, group);
                    dataRefreshCoordinator.handleReadyAck(consumerGroupTopic, topic, traceId);
                } else {
                    String consumerKey = clientId + ":" + topic + ":" + group;
                    boolean isLegacy = remoteConsumers.isLegacyConsumer(consumerKey);

                    if (isLegacy || topic.isEmpty()) {
                        remoteConsumers.markLegacyConsumerReady(clientId);
                        log.info("event=ready_ack.processed mode=legacy clientId={} topic={} group={}",
                                clientId, topic, group);
                    } else {
                        remoteConsumers.markModernConsumerTopicReady(clientId, topic, group);
                        log.info("event=ready_ack.processed mode=modern clientId={} topic={} group={}",
                                clientId, topic, group);
                    }
                }

                log.debug("event=ready_ack.ack_suppressed clientId={} reason=protocol_compatibility", clientId);
            }

        } catch (Exception e) {
            log.error("event=ready_ack.failed clientId={}", clientId, e);
        }
    }
}
