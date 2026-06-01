package com.messaging.broker.handler;

import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.ConsumerRegistrationService;
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
        try (LogMdc.Scope ignored = LogMdc.withTrace(traceId)) {
            // Parse payload: [topicLen:4][topic:var][groupLen:4][group:var]
            ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());

            // Validate payload size
            if (buffer.remaining() < 8) {
                log.error("event=reset_ack.invalid_payload clientId={} reason=payload_too_small bytes={}",
                        clientId, buffer.remaining());
                server.closeConnection(clientId);
                return;
            }

            // Read topic
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 65535) {
                log.error("event=reset_ack.invalid_payload clientId={} reason=topic_length topicLen={}",
                        clientId, topicLen);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < topicLen + 4) {
                log.error("event=reset_ack.invalid_payload clientId={} reason=topic_bytes_missing remaining={} needed={}",
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
                log.error("event=reset_ack.invalid_payload clientId={} reason=group_length groupLen={}",
                        clientId, groupLen);
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
                    try (LogMdc.Scope scope = LogMdc.with(traceId, topic, null, clientId)) {
                        log.warn("event=reset_ack.group_resolution_failed clientId={} topic={}", clientId, topic);
                    }
                    return; // soft-fail, do not close connection
                }
                try (LogMdc.Scope scope = LogMdc.with(traceId, topic, group, clientId)) {
                    log.debug("event=reset_ack.group_resolved mode=legacy clientId={} topic={} group={}",
                            clientId, topic, group);
                }
            } else {
                if (buffer.remaining() < groupLen) {
                    log.error("event=reset_ack.invalid_payload clientId={} reason=group_bytes_missing remaining={} needed={}",
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
                log.debug("event=reset_ack.consumer_resolved clientId={} consumerKey={}", clientId, consumerGroupTopic);

                log.info("event=reset_ack.processed clientId={} topic={} group={}", clientId, topic, group);
                dataRefreshCoordinator.handleResetAck(consumerGroupTopic, clientId, topic, traceId);

                log.debug("event=reset_ack.ack_suppressed clientId={} reason=protocol_compatibility", clientId);
            }

        } catch (Exception e) {
            log.error("event=reset_ack.failed clientId={}", clientId, e);
        }
    }
}
