package com.messaging.broker.handler;

import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.consumer.ConsumerRegistry;
import com.messaging.broker.monitoring.LogMdc;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.model.BrokerMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Handles BATCH_ACK messages - consumer acknowledges receipt and processing of a batch.
 *
 * Supports:
 * - Empty payload: Legacy merged batch ACK (multi-topic)
 * - Topic+Group payload: Modern single-topic batch ACK
 */
@Singleton
public class BatchAckHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(BatchAckHandler.class);

    private final NetworkServer server;
    private final ConsumerRegistry remoteConsumers;
    private final ExecutorService ackExecutor;

    @Inject
    public BatchAckHandler(
            NetworkServer server,
            ConsumerRegistry remoteConsumers,
            @jakarta.inject.Named("ackExecutor") ExecutorService ackExecutor) {
        this.server = server;
        this.remoteConsumers = remoteConsumers;
        this.ackExecutor = ackExecutor;
    }

    @Override
    public BrokerMessage.MessageType getMessageType() {
        return BrokerMessage.MessageType.BATCH_ACK;
    }

    @Override
    public void handle(String clientId, BrokerMessage message, String traceId) {
        try (LogMdc.Scope ignored = LogMdc.withTrace(traceId)) {
            // Legacy client detection: Empty payload indicates legacy merged batch ACK
            if (message.getPayload().length == 0) {
                handleLegacyBatchAck(clientId, traceId);
                return;
            }

            // Parse payload: [topicLen:4][topic:var][groupLen:4][group:var]
            ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());

            // Validate payload size
            if (buffer.remaining() < 8) {
                log.error("event=batch_ack.invalid_payload clientId={} reason=payload_too_small bytes={}",
                        clientId, buffer.remaining());
                server.closeConnection(clientId);
                return;
            }

            // Read topic
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 65535) {
                log.error("event=batch_ack.invalid_payload clientId={} reason=topic_length topicLen={}",
                        clientId, topicLen);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < topicLen + 4) {
                log.error("event=batch_ack.invalid_payload clientId={} reason=topic_bytes_missing remaining={} needed={}",
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
                log.error("event=batch_ack.invalid_payload clientId={} reason=group_length groupLen={}",
                        clientId, groupLen);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < groupLen) {
                log.error("event=batch_ack.invalid_payload clientId={} reason=group_bytes_missing remaining={} needed={}",
                        clientId, buffer.remaining(), groupLen);
                server.closeConnection(clientId);
                return;
            }

            byte[] groupBytes = new byte[groupLen];
            buffer.get(groupBytes);
            String group = new String(groupBytes, StandardCharsets.UTF_8);

            try (LogMdc.Scope scope = LogMdc.with(traceId, topic, group, clientId)) {
                log.debug("event=batch_ack.received mode=modern clientId={} topic={} group={}", clientId, topic, group);

                // Offload ACK processing to dedicated executor to prevent Netty event loop blocking
                final String finalTopic = topic;
                final String finalGroup = group;
                submitAckTask(clientId, "modern", () -> {
                    try (LogMdc.Scope asyncScope = LogMdc.with(traceId, finalTopic, finalGroup, clientId)) {
                        log.debug("event=batch_ack.processing_enqueued mode=modern clientId={} topic={} group={}",
                                clientId, finalTopic, finalGroup);
                        remoteConsumers.handleBatchAck(clientId, finalTopic, finalGroup);
                    } catch (Exception e) {
                        log.error("event=batch_ack.processing_failed mode=modern clientId={} topic={} group={}",
                                clientId, finalTopic, finalGroup, e);
                    }
                });
            }

        } catch (Exception e) {
            log.error("event=batch_ack.failed clientId={}", clientId, e);
        }
    }

    /**
     * Handle BATCH_ACK from legacy client (merged batch from multiple topics).
     */
    private void handleLegacyBatchAck(String clientId, String traceId) {
        try (LogMdc.Scope scope = LogMdc.with(traceId, null, null, clientId)) {
            log.debug("event=batch_ack.received mode=legacy clientId={}", clientId);

            // Look up legacy consumers to find the group
            List<RemoteConsumer> legacyConsumers = remoteConsumers.getLegacyConsumersForClient(clientId);

            if (legacyConsumers.isEmpty()) {
                log.warn("event=batch_ack.unknown_legacy_client clientId={}", clientId);
                return;
            }

            // All legacy consumers for a client share the same group (serviceName)
            String group = legacyConsumers.get(0).getGroup();

            try (LogMdc.Scope groupScope = LogMdc.with(traceId, null, group, clientId)) {
                log.debug("event=batch_ack.processing_enqueued mode=legacy clientId={} group={}", clientId, group);

                // Offload ACK processing to dedicated executor
                submitAckTask(clientId, "legacy", () -> {
                    try (LogMdc.Scope asyncScope = LogMdc.with(traceId, null, group, clientId)) {
                        remoteConsumers.handleLegacyBatchAck(clientId, group);
                    } catch (Exception e) {
                        log.error("event=batch_ack.processing_failed mode=legacy clientId={} group={}",
                                clientId, group, e);
                    }
                });
            }
        }
    }

    private void submitAckTask(String clientId, String mode, Runnable task) {
        try {
            ackExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            log.error("event=batch_ack.rejected mode={} clientId={} action=close_connection",
                    mode, clientId, e);
            server.closeConnection(clientId);
        }
    }
}
