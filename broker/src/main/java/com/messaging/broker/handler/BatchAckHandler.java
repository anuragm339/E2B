package com.messaging.broker.handler;

import com.messaging.broker.handler.MessageHandler;
import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.consumer.ConsumerRegistry;
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
        try {
            // Legacy client detection: Empty payload indicates legacy merged batch ACK
            if (message.getPayload().length == 0) {
                handleLegacyBatchAck(clientId, traceId);
                return;
            }

            // Parse payload: [topicLen:4][topic:var][groupLen:4][group:var]
            ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());

            // Validate payload size
            if (buffer.remaining() < 8) {
                log.error("BATCH_ACK payload too small from {}: {} bytes (expected >= 8), traceId={}",
                         clientId, buffer.remaining(), traceId);
                server.closeConnection(clientId);
                return;
            }

            // Read topic
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 65535) {
                log.error("Invalid topicLen in BATCH_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, topicLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < topicLen + 4) {
                log.error("Not enough data in BATCH_ACK from {}: remaining={}, needed={}, traceId={}",
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
                log.error("Invalid groupLen in BATCH_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, groupLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            if (buffer.remaining() < groupLen) {
                log.error("Not enough data for group in BATCH_ACK from {}: remaining={}, needed={}, traceId={}",
                         clientId, buffer.remaining(), groupLen, traceId);
                server.closeConnection(clientId);
                return;
            }

            byte[] groupBytes = new byte[groupLen];
            buffer.get(groupBytes);
            String group = new String(groupBytes, StandardCharsets.UTF_8);

            log.debug("Received BATCH_ACK from client: {}, topic: {}, group: {}, traceId={}", clientId, topic, group, traceId);

            // Offload ACK processing to dedicated executor to prevent Netty event loop blocking
            final String finalTopic = topic;
            final String finalGroup = group;
            ackExecutor.execute(() -> {
                try {
                    remoteConsumers.handleBatchAck(clientId, finalTopic, finalGroup);
                } catch (Exception e) {
                    log.error("Error processing BATCH_ACK from {}, traceId={}", clientId, traceId, e);
                }
            });

        } catch (Exception e) {
            log.error("Error handling BATCH_ACK from {}, traceId={}", clientId, traceId, e);
        }
    }

    /**
     * Handle BATCH_ACK from legacy client (merged batch from multiple topics).
     */
    private void handleLegacyBatchAck(String clientId, String traceId) {
        log.debug("Received legacy BATCH_ACK from client: {}, traceId={}", clientId, traceId);

        // Look up legacy consumers to find the group
        List<RemoteConsumer> legacyConsumers = remoteConsumers.getLegacyConsumersForClient(clientId);

        if (legacyConsumers.isEmpty()) {
            log.warn("Received legacy BATCH_ACK from unknown client: {}, traceId={}", clientId, traceId);
            return;
        }

        // All legacy consumers for a client share the same group (serviceName)
        String group = legacyConsumers.get(0).getGroup();

        log.debug("Routing legacy BATCH_ACK to group: {}, traceId={}", group, traceId);

        // Offload ACK processing to dedicated executor
        ackExecutor.execute(() -> {
            try {
                remoteConsumers.handleLegacyBatchAck(clientId, group);
            } catch (Exception e) {
                log.error("Error processing legacy BATCH_ACK from {}, traceId={}", clientId, traceId, e);
            }
        });
    }
}
