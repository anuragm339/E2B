package com.messaging.broker.core;

import com.messaging.broker.consumer.AdaptiveBatchDeliveryManager;
import com.messaging.broker.consumer.ConsumerDeliveryManager;
import com.messaging.broker.consumer.ConsumerOffsetTracker;
import com.messaging.broker.consumer.RemoteConsumerRegistry;
import com.messaging.broker.legacy.LegacyClientConfig;
import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.broker.refresh.DataRefreshManager;
import com.messaging.broker.registry.TopologyManager;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main broker service that wires storage and network together
 */
@Singleton
public class BrokerService implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerDeliveryManager consumerDelivery;
    private final RemoteConsumerRegistry remoteConsumers;
    private final AdaptiveBatchDeliveryManager adaptiveDeliveryManager;
    private final TopologyManager topologyManager;
    private final BrokerMetrics metrics;
    private final ConsumerOffsetTracker offsetTracker;
    private final DataRefreshManager dataRefreshManager;
    private final LegacyClientConfig legacyClientConfig;
    private final int serverPort;
    private final ObjectMapper objectMapper;

    // Dedicated executor for ACK processing to prevent event loop blocking
    private final ExecutorService ackExecutor;

    @Inject
    public BrokerService(
            StorageEngine storage,
            NetworkServer server,
            ConsumerDeliveryManager consumerDelivery,
            RemoteConsumerRegistry remoteConsumers,
            AdaptiveBatchDeliveryManager adaptiveDeliveryManager,
            TopologyManager topologyManager,
            BrokerMetrics metrics,
            ConsumerOffsetTracker offsetTracker,
            DataRefreshManager dataRefreshManager,
            LegacyClientConfig legacyClientConfig,
            @Value("${broker.network.port:9092}") int serverPort) {

        this.storage = storage;
        this.server = server;
        this.consumerDelivery = consumerDelivery;
        this.remoteConsumers = remoteConsumers;
        this.adaptiveDeliveryManager = adaptiveDeliveryManager;
        this.topologyManager = topologyManager;
        this.metrics = metrics;
        this.offsetTracker = offsetTracker;
        this.dataRefreshManager = dataRefreshManager;
        this.legacyClientConfig = legacyClientConfig;
        this.serverPort = serverPort;
        this.objectMapper = new ObjectMapper();

        // Create dedicated thread pool for ACK processing (CPU cores * 2)
        // This prevents ACKs from being blocked by heavy delivery operations on Netty event loop
        int ackThreads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.ackExecutor = Executors.newFixedThreadPool(ackThreads, runnable -> {
            Thread t = new Thread(runnable);
            t.setName("ACK-Processor-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        log.info("BrokerService initialized with {} ACK processing threads", ackThreads);
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("Application started, initializing broker...");

        // Recover storage
        try {
            storage.recover();
            log.info("Storage recovered");
        } catch (Exception e) {
            log.error("Failed to recover storage", e);
            throw new RuntimeException("Storage recovery failed", e);
        }

        // Register message handler
        server.registerHandler(this::handleMessage);

        // Register disconnect handler
        server.registerDisconnectHandler(this::handleDisconnect);

        // Start network server
        server.start(serverPort);
        log.info("Broker ready on port {}", serverPort);

        // Start consumer delivery
        consumerDelivery.startDelivery();
        log.info("Consumer delivery started");

        // Start adaptive batch delivery manager (watermark-based polling)
        adaptiveDeliveryManager.start();
        log.info("Adaptive batch delivery manager started");

        // Start topology manager - it will query Cloud Registry and connect to parent
        topologyManager.onMessageReceived(this::handlePipeMessage);
        topologyManager.start();
        log.info("Topology manager started");
    }

    /**
     * Handle messages received from parent via Pipe
     */
    private void handlePipeMessage(MessageRecord record) {
        Timer.Sample e2eSample = metrics.startE2ETimer();

        try {
            log.debug("Received message from parent: key={}, type={}",
                    record.getMsgKey(), record.getEventType());

            // Calculate message size (estimate: key + data + metadata)
            long messageBytes = (record.getMsgKey() != null ? record.getMsgKey().length() : 0) +
                                (record.getData() != null ? record.getData().length() : 0) +
                                50; // metadata overhead estimate

            metrics.recordMessageReceived(messageBytes);

            // Store message in local storage
            // Use topic from record, fallback to default if not set
            String topic = record.getTopic();

            Timer.Sample storageSample = metrics.startStorageWriteTimer();
            long offset = storage.append(topic, 0, record);
            metrics.stopStorageWriteTimer(storageSample);

            metrics.recordMessageStored();
            metrics.recordTopicLastMessageTime(topic);

            log.debug("Stored message from parent: topic={}, offset={}, key={}",
                    topic, offset, record.getMsgKey());

            // Broker will discover new messages via adaptive polling (watermark-based)
            // No notification needed - Pipe is now fully decoupled from Broker

            metrics.stopE2ETimer(e2eSample);

        } catch (Exception e) {
            log.error("Error handling pipe message", e);
        }
    }

    /**
     * Handle incoming messages from clients
     */
    private void handleMessage(String clientId, BrokerMessage message) {
        log.debug("Handling message from {}: type={}, id={}",
                  clientId, message.getType(), message.getMessageId());

        switch (message.getType()) {
            case DATA:
                handleDataMessage(clientId, message);
                break;

            case SUBSCRIBE:
                handleSubscribe(clientId, message);
                break;

            case COMMIT_OFFSET:
                handleCommitOffset(clientId, message);
                break;

            case RESET_ACK:
                handleResetAck(clientId, message);
                break;

            case READY_ACK:
                handleReadyAck(clientId, message);
                break;

            case BATCH_ACK:
                handleBatchAck(clientId, message);
                break;

            default:
                log.warn("Unknown message type: {}", message.getType());
        }
    }

    /**
     * Handle DATA message - store and ACK
     */
    private void handleDataMessage(String clientId, BrokerMessage message) {
        Timer.Sample e2eSample = metrics.startE2ETimer();

        try {
            metrics.recordMessageReceived();
            metrics.recordMessageSize(message.getPayload().length);

            // Decode payload
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            // Extract fields
            String msgKey = json.has("msg_key") ? json.get("msg_key").asText() : "key_" + System.currentTimeMillis();
            String eventTypeStr = json.has("event_type") ? json.get("event_type").asText() : "MESSAGE";
            String data = json.has("data") ? json.get("data").toString() : payload;

            EventType eventType = eventTypeStr.equals("DELETE") ? EventType.DELETE : EventType.MESSAGE;

            // Determine topic (default to "default-topic" if not specified)
            String topic = json.has("topic") ? json.get("topic").asText() : "default-topic";

            // Create message record
            MessageRecord record = new MessageRecord(
                msgKey,
                eventType,
                eventType == EventType.DELETE ? null : data,
                Instant.now()
            );

            // Store
            Timer.Sample storageSample = metrics.startStorageWriteTimer();
            long offset = storage.append(topic, 0, record);
            metrics.stopStorageWriteTimer(storageSample);

            metrics.recordMessageStored();
            metrics.recordTopicLastMessageTime(topic);

            log.debug("Stored message: topic={}, offset={}, key={}, type={}",
                     topic, offset, msgKey, eventType);

            // Broker will discover new messages via adaptive polling (watermark-based)
            // No notification needed - Pipe is now fully decoupled from Broker

            metrics.stopE2ETimer(e2eSample);

            // Send ACK
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send ACK to {}", clientId, ex);
                } else {
                    log.debug("Sent ACK to {} for message {}", clientId, message.getMessageId());
                }
            });

        } catch (Exception e) {
            log.error("Error handling DATA message from {}", clientId, e);
        }
    }

    /**
     * Handle SUBSCRIBE message
     */
    /**
     * Handle SUBSCRIBE message.
     * Supports both modern and legacy formats:
     * - Modern: {"topic": "prices-v1", "group": "price-quote-group"}
     * - Legacy: {"isLegacy": true, "serviceName": "price-quote-service"}
     */
    private void handleSubscribe(String clientId, BrokerMessage message) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            // Check if this is a legacy client
            boolean isLegacy = json.has("isLegacy") && json.get("isLegacy").asBoolean();

            if (isLegacy) {
                // Legacy client - multi-topic subscription
                handleLegacySubscribe(clientId, message, json);
            } else {
                // Modern client - single topic subscription
                handleModernSubscribe(clientId, message, json);
            }

        } catch (Exception e) {
            log.error("Error handling SUBSCRIBE from {}", clientId, e);
        }
    }

    /**
     * Handle modern SUBSCRIBE: single topic, explicit group
     */
    private void handleModernSubscribe(String clientId, BrokerMessage message, JsonNode json) {
        String topic = json.get("topic").asText();
        String group = json.get("group").asText();

        log.info("Modern client {} subscribed: topic={}, group={}", clientId, topic, group);

        // Register consumer for message delivery.
        // B2-3 fix: only record connection metric for genuinely new registrations.
        // Duplicate SUBSCRIBE (same clientId+topic) on the same connection would otherwise
        // increment activeConsumers without a matching decrement on unregister.
        boolean isNew = remoteConsumers.registerConsumer(clientId, topic, group);
        if (isNew) {
            metrics.recordConsumerConnection();
        } else {
            log.warn("Duplicate SUBSCRIBE from clientId={} topic={} group={} — skipping activeConsumers increment",
                    clientId, topic, group);
        }

        log.info("Registered remote consumer {} for topic={}, group={}", clientId, topic, group);

        // Send ACK
        sendSubscribeAck(clientId, message);
    }

    /**
     * Handle legacy SUBSCRIBE: multiple topics based on serviceName
     */
    private void handleLegacySubscribe(String clientId, BrokerMessage message, JsonNode json) {
        String serviceName = json.get("serviceName").asText();

        if (!legacyClientConfig.isEnabled()) {
            log.warn("Legacy client support disabled, rejecting registration from service: {}", serviceName);
            return;
        }

        java.util.List<String> topics = legacyClientConfig.getTopicsForService(serviceName);
        if (topics.isEmpty()) {
            log.error("Unknown legacy service: {}. No topics configured.", serviceName);
            return;
        }

        log.info("Legacy client {} (service={}) subscribing to {} topics: {}",
                clientId, serviceName, topics.size(), topics);

        // Register consumer for ALL topics (serviceName is used as the consumer group)
        int newRegistrations = 0;
        for (String topic : topics) {
            boolean isNew = remoteConsumers.registerConsumer(clientId, topic, serviceName);
            if (isNew) {
                newRegistrations++;
            }
            log.info("Registered legacy consumer {} for topic={}, group={}",
                    clientId, topic, serviceName);
        }

        // Record metrics only for new registrations
        for (int i = 0; i < newRegistrations; i++) {
            metrics.recordConsumerConnection();
        }

        log.info("Legacy client {} registered for {} topics (service={})",
                clientId, topics.size(), serviceName);

        // Send ACK
        sendSubscribeAck(clientId, message);
    }

    /**
     * Send SUBSCRIBE acknowledgment
     */
    private void sendSubscribeAck(String clientId, BrokerMessage message) {
        BrokerMessage ack = new BrokerMessage(
            BrokerMessage.MessageType.ACK,
            message.getMessageId(),
            new byte[0]
        );

        log.debug("Sending ACK to {}: type={}, messageId={}",
                 clientId, ack.getType(), ack.getMessageId());

        server.send(clientId, ack).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Failed to send ACK to {}", clientId, ex);
            } else {
                log.info("Sent ACK to {} for SUBSCRIBE", clientId);
            }
        });
    }

    /**
     * Handle COMMIT_OFFSET message
     */
    private void handleCommitOffset(String clientId, BrokerMessage message) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            String topic = json.get("topic").asText();
            String group = json.get("group").asText();
            long offset = json.get("offset").asLong();

            log.debug("Client {} committed offset: topic={}, group={}, offset={}",
                     clientId, topic, group, offset);

            // CRITICAL: Persist offset to property file - property file is ONLY source of truth
            String consumerId = group + ":" + topic;
            offsetTracker.updateOffset(consumerId, offset);

            log.debug("Persisted offset to property file: consumerId={}, offset={}", consumerId, offset);

            // Send ACK
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack);

        } catch (Exception e) {
            log.error("Error handling COMMIT_OFFSET from {}", clientId, e);
        }
    }

    /**
     * Handle RESET ACK from consumer during data refresh workflow
     */
    private void handleResetAck(String clientId, BrokerMessage message) {
        try {
            // MULTI-GROUP FIX: Parse both topic and group from payload
            // Format: [topicLen:4][topic:var][groupLen:4][group:var]
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(message.getPayload());

            // SECURITY FIX (BROKER-2): Validate payload size before reading
            if (buffer.remaining() < 8) {  // Need at least 4 bytes for topicLen + 4 for groupLen
                log.error("RESET_ACK payload too small from {}: {} bytes (expected >= 8)",
                         clientId, buffer.remaining());
                server.closeConnection(clientId);
                return;
            }

            int topicLen = buffer.getInt();

            // SECURITY FIX (BROKER-2): Validate topicLen to prevent OOM attack
            if (topicLen < 0 || topicLen > 65535) {  // Max 64KB topic name
                log.error("Invalid topicLen in RESET_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, topicLen);
                server.closeConnection(clientId);
                return;
            }

            // SECURITY FIX (BROKER-2): Ensure sufficient data available before allocation
            if (buffer.remaining() < topicLen + 4) {  // Need topicLen bytes + 4 for groupLen
                log.error("Not enough data in RESET_ACK from {}: remaining={}, needed={}",
                         clientId, buffer.remaining(), topicLen + 4);
                server.closeConnection(clientId);
                return;
            }

            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            int groupLen = buffer.getInt();

            // SECURITY FIX (BROKER-2): Validate groupLen to prevent OOM attack
            if (groupLen < 0 || groupLen > 65535) {  // Max 64KB group name
                log.error("Invalid groupLen in RESET_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, groupLen);
                server.closeConnection(clientId);
                return;
            }

            // SECURITY FIX (BROKER-2): Ensure sufficient data available before allocation
            if (buffer.remaining() < groupLen) {
                log.error("Not enough data for group in RESET_ACK from {}: remaining={}, needed={}",
                         clientId, buffer.remaining(), groupLen);
                server.closeConnection(clientId);
                return;
            }

            byte[] groupBytes = new byte[groupLen];
            buffer.get(groupBytes);
            String group = new String(groupBytes, StandardCharsets.UTF_8);

            log.info("Received RESET ACK from client: {} for topic: {}, group: {}", clientId, topic, group);

            // MULTI-GROUP FIX: Construct consumerGroupTopic directly from parsed group and topic
            String consumerGroupTopic = group + ":" + topic;

            log.info("Mapped consumer {} to group:topic identifier: {}", clientId, consumerGroupTopic);

            // Delegate to DataRefreshManager with BOTH identifiers:
            // - consumerGroupTopic for stable tracking
            // - clientId for triggering replay
            dataRefreshManager.handleResetAck(consumerGroupTopic, clientId, topic);

            // Send ACK back to consumer to acknowledge receipt
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send RESET ACK to {}", clientId, ex);
                } else {
                    log.debug("Sent ACK to {} for RESET", clientId);
                }
            });


        } catch (Exception e) {
            log.error("Error handling RESET from {}", clientId, e);
        }
    }

    /**
     * Handle READY ACK from consumer during data refresh workflow
     */
    private void handleReadyAck(String clientId, BrokerMessage message) {
        try {
            // MULTI-GROUP FIX: Parse both topic and group from payload
            // Format: [topicLen:4][topic:var][groupLen:4][group:var]
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(message.getPayload());

            // SECURITY FIX (BROKER-2): Validate payload size before reading
            if (buffer.remaining() < 8) {  // Need at least 4 bytes for topicLen + 4 for groupLen
                log.error("READY_ACK payload too small from {}: {} bytes (expected >= 8)",
                         clientId, buffer.remaining());
                server.closeConnection(clientId);
                return;
            }

            int topicLen = buffer.getInt();

            // SECURITY FIX (BROKER-2): Validate topicLen to prevent OOM attack
            if (topicLen < 0 || topicLen > 65535) {  // Max 64KB topic name
                log.error("Invalid topicLen in READY_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, topicLen);
                server.closeConnection(clientId);
                return;
            }

            // SECURITY FIX (BROKER-2): Ensure sufficient data available before allocation
            if (buffer.remaining() < topicLen + 4) {  // Need topicLen bytes + 4 for groupLen
                log.error("Not enough data in READY_ACK from {}: remaining={}, needed={}",
                         clientId, buffer.remaining(), topicLen + 4);
                server.closeConnection(clientId);
                return;
            }

            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            int groupLen = buffer.getInt();

            // SECURITY FIX (BROKER-2): Validate groupLen to prevent OOM attack
            if (groupLen < 0 || groupLen > 65535) {  // Max 64KB group name
                log.error("Invalid groupLen in READY_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, groupLen);
                server.closeConnection(clientId);
                return;
            }

            // SECURITY FIX (BROKER-2): Ensure sufficient data available before allocation
            if (buffer.remaining() < groupLen) {
                log.error("Not enough data for group in READY_ACK from {}: remaining={}, needed={}",
                         clientId, buffer.remaining(), groupLen);
                server.closeConnection(clientId);
                return;
            }

            byte[] groupBytes = new byte[groupLen];
            buffer.get(groupBytes);
            String group = new String(groupBytes, StandardCharsets.UTF_8);

            log.info("Received READY ACK from client: {} for topic: {}, group: {}", clientId, topic, group);

            // MULTI-GROUP FIX: Construct consumerGroupTopic directly from parsed group and topic
            String consumerGroupTopic = group + ":" + topic;

            log.info("Mapped consumer {} to group:topic identifier: {}", clientId, consumerGroupTopic);

            // Delegate to DataRefreshManager with stable identifier
            dataRefreshManager.handleReadyAck(consumerGroupTopic, topic);

            // Send ACK back to consumer to acknowledge receipt
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send READY ACK to {}", clientId, ex);
                } else {
                    log.debug("Sent ACK to {} for READY", clientId);
                }
            });

        } catch (Exception e) {
            log.error("Error handling READY from {}", clientId, e);
        }
    }

    /**
     * Handle BATCH_ACK message - consumer acknowledges receipt and processing of a batch
     * MULTI-GROUP FIX: Payload now contains both topic and group
     */
    private void handleBatchAck(String clientId, BrokerMessage message) {
        try {
            // MULTI-GROUP FIX: Parse both topic and group from payload
            // Format: [topicLen:4][topic:var][groupLen:4][group:var]
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(message.getPayload());

            // SECURITY FIX (BROKER-2): Validate payload size before reading
            if (buffer.remaining() < 8) {  // Need at least 4 bytes for topicLen + 4 for groupLen
                log.error("BATCH_ACK payload too small from {}: {} bytes (expected >= 8)",
                         clientId, buffer.remaining());
                server.closeConnection(clientId);
                return;
            }

            int topicLen = buffer.getInt();

            // SECURITY FIX (BROKER-2): Validate topicLen to prevent OOM attack
            if (topicLen < 0 || topicLen > 65535) {  // Max 64KB topic name
                log.error("Invalid topicLen in BATCH_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, topicLen);
                server.closeConnection(clientId);
                return;
            }

            // SECURITY FIX (BROKER-2): Ensure sufficient data available before allocation
            if (buffer.remaining() < topicLen + 4) {  // Need topicLen bytes + 4 for groupLen
                log.error("Not enough data in BATCH_ACK from {}: remaining={}, needed={}",
                         clientId, buffer.remaining(), topicLen + 4);
                server.closeConnection(clientId);
                return;
            }

            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            int groupLen = buffer.getInt();

            // SECURITY FIX (BROKER-2): Validate groupLen to prevent OOM attack
            if (groupLen < 0 || groupLen > 65535) {  // Max 64KB group name
                log.error("Invalid groupLen in BATCH_ACK from {}: {} (expected 0-65535). " +
                         "Possible corrupted payload or protocol mismatch. Closing connection.",
                         clientId, groupLen);
                server.closeConnection(clientId);
                return;
            }

            // SECURITY FIX (BROKER-2): Ensure sufficient data available before allocation
            if (buffer.remaining() < groupLen) {
                log.error("Not enough data for group in BATCH_ACK from {}: remaining={}, needed={}",
                         clientId, buffer.remaining(), groupLen);
                server.closeConnection(clientId);
                return;
            }

            byte[] groupBytes = new byte[groupLen];
            buffer.get(groupBytes);
            String group = new String(groupBytes, StandardCharsets.UTF_8);

            log.debug("Received BATCH_ACK from client: {}, topic: {}, group: {}", clientId, topic, group);

            // Offload ACK processing to dedicated executor to prevent Netty event loop blocking
            // This ensures ACKs are processed quickly even when delivery operations are heavy
            final String finalTopic = topic;
            final String finalGroup = group;
            ackExecutor.execute(() -> {
                try {
                    remoteConsumers.handleBatchAck(clientId, finalTopic, finalGroup);
                } catch (Exception e) {
                    log.error("Error processing BATCH_ACK from {}", clientId, e);
                }
            });

        } catch (Exception e) {
            log.error("Error handling BATCH_ACK from {}", clientId, e);
        }
    }

    /**
     * Handle client disconnect
     */
    private void handleDisconnect(String clientId) {
        log.info("Handling disconnect for client: {}", clientId);
        int unregisteredCount = remoteConsumers.unregisterConsumer(clientId);

        // Decrement metrics for each topic subscription that was removed
        for (int i = 0; i < unregisteredCount; i++) {
            metrics.recordConsumerDisconnection();
        }

        log.info("Disconnected client {} with {} topic subscriptions", clientId, unregisteredCount);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down broker...");

        // Shutdown ACK executor first to stop accepting new ACKs
        log.info("Shutting down ACK executor...");
        ackExecutor.shutdown();
        try {
            if (!ackExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("ACK executor did not terminate in time, forcing shutdown");
                ackExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for ACK executor shutdown", e);
            ackExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Stop adaptive delivery manager
        adaptiveDeliveryManager.stop();

        // Stop topology manager (will disconnect from parent)
        topologyManager.shutdown();

        // Stop consumer delivery
        consumerDelivery.shutdown();

        // Stop remote consumer delivery
        remoteConsumers.shutdown();

        // Shutdown server and storage
        server.shutdown();
        try {
            storage.close();
        } catch (Exception e) {
            log.error("Error closing storage during shutdown", e);
        }

        log.info("Broker shutdown complete");
    }
}
