package com.messaging.broker.core;

import com.messaging.broker.consumer.ConsumerDeliveryManager;
import com.messaging.broker.consumer.RemoteConsumerRegistry;
import com.messaging.broker.metrics.BrokerMetrics;
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
    private final TopologyManager topologyManager;
    private final BrokerMetrics metrics;
    private final int serverPort;
    private final ObjectMapper objectMapper;

    @Inject
    public BrokerService(
            StorageEngine storage,
            NetworkServer server,
            ConsumerDeliveryManager consumerDelivery,
            RemoteConsumerRegistry remoteConsumers,
            TopologyManager topologyManager,
            BrokerMetrics metrics,
            @Value("${broker.network.port:9092}") int serverPort) {

        this.storage = storage;
        this.server = server;
        this.consumerDelivery = consumerDelivery;
        this.remoteConsumers = remoteConsumers;
        this.topologyManager = topologyManager;
        this.metrics = metrics;
        this.serverPort = serverPort;
        this.objectMapper = new ObjectMapper();

        log.info("BrokerService initialized");
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("Application started, initializing broker...");

        // Recover storage
        storage.recover();
        log.info("Storage recovered");

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
            if (topic == null || topic.isEmpty()) {
                topic = "price-topic"; // Default fallback
                log.warn("Message received without topic, using default: price-topic");
            }

            Timer.Sample storageSample = metrics.startStorageWriteTimer();
            long offset = storage.append(topic, 0, record);
            metrics.stopStorageWriteTimer(storageSample);

            metrics.recordMessageStored();

            log.debug("Stored message from parent: topic={}, offset={}, key={}",
                    topic, offset, record.getMsgKey());

            // PUSH MODEL: Immediately notify remote consumers about new message
            // This triggers instant delivery instead of waiting for scheduler poll
            remoteConsumers.notifyNewMessage(topic, offset);

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

            case RESET:
                handleReset(clientId, message);
                break;

            case READY:
                handleReady(clientId, message);
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

            log.info("Stored message: topic={}, offset={}, key={}, type={}",
                     topic, offset, msgKey, eventType);

            // PUSH MODEL: Immediately notify consumers
            remoteConsumers.notifyNewMessage(topic, offset);

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
    private void handleSubscribe(String clientId, BrokerMessage message) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            String topic = json.get("topic").asText();
            String group = json.get("group").asText();

            log.info("Client {} subscribed: topic={}, group={}", clientId, topic, group);

            // Register consumer for message delivery
            remoteConsumers.registerConsumer(clientId, topic, group);
            metrics.recordConsumerConnection();

            log.info("Registered remote consumer {} for topic={}, group={}", clientId, topic, group);

            // Send ACK
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

        } catch (Exception e) {
            log.error("Error handling SUBSCRIBE from {}", clientId, e);
        }
    }

    /**
     * Handle COMMIT_OFFSET message
     */
    private void handleCommitOffset(String clientId, BrokerMessage message) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            JsonNode json = objectMapper.readTree(payload);

            String topic = json.get("topic").asText();
            long offset = json.get("offset").asLong();

            log.info("Client {} committed offset: topic={}, offset={}", clientId, topic, offset);

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
     * Handle RESET message - trigger data refresh workflow
     */
    private void handleReset(String clientId, BrokerMessage message) {
        try {
            log.info("Received RESET request from client: {}", clientId);

            // TODO: Implement data refresh workflow
            // 1. Send RESET to all consumers
            // 2. Wait for ACKs
            // 3. Replay all messages from offset 0
            // 4. Send READY when complete

            // For now, just ACK
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack);
            log.info("Sent ACK for RESET to client: {}", clientId);

        } catch (Exception e) {
            log.error("Error handling RESET from {}", clientId, e);
        }
    }

    /**
     * Handle READY message - data refresh complete acknowledgment
     */
    private void handleReady(String clientId, BrokerMessage message) {
        try {
            log.info("Received READY acknowledgment from client: {}", clientId);

            // Send ACK
            BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.ACK,
                message.getMessageId(),
                new byte[0]
            );

            server.send(clientId, ack);
            log.info("Sent ACK for READY to client: {}", clientId);

        } catch (Exception e) {
            log.error("Error handling READY from {}", clientId, e);
        }
    }

    /**
     * Handle client disconnect
     */
    private void handleDisconnect(String clientId) {
        log.info("Handling disconnect for client: {}", clientId);
        remoteConsumers.unregisterConsumer(clientId);
        metrics.recordConsumerDisconnection();
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down broker...");

        // Stop topology manager (will disconnect from parent)
        topologyManager.shutdown();

        // Stop consumer delivery
        consumerDelivery.shutdown();

        // Stop remote consumer delivery
        remoteConsumers.shutdown();

        // Shutdown server and storage
        server.shutdown();
        storage.close();

        log.info("Broker shutdown complete");
    }
}
