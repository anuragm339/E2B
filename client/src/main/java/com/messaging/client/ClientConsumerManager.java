package com.messaging.client;

import com.messaging.common.annotation.Consumer;
import com.messaging.common.api.MessageHandler;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.network.tcp.NettyTcpClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client-side consumer manager - Automatically handles connection, reconnection, and subscription
 * for all @Consumer annotated beans.
 *
 * Features:
 * - Auto-discovery of @Consumer annotated MessageHandler beans
 * - Auto-connect to broker on startup
 * - Auto-reconnect with exponential backoff (5s → 60s max)
 * - Auto-retry when topics don't exist (disconnect and retry after 5s)
 * - Connection health monitoring (30s interval)
 * - Graceful shutdown
 *
 * Consumer apps only need to:
 * 1. Add @Consumer annotation to MessageHandler implementation
 * 2. Configure messaging.broker.host and messaging.broker.port
 */
@Singleton
@Requires(property = "messaging.broker.host")
@Requires(beans = MessageHandler.class)
public class ClientConsumerManager implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(ClientConsumerManager.class);

    private static final int INITIAL_RECONNECT_DELAY_MS = 5000; // 5 seconds
    private static final int MAX_RECONNECT_DELAY_MS = 60000; // 1 minute
    private static final int HEALTH_CHECK_INTERVAL_MS = 10000; // 10 seconds

    @Value("${messaging.broker.host}")
    private String brokerHost;

    @Value("${messaging.broker.port}")
    private int brokerPort;

    @Value("${consumer.type:unknown}")
    private String consumerType;

    @Inject
    private ApplicationContext applicationContext;

    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    private NettyTcpClient client;
    private NettyTcpClient.Connection connection;
    private ScheduledExecutorService reconnectScheduler;
    private ScheduledExecutorService healthCheckScheduler;
    private ScheduledFuture<?> healthCheckTask;

    private final Map<String, ConsumerMetadata> consumers = new ConcurrentHashMap<>();

    public ClientConsumerManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("=== ClientConsumerManager Starting ===");

        discoverConsumers();

        if (consumers.isEmpty()) {
            log.warn("No @Consumer annotated beans found. Consumer framework will not start.");
            return;
        }

        log.info("Discovered {} consumer(s)", consumers.size());
        consumers.values().forEach(m ->
            log.info("  → Consumer: topics={}, group={}", String.join(",", m.topics), m.group)
        );

        running.set(true);
        reconnectScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "consumer-reconnect")
        );
        healthCheckScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "consumer-health")
        );

        connectToBroker();
    }

    private void discoverConsumers() {
        Collection<MessageHandler> handlers = applicationContext.getBeansOfType(MessageHandler.class);
        log.info("Scanning {} MessageHandler bean(s) for @Consumer annotation...", handlers.size());

        for (MessageHandler handler : handlers) {
            Consumer annotation = handler.getClass().getAnnotation(Consumer.class);
            if (annotation != null) {
                registerConsumer(handler, annotation);
            }
        }
    }

    private void registerConsumer(MessageHandler handler, Consumer annotation) {
        String topicConfig = resolveProperty(annotation.topic());
        String[] topics = topicConfig.split(",");
        for (int i = 0; i < topics.length; i++) {
            topics[i] = topics[i].trim();
        }

        String group = resolveProperty(annotation.group());

        ConsumerMetadata metadata = new ConsumerMetadata(handler, topics, group, annotation);
        String consumerId = String.join(",", topics) + ":" + group;
        consumers.put(consumerId, metadata);

        log.info("Registered consumer: topics={}, group={}", String.join(",", topics), group);
    }

    private String resolveProperty(String value) {
        if (value.startsWith("${") && value.endsWith("}")) {
            String propertyPath = value.substring(2, value.length() - 1);
            String[] parts = propertyPath.split(":");
            String propertyName = parts[0];
            String defaultValue = parts.length > 1 ? parts[1] : "";

            String envValue = System.getenv(propertyName);
            if (envValue != null && !envValue.isEmpty()) {
                return envValue;
            }

            try {
                return applicationContext.getProperty(propertyName, String.class).orElse(defaultValue);
            } catch (Exception e) {
                return defaultValue;
            }
        }
        return value;
    }

    private void connectToBroker() {
        if (!running.get()) {
            return;
        }

        try {
            log.info("Connecting to broker at {}:{}... (attempt {})",
                    brokerHost, brokerPort, reconnectAttempts.get() + 1);

            if (client == null) {
                client = new NettyTcpClient();
            }

            CompletableFuture<NettyTcpClient.Connection> connectFuture =
                client.connect(brokerHost, brokerPort);

            connection = connectFuture.get(10, TimeUnit.SECONDS);
            connection.onMessage(this::handleMessage);

            // Set up disconnect handler for immediate reconnection
            if (connection instanceof NettyTcpClient.TcpConnection) {
                ((NettyTcpClient.TcpConnection) connection).onDisconnect(() -> {
                    log.warn("⚠ Connection lost - triggering reconnection");
                    connected.set(false);
                    scheduleReconnect();
                });
            }

            connected.set(true);
            reconnectAttempts.set(0);

            log.info("✓ Connected to broker successfully");

            subscribeAll();
            startHealthMonitoring();

        } catch (Exception e) {
            log.error("✗ Failed to connect to broker: {}", e.getMessage());
            connected.set(false);
            scheduleReconnect();
        }
    }

    private void subscribeAll() {
        if (!connected.get()) {
            return;
        }

        log.info("Subscribing all consumers...");

        for (ConsumerMetadata metadata : consumers.values()) {
            for (String topic : metadata.topics) {
                subscribe(topic, metadata.group);
            }
        }
    }

    private void subscribe(String topic, String group) {
        try {
            String payload = String.format("{\"topic\":\"%s\",\"group\":\"%s\"}", topic, group);

            BrokerMessage subscribeMsg = new BrokerMessage(
                BrokerMessage.MessageType.SUBSCRIBE,
                System.currentTimeMillis(),
                payload.getBytes(StandardCharsets.UTF_8)
            );

            connection.send(subscribeMsg).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to SUBSCRIBE to topic: {}", topic, ex);
                    scheduleReconnect();
                } else {
                    log.info("✓ SUBSCRIBE sent for topic: {}, group: {}", topic, group);
                }
            });

        } catch (Exception e) {
            log.error("Error subscribing to topic: {}", topic, e);
        }
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            return;
        }

        // Prevent duplicate reconnect schedules
        if (!reconnectScheduled.compareAndSet(false, true)) {
            log.debug("Reconnect already scheduled, skipping duplicate");
            return;
        }

        int attempts = reconnectAttempts.incrementAndGet();
        long delayMs = Math.min(
            INITIAL_RECONNECT_DELAY_MS * (1L << Math.min(attempts - 1, 4)),
            MAX_RECONNECT_DELAY_MS
        );

        log.info("⟳ Scheduling reconnect in {}ms (attempt {})", delayMs, attempts);

        reconnectScheduler.schedule(() -> {
            reconnectScheduled.set(false);
            cleanupConnection();
            connectToBroker();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void startHealthMonitoring() {
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
        }

        healthCheckTask = healthCheckScheduler.scheduleAtFixedRate(() -> {
            try {
                if (!connected.get()) {
                    log.debug("Health check: already disconnected, skip check");
                    return;
                }

                if (connection == null || !connection.isAlive()) {
                    log.warn("Health check failed: connection is " + (connection == null ? "null" : "not alive"));
                    connected.set(false);
                    scheduleReconnect();
                    return;
                }

                log.debug("Health check: connection is alive");

            } catch (Exception e) {
                log.error("Health check error", e);
                connected.set(false);
                scheduleReconnect();
            }
        }, HEALTH_CHECK_INTERVAL_MS, HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void handleMessage(BrokerMessage message) {
        try {
            log.debug("Received message: type={}", message.getType());

            switch (message.getType()) {
                case DATA:
                    handleDataMessage(message);
                    break;

                case ACK:
                    log.debug("Received ACK");
                    break;

                case RESET:
                case READY:
                    handleControlMessage(message);
                    break;

                case DISCONNECT:
                    log.warn("Received DISCONNECT from broker");
                    connected.set(false);
                    scheduleReconnect();
                    break;

                default:
                    log.debug("Received message type: {}", message.getType());
            }

        } catch (Exception e) {
            log.error("Error handling message", e);
        }
    }

    private void handleDataMessage(BrokerMessage message) {
        try {
            byte[] payload = message.getPayload();

            List<ConsumerRecord> records;
            if (payload.length > 0 && payload[0] == '[') {
                records = parseJsonBatch(payload);
            } else {
                records = parseRawBatch(payload);
            }

            for (ConsumerMetadata metadata : consumers.values()) {
                try {
                    metadata.handler.handleBatch(records);
                } catch (Exception e) {
                    log.error("Error in consumer handler", e);
                }
            }

        } catch (Exception e) {
            log.error("Error handling DATA message", e);
        }
    }


    private void handleControlMessage(BrokerMessage message) {
        try {
            if (message.getType() == BrokerMessage.MessageType.RESET) {
                handleResetMessage(message);
            } else if (message.getType() == BrokerMessage.MessageType.READY) {
                handleReadyMessage(message);
            }
        } catch (Exception e) {
            log.error("Error handling control message", e);
        }
    }

    private void handleResetMessage(BrokerMessage message) {
        try {
            // Parse RESET message to get topic
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String topic = extractTopicFromPayload(payload);

            log.info("Received RESET for topic: {}, preparing to receive refreshed data", topic);

            // Call onReset only on handlers subscribed to this topic
            for (ConsumerMetadata metadata : consumers.values()) {
                // Check if this handler is subscribed to the topic
                boolean isSubscribed = false;
                for (String subscribedTopic : metadata.topics) {
                    if (subscribedTopic.equals(topic)) {
                        isSubscribed = true;
                        break;
                    }
                }

                if (isSubscribed) {
                    try {
                        metadata.handler.onReset(topic);
                        log.info("Called onReset for topic: {} on handler for group: {}", topic, metadata.group);
                    } catch (Exception e) {
                        log.error("Error in consumer onReset handler for topic: {}, group: {}", topic, metadata.group, e);
                    }
                }
            }

            // Send RESET ACK back to broker (topic as plain text, not JSON)
            BrokerMessage resetAck = new BrokerMessage(
                BrokerMessage.MessageType.RESET_ACK,
                System.currentTimeMillis(),
                topic.getBytes(StandardCharsets.UTF_8)
            );

            connection.send(resetAck).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send RESET ACK for topic: {}", topic, ex);
                } else {
                    log.info("✓ RESET ACK sent for topic: {}", topic);
                }
            });

        } catch (Exception e) {
            log.error("Error handling RESET message", e);
        }
    }

    private void handleReadyMessage(BrokerMessage message) {
        try {
            // Parse READY message to get topic
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String topic = extractTopicFromPayload(payload);

            log.info("Received READY for topic: {}, refresh complete", topic);

            // Call onReady only on handlers subscribed to this topic
            for (ConsumerMetadata metadata : consumers.values()) {
                // Check if this handler is subscribed to the topic
                boolean isSubscribed = false;
                for (String subscribedTopic : metadata.topics) {
                    if (subscribedTopic.equals(topic)) {
                        isSubscribed = true;
                        break;
                    }
                }

                if (isSubscribed) {
                    try {
                        metadata.handler.onReady(topic);
                        log.info("Called onReady for topic: {} on handler for group: {}", topic, metadata.group);
                    } catch (Exception e) {
                        log.error("Error in consumer onReady handler for topic: {}, group: {}", topic, metadata.group, e);
                    }
                }
            }

            // Send READY ACK back to broker (topic as plain text, not JSON)
            BrokerMessage readyAck = new BrokerMessage(
                BrokerMessage.MessageType.READY_ACK,
                System.currentTimeMillis(),
                topic.getBytes(StandardCharsets.UTF_8)
            );

            connection.send(readyAck).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send READY ACK for topic: {}", topic, ex);
                } else {
                    log.info("✓ READY ACK sent for topic: {}", topic);
                }
            });

        } catch (Exception e) {
            log.error("Error handling READY message", e);
        }
    }

    private String extractTopicFromPayload(String payload) {
        // Payload might be JSON or plain topic name
        try {
            // Try JSON format first: {"topic":"topic-name",...}
            if (payload.contains("\"topic\":\"")) {
                int start = payload.indexOf("\"topic\":\"") + 9;
                int end = payload.indexOf("\"", start);
                return payload.substring(start, end);
            }
            // Otherwise, payload is just the topic name
            return payload.trim();
        } catch (Exception e) {
            log.error("Failed to extract topic from payload: {}", payload, e);
            return payload; // Return as-is if parsing fails
        }
    }

    private List<ConsumerRecord> parseJsonBatch(byte[] payload) throws Exception {
        String json = new String(payload, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, new TypeReference<List<ConsumerRecord>>() {});
    }

    private List<ConsumerRecord> parseRawBatch(byte[] payload) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(payload);
        List<ConsumerRecord> records = new java.util.ArrayList<>();

        while (buffer.hasRemaining() && buffer.remaining() >= 8) {
            try {
                long offset = buffer.getLong();
                if (buffer.remaining() < 4) break;

                int keyLen = buffer.getInt();
                if (buffer.remaining() < keyLen) break;

                byte[] keyBytes = new byte[keyLen];
                buffer.get(keyBytes);
                String msgKey = new String(keyBytes, StandardCharsets.UTF_8);

                if (buffer.remaining() < 1) break;
                byte eventTypeCode = buffer.get();

                if (buffer.remaining() < 4) break;
                int dataLen = buffer.getInt();
                if (buffer.remaining() < dataLen) break;

                String data = null;
                if (dataLen > 0) {
                    byte[] dataBytes = new byte[dataLen];
                    buffer.get(dataBytes);
                    data = new String(dataBytes, StandardCharsets.UTF_8);
                }

                if (buffer.remaining() < 12) break;
                long timestampMillis = buffer.getLong();
                int crc32 = buffer.getInt();

                ConsumerRecord record = new ConsumerRecord(
                    msgKey,
                    com.messaging.common.model.EventType.fromCode((char) eventTypeCode),
                    data,
                    java.time.Instant.ofEpochMilli(timestampMillis)
                );

                records.add(record);
            } catch (Exception e) {
                log.error("Error parsing record", e);
                break;
            }
        }

        return records;
    }

    private void cleanupConnection() {
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
            healthCheckTask = null;
        }

        if (connection != null) {
            try {
                connection.disconnect();
            } catch (Exception e) {
                log.debug("Error disconnecting", e);
            }
            connection = null;
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("=== ClientConsumerManager Shutting Down ===");

        running.set(false);
        connected.set(false);

        cleanupConnection();

        if (reconnectScheduler != null) {
            reconnectScheduler.shutdownNow();
        }

        if (healthCheckScheduler != null) {
            healthCheckScheduler.shutdownNow();
        }

        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                log.error("Error shutting down client", e);
            }
        }

        log.info("ClientConsumerManager shutdown complete");
    }

    public boolean isConnected() {
        return connected.get();
    }

    public int getReconnectAttempts() {
        return reconnectAttempts.get();
    }

    private static class ConsumerMetadata {
        final MessageHandler handler;
        final String[] topics;
        final String group;
        final Consumer annotation;

        ConsumerMetadata(MessageHandler handler, String[] topics, String group, Consumer annotation) {
            this.handler = handler;
            this.topics = topics;
            this.group = group;
            this.annotation = annotation;
        }
    }
}
