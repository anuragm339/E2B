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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client-side consumer manager - Automatically handles connection, reconnection, and subscription
 * for all @Consumer annotated beans.
 *
 * Features:
 * - Auto-discovery of @Consumer annotated MessageHandler beans
 * - Auto-connect to broker on startup (ONE CONNECTION PER TOPIC - B1-7 fix)
 * - Auto-reconnect with exponential backoff (5s → 60s max)
 * - Auto-retry when topics don't exist (disconnect and retry after 5s)
 * - Connection health monitoring (30s interval)
 * - Graceful shutdown
 *
 * Architecture (B1-7 FIX):
 * - Creates one dedicated TCP connection per topic to prevent zero-copy batch interleaving
 * - Each connection carries only one topic stream, ensuring decoder safety
 * - For price-quote with 6 topics: 6 independent TCP connections to broker
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
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    // B1-7 FIX: One TCP connection per topic to prevent zero-copy batch interleaving
    private final Map<String, NettyTcpClient> clientsPerTopic = new ConcurrentHashMap<>();
    private final Map<String, NettyTcpClient.Connection> connectionsPerTopic = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean> connectedPerTopic = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> reconnectAttemptsPerTopic = new ConcurrentHashMap<>();

    private ScheduledExecutorService reconnectScheduler;
    private ScheduledExecutorService healthCheckScheduler;
    private ScheduledFuture<?> healthCheckTask;

    private final Map<String, ConsumerMetadata> consumers = new ConcurrentHashMap<>();

    // Map: topic -> MessageHandler (for routing messages to correct handler)
    private final Map<String, MessageHandler> topicToHandler = new ConcurrentHashMap<>();
    // Map: topic -> group (for SUBSCRIBE messages)
    private final Map<String, String> topicToGroup = new ConcurrentHashMap<>();

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

        // B1-7 FIX: Build topic -> handler and topic -> group mappings
        for (String topic : topics) {
            topicToHandler.put(topic, handler);
            topicToGroup.put(topic, group);
            log.info("Registered mapping: topic={} -> handler={}, group={}", topic, handler.getClass().getSimpleName(), group);
        }

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

    /**
     * B1-7 FIX: Create one dedicated TCP connection per topic
     * This prevents zero-copy batch interleaving on multi-topic consumers
     */
    private void connectToBroker() {
        if (!running.get()) {
            return;
        }

        log.info("=== B1-7 FIX: Creating one TCP connection per topic ===");
        log.info("Connecting to broker at {}:{}", brokerHost, brokerPort);

        // Get all unique topics across all consumers
        Set<String> allTopics = new java.util.HashSet<>();
        for (ConsumerMetadata metadata : consumers.values()) {
            allTopics.addAll(java.util.Arrays.asList(metadata.topics));
        }

        log.info("Total topics to connect: {}", allTopics.size());

        int successCount = 0;
        int failCount = 0;

        for (String topic : allTopics) {
            try {
                connectToTopic(topic);
                successCount++;
            } catch (Exception e) {
                log.error("✗ Failed to connect topic {}: {}", topic, e.getMessage());
                failCount++;
            }
        }

        if (successCount > 0) {
            log.info("✓ Connected {} topic(s) successfully, {} failed", successCount, failCount);
            reconnectAttempts.set(0);
            startHealthMonitoring();
        } else {
            log.error("✗ All topic connections failed, scheduling reconnect");
            scheduleReconnect();
        }
    }

    /**
     * B1-7 FIX: Create dedicated connection for a single topic
     */
    private void connectToTopic(String topic) throws Exception {
        int attempt = reconnectAttemptsPerTopic.computeIfAbsent(topic, k -> new AtomicInteger(0)).get() + 1;

        log.info("Connecting topic '{}' to broker (attempt {})...", topic, attempt);

        // Create new client for this topic
        NettyTcpClient topicClient = new NettyTcpClient();
        clientsPerTopic.put(topic, topicClient);

        // Connect
        CompletableFuture<NettyTcpClient.Connection> connectFuture =
            topicClient.connect(brokerHost, brokerPort);

        NettyTcpClient.Connection topicConnection = connectFuture.get(10, TimeUnit.SECONDS);
        connectionsPerTopic.put(topic, topicConnection);

        // Set up message handler for this topic's connection
        topicConnection.onMessage(message -> handleMessage(topic, message));

        // Set up disconnect handler for this topic
        if (topicConnection instanceof NettyTcpClient.TcpConnection) {
            ((NettyTcpClient.TcpConnection) topicConnection).onDisconnect(() -> {
                log.warn("⚠ Connection lost for topic '{}' - triggering reconnection", topic);
                connectedPerTopic.put(topic, new AtomicBoolean(false));
                scheduleTopicReconnect(topic);
            });
        }

        connectedPerTopic.put(topic, new AtomicBoolean(true));
        reconnectAttemptsPerTopic.get(topic).set(0);

        log.info("✓ Connected topic '{}' successfully", topic);

        // Subscribe this topic on its dedicated connection
        subscribeTopicOnConnection(topic, topicConnection);
    }

    /**
     * B1-7 FIX: Subscribe a topic on its dedicated connection
     */
    private void subscribeTopicOnConnection(String topic, NettyTcpClient.Connection connection) {
        String group = topicToGroup.get(topic);
        if (group == null) {
            log.error("No group mapping found for topic: {}", topic);
            return;
        }

        try {
            String payload = String.format("{\"topic\":\"%s\",\"group\":\"%s\"}", topic, group);

            BrokerMessage subscribeMsg = new BrokerMessage(
                BrokerMessage.MessageType.SUBSCRIBE,
                System.currentTimeMillis(),
                payload.getBytes(StandardCharsets.UTF_8)
            );

            connection.send(subscribeMsg).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to SUBSCRIBE topic '{}' on dedicated connection", topic, ex);
                    scheduleTopicReconnect(topic);
                } else {
                    log.info("✓ SUBSCRIBE sent for topic '{}', group '{}' on dedicated connection", topic, group);
                }
            });

        } catch (Exception e) {
            log.error("Error subscribing topic '{}' on dedicated connection", topic, e);
        }
    }

    /**
     * B1-7 FIX: Schedule reconnection for a specific topic
     */
    private void scheduleTopicReconnect(String topic) {
        if (!running.get()) {
            return;
        }

        AtomicInteger attempts = reconnectAttemptsPerTopic.computeIfAbsent(topic, k -> new AtomicInteger(0));
        int attemptCount = attempts.incrementAndGet();

        long delayMs = Math.min(
            INITIAL_RECONNECT_DELAY_MS * (1L << Math.min(attemptCount - 1, 4)),
            MAX_RECONNECT_DELAY_MS
        );

        log.info("⟳ Scheduling reconnect for topic '{}' in {}ms (attempt {})", topic, delayMs, attemptCount);

        reconnectScheduler.schedule(() -> {
            cleanupTopicConnection(topic);
            try {
                connectToTopic(topic);
            } catch (Exception e) {
                log.error("Reconnect failed for topic '{}': {}", topic, e.getMessage());
                scheduleTopicReconnect(topic);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * B1-7 FIX: Schedule full reconnect for all topics (used on initial connection failure)
     */
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

        log.info("⟳ Scheduling full reconnect in {}ms (attempt {})", delayMs, attempts);

        reconnectScheduler.schedule(() -> {
            reconnectScheduled.set(false);
            cleanupAllConnections();
            connectToBroker();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * B1-7 FIX: Monitor health of all per-topic connections
     */
    private void startHealthMonitoring() {
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
        }

        healthCheckTask = healthCheckScheduler.scheduleAtFixedRate(() -> {
            try {
                int aliveCount = 0;
                int deadCount = 0;

                for (Map.Entry<String, NettyTcpClient.Connection> entry : connectionsPerTopic.entrySet()) {
                    String topic = entry.getKey();
                    NettyTcpClient.Connection conn = entry.getValue();

                    AtomicBoolean isConnected = connectedPerTopic.get(topic);
                    if (isConnected == null || !isConnected.get()) {
                        log.debug("Health check: topic '{}' already disconnected, skip check", topic);
                        continue;
                    }

                    if (conn == null || !conn.isAlive()) {
                        log.warn("Health check failed for topic '{}': connection is " +
                                (conn == null ? "null" : "not alive"), topic);
                        isConnected.set(false);
                        scheduleTopicReconnect(topic);
                        deadCount++;
                    } else {
                        aliveCount++;
                    }
                }

                if (aliveCount + deadCount > 0) {
                    log.debug("Health check: {} alive, {} dead topic connections", aliveCount, deadCount);
                }

            } catch (Exception e) {
                log.error("Health check error", e);
            }
        }, HEALTH_CHECK_INTERVAL_MS, HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * B1-7 FIX: Handle message for a specific topic's connection
     * Each topic has its own connection, so we know which topic this message is for
     */
    private void handleMessage(String topic, BrokerMessage message) {
        try {
            log.debug("Received message for topic '{}': type={}", topic, message.getType());

            switch (message.getType()) {
                case DATA:
                case BATCH_HEADER:
                    handleDataMessage(topic, message);
                    break;

                case ACK:
                    log.debug("Received ACK for topic '{}'", topic);
                    break;

                case RESET:
                case READY:
                    handleControlMessage(topic, message);
                    break;

                case DISCONNECT:
                    log.warn("Received DISCONNECT from broker for topic '{}'", topic);
                    connectedPerTopic.get(topic).set(false);
                    scheduleTopicReconnect(topic);
                    break;

                default:
                    log.debug("Received message type {} for topic '{}'", message.getType(), topic);
            }

        } catch (Exception e) {
            log.error("Error handling message for topic '{}'", topic, e);
        }
    }

    /**
     * B1-7 FIX: Handle DATA message for a specific topic
     * Route to the correct handler based on topic mapping
     */
    private void handleDataMessage(String topic, BrokerMessage message) {
        try {
            byte[] payload = message.getPayload();

            List<ConsumerRecord> records;
            if (payload.length > 0 && payload[0] == '[') {
                records = parseJsonBatch(payload);
            } else {
                records = parseRawBatch(payload);
            }

            // Route to the handler registered for this topic
            MessageHandler handler = topicToHandler.get(topic);
            if (handler != null) {
                try {
                    handler.handleBatch(records);
                    log.debug("Delivered {} records to handler for topic '{}'", records.size(), topic);
                } catch (Exception e) {
                    log.error("Error in consumer handler for topic '{}'", topic, e);
                }
            } else {
                log.warn("No handler found for topic '{}', dropping {} records", topic, records.size());
            }

        } catch (Exception e) {
            log.error("Error handling DATA message for topic '{}'", topic, e);
        }
    }


    /**
     * B1-7 FIX: Handle control message for a specific topic
     */
    private void handleControlMessage(String topic, BrokerMessage message) {
        try {
            if (message.getType() == BrokerMessage.MessageType.RESET) {
                handleResetMessage(topic, message);
            } else if (message.getType() == BrokerMessage.MessageType.READY) {
                handleReadyMessage(topic, message);
            }
        } catch (Exception e) {
            log.error("Error handling control message for topic '{}'", topic, e);
        }
    }

    /**
     * B1-7 FIX: Handle RESET message for a specific topic
     * Uses the topic's dedicated connection for ACK
     */
    private void handleResetMessage(String topic, BrokerMessage message) {
        try {
            // Parse RESET message to get topic (should match the connection's topic)
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String messageTopic = extractTopicFromPayload(payload);

            log.info("Received RESET for topic: '{}', preparing to receive refreshed data", messageTopic);

            // Call onReset on the handler for this topic
            MessageHandler handler = topicToHandler.get(messageTopic);
            if (handler != null) {
                try {
                    handler.onReset(messageTopic);
                    String group = topicToGroup.get(messageTopic);
                    log.info("Called onReset for topic '{}', group '{}'", messageTopic, group);
                } catch (Exception e) {
                    log.error("Error in consumer onReset handler for topic '{}'", messageTopic, e);
                }
            }

            // Send RESET ACK back to broker on this topic's dedicated connection
            BrokerMessage resetAck = new BrokerMessage(
                BrokerMessage.MessageType.RESET_ACK,
                System.currentTimeMillis(),
                messageTopic.getBytes(StandardCharsets.UTF_8)
            );

            NettyTcpClient.Connection topicConnection = connectionsPerTopic.get(topic);
            if (topicConnection != null) {
                topicConnection.send(resetAck).whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send RESET ACK for topic '{}'", topic, ex);
                    } else {
                        log.info("✓ RESET ACK sent for topic '{}' on dedicated connection", topic);
                    }
                });
            } else {
                log.error("No connection found for topic '{}' to send RESET ACK", topic);
            }

        } catch (Exception e) {
            log.error("Error handling RESET message for topic '{}'", topic, e);
        }
    }

    /**
     * B1-7 FIX: Handle READY message for a specific topic
     * Uses the topic's dedicated connection for ACK
     */
    private void handleReadyMessage(String topic, BrokerMessage message) {
        try {
            // Parse READY message to get topic (should match the connection's topic)
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String messageTopic = extractTopicFromPayload(payload);

            log.info("Received READY for topic '{}', refresh complete", messageTopic);

            // Call onReady on the handler for this topic
            MessageHandler handler = topicToHandler.get(messageTopic);
            if (handler != null) {
                try {
                    handler.onReady(messageTopic);
                    String group = topicToGroup.get(messageTopic);
                    log.info("Called onReady for topic '{}', group '{}'", messageTopic, group);
                } catch (Exception e) {
                    log.error("Error in consumer onReady handler for topic '{}'", messageTopic, e);
                }
            }

            // Send READY ACK back to broker on this topic's dedicated connection
            BrokerMessage readyAck = new BrokerMessage(
                BrokerMessage.MessageType.READY_ACK,
                System.currentTimeMillis(),
                messageTopic.getBytes(StandardCharsets.UTF_8)
            );

            NettyTcpClient.Connection topicConnection = connectionsPerTopic.get(topic);
            if (topicConnection != null) {
                topicConnection.send(readyAck).whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send READY ACK for topic '{}'", topic, ex);
                    } else {
                        log.info("✓ READY ACK sent for topic '{}' on dedicated connection", topic);
                    }
                });
            } else {
                log.error("No connection found for topic '{}' to send READY ACK", topic);
            }

        } catch (Exception e) {
            log.error("Error handling READY message for topic '{}'", topic, e);
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

    /**
     * B1-7 FIX: Cleanup a specific topic connection
     */
    private void cleanupTopicConnection(String topic) {
        NettyTcpClient.Connection conn = connectionsPerTopic.remove(topic);
        if (conn != null) {
            try {
                conn.disconnect();
                log.debug("Disconnected topic '{}' connection", topic);
            } catch (Exception e) {
                log.debug("Error disconnecting topic '{}' connection", topic, e);
            }
        }

        NettyTcpClient client = clientsPerTopic.remove(topic);
        if (client != null) {
            try {
                client.shutdown();
                log.debug("Shutdown client for topic '{}'", topic);
            } catch (Exception e) {
                log.debug("Error shutting down client for topic '{}'", topic, e);
            }
        }
    }

    /**
     * B1-7 FIX: Cleanup all topic connections
     */
    private void cleanupAllConnections() {
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
            healthCheckTask = null;
        }

        log.info("Cleaning up all {} topic connections...", connectionsPerTopic.size());

        for (String topic : new java.util.HashSet<>(connectionsPerTopic.keySet())) {
            cleanupTopicConnection(topic);
        }

        connectionsPerTopic.clear();
        clientsPerTopic.clear();
        connectedPerTopic.clear();

        log.info("All topic connections cleaned up");
    }

    /**
     * B1-7 FIX: Shutdown all per-topic connections gracefully
     */
    @PreDestroy
    public void shutdown() {
        log.info("=== ClientConsumerManager Shutting Down (B1-7 per-topic connections) ===");

        running.set(false);

        // Mark all topics as disconnected
        for (AtomicBoolean connected : connectedPerTopic.values()) {
            connected.set(false);
        }

        cleanupAllConnections();

        if (reconnectScheduler != null) {
            reconnectScheduler.shutdownNow();
        }

        if (healthCheckScheduler != null) {
            healthCheckScheduler.shutdownNow();
        }

        log.info("ClientConsumerManager shutdown complete - closed {} topic connections",
                connectionsPerTopic.size());
    }

    /**
     * B1-7 FIX: Check if any topic connection is alive
     */
    public boolean isConnected() {
        return connectedPerTopic.values().stream()
                .anyMatch(AtomicBoolean::get);
    }

    public int getReconnectAttempts() {
        return reconnectAttempts.get();
    }

    /**
     * B1-7 FIX: Get total number of active topic connections
     */
    public int getConnectedTopicCount() {
        return (int) connectedPerTopic.values().stream()
                .filter(AtomicBoolean::get)
                .count();
    }

    /**
     * B1-7 FIX: Get total number of topic connections (connected + disconnected)
     */
    public int getTotalTopicCount() {
        return connectionsPerTopic.size();
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
