package com.messaging.client;

import com.messaging.common.annotation.Consumer;
import com.messaging.common.api.MessageHandler;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.network.tcp.NettyTcpClient;
import com.messaging.network.metrics.ConsumerDecodeMetrics;
import com.messaging.network.metrics.DecodeErrorRecorder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.MeterRegistry;
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

    // N2 FIX: Shared EventLoopGroup for all connections to prevent thread explosion
    private io.netty.channel.EventLoopGroup sharedEventLoopGroup;
    private DecodeErrorRecorder decodeErrorRecorder = DecodeErrorRecorder.noop();

    // N1 + N2 FIX: One TCP connection per topic:group to prevent zero-copy batch interleaving
    // AND avoid thread explosion by sharing EventLoopGroup
    private final Map<String, NettyTcpClient> clientsPerTopicGroup = new ConcurrentHashMap<>();
    private final Map<String, NettyTcpClient.Connection> connectionsPerTopicGroup = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean> connectedPerTopicGroup = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> reconnectAttemptsPerTopicGroup = new ConcurrentHashMap<>();
    private final Set<String> reconnectScheduledPerTopicGroup = ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService reconnectScheduler;
    private ScheduledExecutorService healthCheckScheduler;
    private ScheduledFuture<?> healthCheckTask;

    private final Map<String, ConsumerMetadata> consumers = new ConcurrentHashMap<>();

    // Map: topic -> List<MessageHandler> (for routing messages to all handlers for that topic)
    // #13: keyed by "topic:group" (NOT topic). Each @Consumer handler belongs to exactly one
    // group, so routing must be per topic:group — otherwise a delivery on group A's connection
    // would also be dispatched to group B's handler for the same topic.
    private final Map<String, List<MessageHandler>> topicGroupToHandlers = new ConcurrentHashMap<>();
    // Map: topic -> Set<String> groups (to know all topic:group combinations we need to connect)
    private final Map<String, Set<String>> topicToGroups = new ConcurrentHashMap<>();

    public ClientConsumerManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("=== ClientConsumerManager Starting ===");

        // Initialize decode error metrics if Micrometer is available
        try {
            this.decodeErrorRecorder = applicationContext.findBean(MeterRegistry.class)
                    .<DecodeErrorRecorder>map(ConsumerDecodeMetrics::new)
                    .orElse(DecodeErrorRecorder.noop());
        } catch (Exception e) {
            log.debug("Micrometer MeterRegistry not available for decode metrics; using no-op recorder");
            this.decodeErrorRecorder = DecodeErrorRecorder.noop();
        }

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

        // N2 FIX: Create one shared EventLoopGroup for all connections
        // This prevents thread explosion when creating many per-topic:group connections
        sharedEventLoopGroup = new io.netty.channel.nio.NioEventLoopGroup();
        log.info("N2 FIX: Created shared EventLoopGroup for all connections");

        reconnectScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread thread = new Thread(r, "consumer-reconnect");
                thread.setDaemon(true);
                return thread;
            }
        );
        healthCheckScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread thread = new Thread(r, "consumer-health");
                thread.setDaemon(true);
                return thread;
            }
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

        // FIX #1: Build topic -> handlers and topic -> groups mappings (supports multiple groups per topic)
        for (String topic : topics) {
            // Add handler to list (may have multiple handlers for same topic)
            topicGroupToHandlers.computeIfAbsent(topic + ":" + group, k -> new java.util.ArrayList<>()).add(handler);

            // Add group to set (may have multiple groups for same topic)
            topicToGroups.computeIfAbsent(topic, k -> new java.util.HashSet<>()).add(group);

            log.info("Registered mapping: topic='{}' -> handler={}, group={}",
                    topic, handler.getClass().getSimpleName(), group);
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
     * N1 + N2 FIX: Create one dedicated TCP connection per topic:group
     * This prevents zero-copy batch interleaving even when multiple groups share same topic
     */
    private void connectToBroker() {
        if (!running.get()) {
            return;
        }

        log.info("=== N1 + N2 FIX: Creating one TCP connection per topic:group ===");
        log.info("Connecting to broker at {}:{}", brokerHost, brokerPort);

        // Get all unique topic:group combinations
        Set<String> allTopicGroups = new java.util.HashSet<>();
        for (String topic : topicToGroups.keySet()) {
            Set<String> groups = topicToGroups.get(topic);
            for (String group : groups) {
                String topicGroup = topic + ":" + group;
                allTopicGroups.add(topicGroup);
            }
        }

        log.info("Total topic:group connections to create: {}", allTopicGroups.size());

        int successCount = 0;
        int failCount = 0;

        for (String topicGroup : allTopicGroups) {
            try {
                connectToTopicGroup(topicGroup);
                successCount++;
            } catch (Exception e) {
                log.error("✗ Failed to connect topic:group '{}': {}", topicGroup, e.getMessage());
                failCount++;
                // Schedule per-topic:group reconnect for failed initial connections
                scheduleTopicGroupReconnect(topicGroup);
            }
        }

        if (successCount > 0) {
            log.info("✓ Connected {} topic:group connection(s) successfully, {} failed (retry scheduled)",
                    successCount, failCount);
            reconnectAttempts.set(0);
            startHealthMonitoring();
        } else {
            log.error("✗ All topic:group connections failed, individual retries scheduled");
        }
    }

    /**
     * N1 + N2 FIX: Create dedicated connection for a single topic:group combination
     * Uses shared EventLoopGroup to prevent thread explosion
     */
    private void connectToTopicGroup(String topicGroup) throws Exception {
        int attempt = reconnectAttemptsPerTopicGroup.computeIfAbsent(topicGroup, k -> new AtomicInteger(0)).get() + 1;

        log.info("Connecting topic:group '{}' to broker (attempt {})...", topicGroup, attempt);

        // Parse topic and group from "topic:group"
        String[] parts = topicGroup.split(":", 2);
        String topic = parts[0];
        String group = parts[1];

        // N2 FIX: Create new client with shared EventLoopGroup
        NettyTcpClient topicGroupClient = new NettyTcpClient(sharedEventLoopGroup, decodeErrorRecorder);
        clientsPerTopicGroup.put(topicGroup, topicGroupClient);

        // Connect
        CompletableFuture<NettyTcpClient.Connection> connectFuture =
            topicGroupClient.connect(brokerHost, brokerPort);

        NettyTcpClient.Connection topicGroupConnection = connectFuture.get(10, TimeUnit.SECONDS);
        connectionsPerTopicGroup.put(topicGroup, topicGroupConnection);

        // Set up message handler for this topic:group's connection
        // N1 FIX: Pass topicGroup to track which connection the message came from
        topicGroupConnection.onMessage(message -> handleMessage(topicGroup, message));

        // Set up disconnect handler for this topic:group
        if (topicGroupConnection instanceof NettyTcpClient.TcpConnection) {
            ((NettyTcpClient.TcpConnection) topicGroupConnection).onDisconnect(() -> {
                log.warn("⚠ Connection lost for topic:group '{}' - triggering reconnection", topicGroup);
                connectedPerTopicGroup.put(topicGroup, new AtomicBoolean(false));
                scheduleTopicGroupReconnect(topicGroup);
            });
        }

        connectedPerTopicGroup.put(topicGroup, new AtomicBoolean(true));
        reconnectAttemptsPerTopicGroup.get(topicGroup).set(0);

        log.info("✓ Connected topic:group '{}' successfully", topicGroup);

        // Subscribe this topic:group on its dedicated connection
        subscribeTopicGroupOnConnection(topic, group, topicGroupConnection);
    }

    /**
     * N1 FIX: Subscribe a specific topic:group on its dedicated connection
     * Each connection now handles exactly one topic:group pair
     */
    private void subscribeTopicGroupOnConnection(String topic, String group, NettyTcpClient.Connection connection) {
        try {
            String payload = String.format("{\"topic\":\"%s\",\"group\":\"%s\"}", topic, group);

            BrokerMessage subscribeMsg = new BrokerMessage(
                BrokerMessage.MessageType.SUBSCRIBE,
                System.currentTimeMillis(),
                payload.getBytes(StandardCharsets.UTF_8)
            );

            String topicGroup = topic + ":" + group;
            connection.send(subscribeMsg).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to SUBSCRIBE topic:group '{}' on dedicated connection", topicGroup, ex);
                    scheduleTopicGroupReconnect(topicGroup);
                } else {
                    log.info("✓ SUBSCRIBE sent for topic:group '{}' on dedicated connection", topicGroup);
                }
            });

        } catch (Exception e) {
            log.error("Error subscribing topic '{}' group '{}' on dedicated connection", topic, group, e);
        }
    }

    /**
     * B1-7 FIX: Schedule reconnection for a specific topic
     */
    /**
     * N1 FIX: Schedule reconnect for a specific topic:group connection
     */
    private void scheduleTopicGroupReconnect(String topicGroup) {
        if (!running.get() || !reconnectScheduledPerTopicGroup.add(topicGroup)) {
            return;
        }

        AtomicInteger attempts = reconnectAttemptsPerTopicGroup.computeIfAbsent(topicGroup, k -> new AtomicInteger(0));
        int attemptCount = attempts.incrementAndGet();

        long delayMs = Math.min(
            INITIAL_RECONNECT_DELAY_MS * (1L << Math.min(attemptCount - 1, 4)),
            MAX_RECONNECT_DELAY_MS
        );

        log.info("⟳ Scheduling reconnect for topic:group '{}' in {}ms (attempt {})", topicGroup, delayMs, attemptCount);

        try {
            reconnectScheduler.schedule(() -> {
                reconnectScheduledPerTopicGroup.remove(topicGroup);
                if (!running.get()) {
                    return;
                }
                cleanupTopicGroupConnection(topicGroup);
                try {
                    connectToTopicGroup(topicGroup);
                } catch (Exception e) {
                    log.error("Reconnect failed for topic:group '{}': {}", topicGroup, e.getMessage());
                    scheduleTopicGroupReconnect(topicGroup);
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            reconnectScheduledPerTopicGroup.remove(topicGroup);
            if (running.get()) {
                throw e;
            }
        }
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
    /**
     * N1 FIX: Health monitoring for all topic:group connections
     */
    private void startHealthMonitoring() {
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
        }

        healthCheckTask = healthCheckScheduler.scheduleAtFixedRate(() -> {
            try {
                int aliveCount = 0;
                int deadCount = 0;

                for (Map.Entry<String, NettyTcpClient.Connection> entry : connectionsPerTopicGroup.entrySet()) {
                    String topicGroup = entry.getKey();
                    NettyTcpClient.Connection conn = entry.getValue();

                    AtomicBoolean isConnected = connectedPerTopicGroup.get(topicGroup);
                    if (isConnected == null || !isConnected.get()) {
                        log.debug("Health check: topic:group '{}' already disconnected, skip check", topicGroup);
                        continue;
                    }

                    if (conn == null || !conn.isAlive()) {
                        log.warn("Health check failed for topic:group '{}': connection is " +
                                (conn == null ? "null" : "not alive"), topicGroup);
                        isConnected.set(false);
                        scheduleTopicGroupReconnect(topicGroup);
                        deadCount++;
                    } else {
                        aliveCount++;
                    }
                }

                if (aliveCount + deadCount > 0) {
                    log.debug("Health check: {} alive, {} dead topic:group connections", aliveCount, deadCount);
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
    /**
     * N1 FIX: Handle message from a specific topic:group connection
     * @param topicGroup The "topic:group" identifier for this connection
     * @param message The received message
     */
    private void handleMessage(String topicGroup, BrokerMessage message) {
        try {
            // Extract topic from topicGroup (format: "topic:group")
            String topic = topicGroup.split(":", 2)[0];

            log.debug("Received message for topic:group '{}': type={}", topicGroup, message.getType());

            switch (message.getType()) {
                case DATA:
                case BATCH_HEADER:
                    handleDataMessage(topicGroup, message);
                    break;

                case ACK:
                    log.debug("Received ACK for topic:group '{}'", topicGroup);
                    break;

                case RESET:
                case READY:
                    // N1 FIX: Pass topicGroup to send ACK on correct connection
                    handleControlMessage(topic, topicGroup, message);
                    break;

                case DISCONNECT:
                    log.warn("Received DISCONNECT from broker for topic:group '{}'", topicGroup);
                    AtomicBoolean connected = connectedPerTopicGroup.get(topicGroup);
                    if (connected != null) {
                        connected.set(false);
                    }
                    scheduleTopicGroupReconnect(topicGroup);
                    break;

                default:
                    log.debug("Received message type {} for topic:group '{}'", message.getType(), topicGroup);
            }

        } catch (Exception e) {
            log.error("Error handling message for topic:group '{}'", topicGroup, e);
        }
    }

    /**
     * B1-7 FIX: Handle DATA message for a specific topic
     * FIX #1: Route to ALL handlers registered for this topic (may have multiple groups)
     */
    private void handleDataMessage(String topicGroup, BrokerMessage message) {
        String topic = topicGroup.split(":", 2)[0];
        try {
            byte[] payload = message.getPayload();

            List<ConsumerRecord> records;
            if (payload.length > 0 && payload[0] == '[') {
                records = parseJsonBatch(payload);
            } else {
                records = parseRawBatch(payload);
            }

            // #13: route ONLY to handlers for THIS topic:group — a batch that arrived on group A's
            // connection must not be delivered into group B's handler for the same topic.
            List<MessageHandler> handlers = topicGroupToHandlers.get(topicGroup);
            if (handlers != null && !handlers.isEmpty()) {
                boolean allSucceeded = true;
                for (MessageHandler handler : handlers) {
                    try {
                        handler.handleBatch(records);
                        log.debug("Delivered {} records to handler {} for topic '{}'",
                                records.size(), handler.getClass().getSimpleName(), topic);
                    } catch (Exception e) {
                        log.error("Error in consumer handler {} for topic '{}'",
                                handler.getClass().getSimpleName(), topic, e);
                        allSucceeded = false;
                    }
                }
                // #1 (at-least-once): ACK only AFTER every handler has processed the batch. The
                // BATCH_ACK used to be sent by BatchAckHandler on the wire BEFORE this point, so a
                // failing/crashing handler would lose data the broker had already committed. On any
                // handler failure we now skip the ack — the broker's ack-timeout reverts the offset
                // and redelivers.
                if (allSucceeded) {
                    sendBatchAck(topicGroup, records.size());
                } else {
                    log.warn("Not acking batch for topic:group '{}' — a handler failed; broker will "
                            + "redeliver after ack-timeout", topicGroup);
                }
            } else {
                // No handler for this connection: do NOT ack unprocessed data — let it redeliver.
                log.error("No handlers for topic:group '{}'; not acking {} records (will redeliver)",
                        topicGroup, records.size());
            }

        } catch (Exception e) {
            log.error("Error handling DATA message for topic '{}'", topic, e);
        }
    }

    /**
     * #1: send a BATCH_ACK for a topic:group AFTER its batch was successfully processed by every
     * handler. Moved here from {@code BatchAckHandler} (which acked on the wire before processing).
     * Payload mirrors RESET_ACK/READY_ACK: [topicLen:4][topic][groupLen:4][group].
     */
    private void sendBatchAck(String topicGroup, int recordCount) {
        NettyTcpClient.Connection connection = connectionsPerTopicGroup.get(topicGroup);
        if (connection == null) {
            log.error("No connection found for topic:group '{}' to send BATCH_ACK ({} records will "
                    + "redeliver)", topicGroup, recordCount);
            return;
        }
        String[] parts = topicGroup.split(":", 2);
        byte[] topicBytes = parts[0].getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = (parts.length > 1 ? parts[1] : "").getBytes(StandardCharsets.UTF_8);

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(groupBytes.length);
        buffer.put(groupBytes);

        BrokerMessage ack = new BrokerMessage(
                BrokerMessage.MessageType.BATCH_ACK,
                System.currentTimeMillis(),
                buffer.array()
        );

        connection.send(ack).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Failed to send BATCH_ACK for topic:group '{}'", topicGroup, ex);
            } else {
                log.debug("BATCH_ACK sent for topic:group '{}' after processing {} records",
                        topicGroup, recordCount);
            }
        });
    }


    /**
     * B1-7 FIX: Handle control message for a specific topic
     */
    /**
     * N1 FIX: Handle control messages (RESET/READY) on a specific topic:group connection
     */
    private void handleControlMessage(String topic, String topicGroup, BrokerMessage message) {
        try {
            if (message.getType() == BrokerMessage.MessageType.RESET) {
                handleResetMessage(topic, topicGroup, message);
            } else if (message.getType() == BrokerMessage.MessageType.READY) {
                handleReadyMessage(topic, topicGroup, message);
            }
        } catch (Exception e) {
            log.error("Error handling control message for topic:group '{}'", topicGroup, e);
        }
    }

    /**
     * N1 FIX: Handle RESET message for a specific topic:group connection
     * Calls onReset on ALL handlers for the topic, but sends exactly ONE ACK for this specific topic:group
     */
    private void handleResetMessage(String topic, String topicGroup, BrokerMessage message) {
        try {
            // Parse RESET message to get topic (should match the connection's topic)
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String messageTopic = extractTopicFromPayload(payload);

            log.info("Received RESET for topic:group '{}', preparing to receive refreshed data", topicGroup);

            // #13: call onReset only on handlers for THIS topic:group — group B must not be
            // reset just because group A refreshed.
            List<MessageHandler> handlers = topicGroupToHandlers.get(topicGroup);
            boolean allSucceeded = true;
            if (handlers != null && !handlers.isEmpty()) {
                for (MessageHandler handler : handlers) {
                    try {
                        handler.onReset(messageTopic);
                        log.info("Called onReset for topic '{}' on handler {}",
                                messageTopic, handler.getClass().getSimpleName());
                    } catch (Exception e) {
                        log.error("Error in consumer onReset handler {} for topic '{}'",
                                handler.getClass().getSimpleName(), messageTopic, e);
                        allSucceeded = false;
                    }
                }
            }

            // #1: ACK the RESET only if every handler actually reset. If a handler threw, the
            // consumer is NOT in a clean reset state — sending RESET_ACK would let the broker
            // proceed to replay refreshed data onto a half-reset consumer. Withhold the ack so the
            // refresh times out and aborts (leaving ingestion paused, by design) instead.
            if (!allSucceeded) {
                log.error("onReset failed for topic:group '{}' — withholding RESET_ACK; the refresh "
                        + "will time out rather than replay onto a half-reset consumer", topicGroup);
                return;
            }

            // N1 FIX: Get connection for THIS specific topic:group
            NettyTcpClient.Connection topicGroupConnection = connectionsPerTopicGroup.get(topicGroup);

            if (topicGroupConnection == null) {
                log.error("No connection found for topic:group '{}' to send RESET ACK", topicGroup);
                return;
            }

            // Extract group from topicGroup (format: "topic:group")
            String group = topicGroup.split(":", 2)[1];

            // Send exactly ONE RESET ACK for this topic:group on its dedicated connection
            // Format: [topicLen:4][topic:var][groupLen:4][group:var]
            byte[] topicBytes = messageTopic.getBytes(StandardCharsets.UTF_8);
            byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8);

            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length);
            buffer.putInt(topicBytes.length);
            buffer.put(topicBytes);
            buffer.putInt(groupBytes.length);
            buffer.put(groupBytes);

            BrokerMessage resetAck = new BrokerMessage(
                BrokerMessage.MessageType.RESET_ACK,
                System.currentTimeMillis(),
                buffer.array()
            );

            topicGroupConnection.send(resetAck).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send RESET ACK for topic:group '{}'", topicGroup, ex);
                } else {
                    log.info("✓ RESET ACK sent for topic:group '{}' on dedicated connection", topicGroup);
                }
            });

        } catch (Exception e) {
            log.error("Error handling RESET message for topic:group '{}'", topicGroup, e);
        }
    }

    /**
     * N1 FIX: Handle READY message for a specific topic:group connection
     * Calls onReady on ALL handlers for the topic, but sends exactly ONE ACK for this specific topic:group
     */
    private void handleReadyMessage(String topic, String topicGroup, BrokerMessage message) {
        try {
            // Parse READY message to get topic (should match the connection's topic)
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            String messageTopic = extractTopicFromPayload(payload);

            log.info("Received READY for topic:group '{}', refresh complete", topicGroup);

            // #13: call onReady only on handlers for THIS topic:group.
            List<MessageHandler> handlers = topicGroupToHandlers.get(topicGroup);
            boolean allSucceeded = true;
            if (handlers != null && !handlers.isEmpty()) {
                for (MessageHandler handler : handlers) {
                    try {
                        handler.onReady(messageTopic);
                        log.info("Called onReady for topic '{}' on handler {}",
                                messageTopic, handler.getClass().getSimpleName());
                    } catch (Exception e) {
                        log.error("Error in consumer onReady handler {} for topic '{}'",
                                handler.getClass().getSimpleName(), messageTopic, e);
                        allSucceeded = false;
                    }
                }
            }

            // #1: ACK the READY only if every handler completed onReady. If a handler threw, the
            // consumer has not cleanly finished the refresh — withhold READY_ACK so the broker does
            // not mark the refresh complete (and resume the pipe) against a consumer that failed to
            // activate the refreshed data. The refresh then times out and aborts.
            if (!allSucceeded) {
                log.error("onReady failed for topic:group '{}' — withholding READY_ACK; refresh will "
                        + "not be marked complete", topicGroup);
                return;
            }

            // N1 FIX: Get connection for THIS specific topic:group
            NettyTcpClient.Connection topicGroupConnection = connectionsPerTopicGroup.get(topicGroup);

            if (topicGroupConnection == null) {
                log.error("No connection found for topic:group '{}' to send READY ACK", topicGroup);
                return;
            }

            // Extract group from topicGroup (format: "topic:group")
            String group = topicGroup.split(":", 2)[1];

            // Send exactly ONE READY ACK for this topic:group on its dedicated connection
            // Format: [topicLen:4][topic:var][groupLen:4][group:var]
            byte[] topicBytes = messageTopic.getBytes(StandardCharsets.UTF_8);
            byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8);

            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length);
            buffer.putInt(topicBytes.length);
            buffer.put(topicBytes);
            buffer.putInt(groupBytes.length);
            buffer.put(groupBytes);

            BrokerMessage readyAck = new BrokerMessage(
                BrokerMessage.MessageType.READY_ACK,
                System.currentTimeMillis(),
                buffer.array()
            );

            topicGroupConnection.send(readyAck).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Failed to send READY ACK for topic:group '{}'", topicGroup, ex);
                } else {
                    log.info("✓ READY ACK sent for topic:group '{}' on dedicated connection", topicGroup);
                }
            });

        } catch (Exception e) {
            log.error("Error handling READY message for topic:group '{}'", topicGroup, e);
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
    /**
     * N1 FIX: Cleanup connection for a specific topic:group
     */
    private void cleanupTopicGroupConnection(String topicGroup) {
        NettyTcpClient.Connection conn = connectionsPerTopicGroup.remove(topicGroup);
        if (conn != null) {
            try {
                conn.disconnect();
                log.debug("Disconnected topic:group '{}' connection", topicGroup);
            } catch (Exception e) {
                log.debug("Error disconnecting topic:group '{}' connection", topicGroup, e);
            }
        }

        NettyTcpClient client = clientsPerTopicGroup.remove(topicGroup);
        if (client != null) {
            try {
                // N2 FIX: Don't shutdown client - it uses shared EventLoopGroup
                // The client will be garbage collected
                log.debug("Removed client for topic:group '{}'", topicGroup);
            } catch (Exception e) {
                log.debug("Error removing client for topic:group '{}'", topicGroup, e);
            }
        }
    }

    /**
     * B1-7 FIX: Cleanup all topic connections
     */
    /**
     * N1 FIX: Cleanup all topic:group connections
     */
    private void cleanupAllConnections() {
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
            healthCheckTask = null;
        }

        log.info("Cleaning up all {} topic:group connections...", connectionsPerTopicGroup.size());

        for (String topicGroup : new java.util.HashSet<>(connectionsPerTopicGroup.keySet())) {
            cleanupTopicGroupConnection(topicGroup);
        }

        connectionsPerTopicGroup.clear();
        clientsPerTopicGroup.clear();
        connectedPerTopicGroup.clear();

        log.info("All topic:group connections cleaned up");
    }

    /**
     * B1-7 FIX: Shutdown all per-topic connections gracefully
     */
    @PreDestroy
    public void shutdown() {
        log.info("=== ClientConsumerManager Shutting Down (N1+N2 per-topic:group connections) ===");

        running.set(false);
        reconnectScheduledPerTopicGroup.clear();

        // Mark all topic:group connections as disconnected
        for (AtomicBoolean connected : connectedPerTopicGroup.values()) {
            connected.set(false);
        }

        cleanupAllConnections();

        shutdownScheduler(reconnectScheduler, "reconnect");
        shutdownScheduler(healthCheckScheduler, "health-check");

        // N2 FIX: Shutdown shared EventLoopGroup
        if (sharedEventLoopGroup != null) {
            try {
                sharedEventLoopGroup.shutdownGracefully().sync();
                log.info("Shared EventLoopGroup shut down gracefully");
            } catch (InterruptedException e) {
                log.warn("Interrupted while shutting down EventLoopGroup", e);
                Thread.currentThread().interrupt();
            }
        }

        log.info("ClientConsumerManager shutdown complete - closed {} topic:group connections",
                connectionsPerTopicGroup.size());
    }

    private void shutdownScheduler(ScheduledExecutorService scheduler, String name) {
        if (scheduler == null) {
            return;
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("{} scheduler did not terminate within 5 seconds", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while stopping {} scheduler", name);
        }
    }

    /**
     * N1 FIX: Check if any topic:group connection is alive
     */
    public boolean isConnected() {
        return connectedPerTopicGroup.values().stream()
                .anyMatch(AtomicBoolean::get);
    }

    public int getReconnectAttempts() {
        return reconnectAttempts.get();
    }

    /**
     * N1 FIX: Get total number of active topic:group connections
     */
    public int getConnectedTopicCount() {
        return (int) connectedPerTopicGroup.values().stream()
                .filter(AtomicBoolean::get)
                .count();
    }

    /**
     * N1 FIX: Get total number of topic:group connections (connected + disconnected)
     */
    public int getTotalTopicCount() {
        return connectionsPerTopicGroup.size();
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
