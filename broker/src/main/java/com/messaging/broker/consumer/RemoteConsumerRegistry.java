package com.messaging.broker.consumer;

import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.broker.metrics.DataRefreshMetrics;
import com.messaging.broker.refresh.DataRefreshManager;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.mmap.MMapStorageEngine;
import com.messaging.storage.segment.Segment;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.netty.channel.FileRegion;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Registry for TCP-connected remote consumers
 */
@Singleton
public class RemoteConsumerRegistry {
    private static final Logger log = LoggerFactory.getLogger(RemoteConsumerRegistry.class);

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final DataRefreshMetrics dataRefreshMetrics;
    private volatile DataRefreshManager dataRefreshManager; // Lazy injection to avoid circular dependency
    private volatile AdaptiveBatchDeliveryManager adaptiveDeliveryManager; // Lazy injection to avoid circular dependency
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService storageExecutor; // Separate executor for storage operations to prevent deadlock
    private final long maxMessageSizePerConsumer;

    // Map: "clientId:topic" -> RemoteConsumer (composite key to support multiple topic subscriptions per client)
    private final Map<String, RemoteConsumer> consumers;

    // ACK-based flow control tracking
    // Map: "clientId:topic" -> in-flight delivery status (true = delivery in progress)
    private final Map<String, AtomicBoolean> inFlightDeliveries;

    // Map: "clientId:topic:pending" -> next offset to commit on ACK
    private final Map<String, Long> pendingOffsets;

    @Inject
    public RemoteConsumerRegistry(StorageEngine storage, NetworkServer server,
                                  ConsumerOffsetTracker offsetTracker, BrokerMetrics metrics,
                                  DataRefreshMetrics dataRefreshMetrics,
                                  @Value("${broker.consumer.max-message-size-per-consumer}") long maxMessageSizePerConsumer) {
        this.storage = storage;
        this.server = server;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.dataRefreshMetrics = dataRefreshMetrics;
        this.maxMessageSizePerConsumer = maxMessageSizePerConsumer;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules(); // Register JSR310 module for Java 8 date/time
        // Reduced thread pool: use half of available cores (min 2) to lower CPU usage
        int schedulerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        this.scheduler = Executors.newScheduledThreadPool(schedulerThreads, runnable -> {
            Thread t = new Thread(runnable);
            t.setName("RemoteConsumerRegistry-" + t.getId());
            return t;
        });
        // Separate thread pool for storage operations to prevent deadlock
        // Reduced to 1x CPU cores to minimize memory usage (was 2x)
        this.storageExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), runnable -> {
            Thread t = new Thread(runnable);
            t.setName("StorageReader-" + t.getId());
            return t;
        });
        this.consumers = new ConcurrentHashMap<>();
        this.inFlightDeliveries = new ConcurrentHashMap<>();
        this.pendingOffsets = new ConcurrentHashMap<>();
        log.info("RemoteConsumerRegistry initialized with maxMessageSize={}bytes per consumer (size-only batching)",
                 maxMessageSizePerConsumer);
    }

    /**
     * Lazy setter for DataRefreshManager to avoid circular dependency
     */
    public void setDataRefreshManager(DataRefreshManager dataRefreshManager) {
        this.dataRefreshManager = dataRefreshManager;
    }

    public void setAdaptiveBatchDeliveryManager(AdaptiveBatchDeliveryManager adaptiveDeliveryManager) {
        this.adaptiveDeliveryManager = adaptiveDeliveryManager;
    }

    /**
     * Register a remote consumer when it subscribes
     */
    public void registerConsumer(String clientId, String topic, String group) {
        RemoteConsumer consumer = new RemoteConsumer(clientId, topic, group);

        // Load persisted offset from property file - ONLY source of truth
        String consumerId = group + ":" + topic;
        long startOffset = offsetTracker.getOffset(consumerId);

        consumer.setCurrentOffset(startOffset);

        // Initialize consumer metrics
        metrics.updateConsumerOffset(clientId, topic, group, startOffset);
        metrics.updateConsumerLag(clientId, topic, group, 0);

        // Use composite key to support multiple topic subscriptions per client
        String consumerKey = clientId + ":" + topic;
        consumers.put(consumerKey, consumer);

        // Notify adaptive delivery manager to start polling for this consumer
        if (adaptiveDeliveryManager != null) {
            adaptiveDeliveryManager.registerConsumer(consumer);
            log.info("Triggered adaptive delivery for new consumer: {}:{}", clientId, topic);
        }

        log.debug("Registered remote consumer: consumerKey={}, clientId={}, topic={}, group={}, startOffset={}",
                 consumerKey, clientId, topic, group, startOffset);
    }

    /**
     * Unregister consumer when it disconnects
     * Removes all topic subscriptions for this client
     *
     * @return Number of topic subscriptions removed
     */
    public int unregisterConsumer(String clientId) {
        // Find and remove all consumers for this clientId (all topics)
        List<String> keysToRemove = new ArrayList<>();
        for (Map.Entry<String, RemoteConsumer> entry : consumers.entrySet()) {
            if (entry.getValue().clientId.equals(clientId)) {
                keysToRemove.add(entry.getKey());
            }
        }

        int removedCount = 0;
        for (String key : keysToRemove) {
            RemoteConsumer consumer = consumers.remove(key);
            if (consumer != null) {
                if (consumer.deliveryTask != null) {
                    consumer.deliveryTask.cancel(false);
                }
                // Clean up metrics (though with stable identifiers, metrics are now retained)
                metrics.removeConsumerMetrics(clientId, consumer.topic, consumer.group);
                log.info("Unregistered remote consumer: consumerKey={}, clientId={}, topic={}, group={}",
                         key, clientId, consumer.topic, consumer.group);
                removedCount++;
            }
        }

        log.info("Unregistered {} topic subscriptions for client: {}", removedCount, clientId);
        return removedCount;
    }

    /**
     * Get all registered consumers
     * Used by AdaptiveBatchDeliveryManager to schedule delivery tasks
     *
     * @return List of all registered consumers
     */
    public List<RemoteConsumer> getAllConsumers() {
        return new ArrayList<>(consumers.values());
    }

    /**
     * Deliver batch to consumer (for AdaptiveBatchDeliveryManager)
     * Returns true if data was found and delivered, false if skipped
     *
     * @param consumer Consumer to deliver to
     * @param batchSizeBytes Max batch size in bytes
     * @return true if data delivered, false if no data or delivery blocked
     */
    public boolean deliverBatch(RemoteConsumer consumer, long batchSizeBytes) {
        String deliveryKey = consumer.clientId + ":" + consumer.topic;
        String pendingKey = deliveryKey + ":pending";

        AtomicBoolean inFlight = inFlightDeliveries.computeIfAbsent(
                deliveryKey, k -> new AtomicBoolean(false)
        );

        // Gate 1: Check in-flight (per topic:consumer)
        if (!inFlight.compareAndSet(false, true)) {
            log.info("DEBUG deliverBatch: Gate 1 BLOCKED (in-flight) for {}", deliveryKey);
            return false;  // Already delivering to this consumer:topic
        }

        // Gate 2: Check pending ACK
        if (pendingOffsets.containsKey(pendingKey)) {
            log.info("DEBUG deliverBatch: Gate 2 BLOCKED (pending ACK) for {}, pendingOffset={}",
                     deliveryKey, pendingOffsets.get(pendingKey));
            inFlight.set(false);
            return false;  // Waiting for ACK
        }

        log.info("DEBUG deliverBatch: Gates passed, proceeding with batch read for {}", deliveryKey);

        // Gate 3 removed: Parallel topic delivery now allowed

        long startOffset = consumer.getCurrentOffset();
        Timer.Sample readSample = null;
        Timer.Sample deliverySample = null;

        try {
            /* ================= STORAGE READ METRICS ================= */
            readSample = metrics.startStorageReadTimer();

            Segment.BatchFileRegion batch = storageExecutor.submit(() ->
                    ((com.messaging.storage.filechannel.FileChannelStorageEngine) storage)
                            .getZeroCopyBatch(
                                    consumer.topic,
                                    0,
                                    startOffset,
                                    batchSizeBytes
                            )
            ).get(10, TimeUnit.MINUTES);

            metrics.stopStorageReadTimer(readSample);
            metrics.recordStorageRead();

            log.info("DEBUG deliverBatch: Batch read complete for {}, recordCount={}, fileRegion={}",
                     deliveryKey, batch.recordCount, (batch.fileRegion != null ? "present" : "NULL"));

            if (batch.recordCount == 0 || batch.fileRegion == null) {
                log.info("DEBUG deliverBatch: EMPTY BATCH (recordCount={}, fileRegion={}) for {}, startOffset={}",
                         batch.recordCount, (batch.fileRegion != null ? "present" : "NULL"), deliveryKey, startOffset);
                inFlight.set(false);
                return false;  // No data available
            }

            /* ================= BATCH VISIBILITY ================= */
            metrics.recordBatchSize(batch.recordCount);

            /* ================= OFFSET RESERVATION ================= */
            // Store original offset BEFORE reservation (for timeout revert)
            long originalOffset = startOffset;
            long nextOffset = batch.lastOffset + 1;
            consumer.setCurrentOffset(nextOffset);
            pendingOffsets.put(pendingKey, nextOffset);

            /* ================= ACK TIMEOUT SETUP ================= */
            scheduler.schedule(() -> {
                if (pendingOffsets.remove(pendingKey) != null) {
                    log.warn("ACK timeout for {}, reverting offset from {} to {}",
                             deliveryKey, nextOffset, originalOffset);

                    // REVERT consumer offset to prevent delivery gap
                    consumer.setCurrentOffset(originalOffset);
                    inFlight.set(false);

                    metrics.recordAckTimeout(consumer.topic);
                }
            }, 30, TimeUnit.MINUTES);

            /* ================= DELIVERY METRICS ================= */
            deliverySample = metrics.startConsumerDeliveryTimer();

            sendBatchToConsumer(consumer, batch, startOffset);

            metrics.stopConsumerDeliveryTimer(
                    deliverySample,
                    consumer.clientId,
                    consumer.topic,
                    consumer.group
            );

            return true;  // Data delivered successfully

        } catch (Exception e) {
            log.error("Delivery failed for {}", deliveryKey, e);
            inFlight.set(false);

            // Revert offset reservation on error
            consumer.setCurrentOffset(startOffset);
            pendingOffsets.remove(pendingKey);

            // Check if this is a connection error (Broken pipe, Connection reset, etc.)
            if (isConnectionError(e)) {
                log.warn("Connection error detected for {}, unregistering consumer", deliveryKey);
                // Unregister consumer to stop adaptive polling
                unregisterConsumer(consumer.clientId);
            }

            return false;  // Delivery failed
        }
    }


    /**
     * Send a batch of messages to consumer using Kafka-style zero-copy (sendfile syscall)
     * This eliminates heap allocations and uses OS kernel zero-copy
     */
    private void sendBatchToConsumer(RemoteConsumer consumer, Segment.BatchFileRegion batchRegion, long startOffset)
            throws Exception {

        if (batchRegion.fileRegion == null) {
            log.debug("No data to send for consumer {}", consumer.clientId);
            return;
        }

        // Step 1: Create and send header message with batch metadata
        // Include topic name so consumer knows what to ACK
        byte[] topicBytes = consumer.topic.getBytes(StandardCharsets.UTF_8);
        int topicLen = topicBytes.length;

        // UNIFIED FORMAT Header: [recordCount:4][totalBytes:8][topicLen:4][topic:var]
        // Removed lastOffset - consumer doesn't need it (broker tracks offsets)
        ByteBuffer headerBuffer = ByteBuffer.allocate(12 + 4 + topicLen);
        headerBuffer.putInt(batchRegion.recordCount);
        headerBuffer.putLong(batchRegion.totalBytes);
        headerBuffer.putInt(topicLen);
        headerBuffer.put(topicBytes);
        headerBuffer.flip();

        byte[] header = new byte[12 + 4 + topicLen];
        headerBuffer.get(header);

        // Send header using BATCH_HEADER message type
        BrokerMessage headerMsg = new BrokerMessage(
            BrokerMessage.MessageType.BATCH_HEADER,
            System.currentTimeMillis(),
            header
        );

        log.debug("Sending BATCH_HEADER to consumer {}: topic={}, recordCount={}, totalBytes={}",
                 consumer.clientId, consumer.topic, batchRegion.recordCount, batchRegion.totalBytes);

        long timeoutSeconds = 1 + (batchRegion.totalBytes / (1024 * 1024) * 10);
        // Increased header timeout from 1s to 10s for reliability
        server.send(consumer.clientId, headerMsg).get(timeoutSeconds, TimeUnit.MINUTES);

        // Step 2: Send FileRegion for true zero-copy transfer (Kafka-style sendfile)
        // This uses OS sendfile() syscall - data goes from file → kernel → socket
        // NO user-space copies, NO heap allocations!
        log.info("Sending zero-copy FileRegion: position={}, count={}, records={}, bytes={}, consumer={}, topic={}, startOffset={}",
                 batchRegion.fileRegion.position(), batchRegion.fileRegion.count(),
                 batchRegion.recordCount, batchRegion.totalBytes, consumer.clientId, consumer.topic, startOffset);

        // Increased timeout for larger batches (3MB batches may take longer to transfer)
        // Timeout = 10s base + 10s per MB (e.g., 3MB = 10 + 30 = 40 seconds)
        log.debug("Sending FileRegion with timeout of {}s for {} bytes", timeoutSeconds, batchRegion.totalBytes);

        server.sendFileRegion(consumer.clientId, batchRegion.fileRegion)
              .get(timeoutSeconds, TimeUnit.MINUTES);

        log.info("✓ Sent zero-copy batch to consumer {}: recordCount={}, bytes={}, startOffset={}, lastOffset={}",
                 consumer.clientId, batchRegion.recordCount, batchRegion.totalBytes, startOffset, batchRegion.lastOffset);

        // Record metrics for messages and bytes sent (global counters)
        metrics.recordBatchMessagesSent(batchRegion.recordCount, batchRegion.totalBytes);

        // Record per-consumer metrics (efficient batch version)
        metrics.recordConsumerBatchSent(consumer.clientId, consumer.topic, consumer.group,
                                       batchRegion.recordCount, batchRegion.totalBytes);
    }

    /**
     * Handle BATCH_ACK from consumer
     * This is called when the consumer successfully processes a batch and sends acknowledgment
     *
     * @param clientId The client ID that sent the ACK
     * @param topic The topic that was acknowledged
     */
    public void handleBatchAck(String clientId, String topic) {
        String deliveryKey = clientId + ":" + topic;
        String pendingKey = deliveryKey + ":pending";

        Long committedOffset = pendingOffsets.remove(pendingKey);
        if (committedOffset == null) {
            log.warn("ACK with no pending offset: {}", deliveryKey);
            return;
        }

        RemoteConsumer consumer = consumers.get(deliveryKey);
        if (consumer != null) {
            offsetTracker.updateOffset(consumer.group + ":" + topic, committedOffset);
            // Update Prometheus gauge metric to reflect new offset
            metrics.updateConsumerOffset(clientId, topic, consumer.group, committedOffset);
        }

        AtomicBoolean inFlight = inFlightDeliveries.get(deliveryKey);
        if (inFlight != null) {
            inFlight.set(false);
        }

        log.info("ACK committed for {} at offset {}", deliveryKey, committedOffset);
    }

    /**
     * Get all consumer clientIds subscribed to a topic (for DataRefresh)
     */
    public java.util.Set<String> getConsumersForTopic(String topic) {
        java.util.Set<String> clientIds = new java.util.HashSet<>();
        for (Map.Entry<String, RemoteConsumer> entry : consumers.entrySet()) {
            if (entry.getValue().topic.equals(topic)) {
                clientIds.add(entry.getValue().clientId);
            }
        }
        log.debug("Found {} consumers for topic: {}", clientIds.size(), topic);
        return clientIds;
    }

    /**
     * Broadcast RESET message to all consumers of a topic (for DataRefresh)
     */
    public void broadcastResetToTopic(String topic) {
        java.util.Set<String> clientIds = getConsumersForTopic(topic);
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);

        BrokerMessage resetMsg = new BrokerMessage(
            BrokerMessage.MessageType.RESET,
            System.currentTimeMillis(),
            payload
        );

        for (String clientId : clientIds) {
            try {
                server.send(clientId, resetMsg);
                log.info("Sent RESET to consumer: {} for topic: {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send RESET to consumer: {}", clientId, e);
            }
        }
    }

    /**
     * Broadcast READY message to all consumers of a topic (for DataRefresh)
     */
    public void broadcastReadyToTopic(String topic) {
        java.util.Set<String> clientIds = getConsumersForTopic(topic);
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);

        BrokerMessage readyMsg = new BrokerMessage(
            BrokerMessage.MessageType.READY,
            System.currentTimeMillis(),
            payload
        );

        for (String clientId : clientIds) {
            try {
                server.send(clientId, readyMsg);
                log.info("Sent READY to consumer: {} for topic: {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send READY to consumer: {}", clientId, e);
            }
        }
    }

    /**
     * Reset offset for a specific consumer (for DataRefresh)
     */
    public void resetConsumerOffset(String clientId, String topic, long offset) {
        String consumerKey = clientId + ":" + topic;
        RemoteConsumer consumer = consumers.get(consumerKey);

        if (consumer == null) {
            log.warn("Cannot reset offset - consumer not found: consumerKey={}", consumerKey);
            return;
        }

        consumer.setCurrentOffset(offset);

        String consumerId = consumer.group + ":" + consumer.topic;
        offsetTracker.resetOffset(consumerId, offset);

        log.info("Reset offset to {} for consumer: {} ({})", offset, clientId, consumerId);
    }

    /**
     * Check if all specified consumers have caught up to latest offset (for DataRefresh)
     */
    public boolean allConsumersCaughtUp(String topic, java.util.Set<String> consumerClientIds) {
        // Use getMaxOffsetFromMetadata instead of getCurrentOffset for data refresh
        // getCurrentOffset returns the in-memory write head which may be low during refresh (pipes paused)
        // getMaxOffsetFromMetadata reads from persistent segment metadata to get the true max offset
        long latestOffset = storage.getMaxOffsetFromMetadata(topic, 0);

        log.debug("Checking if consumers caught up for topic {}: latestOffset from metadata = {}", topic, latestOffset);

        for (String clientId : consumerClientIds) {
            String consumerKey = clientId + ":" + topic;
            RemoteConsumer consumer = consumers.entrySet()
                    .stream()
                    .filter(e -> e.getKey().contains(topic))
                    .peek(e -> log.debug("Consumer key in registry: {}", e.getKey()))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElse(null);

            if (consumer == null) {
                log.debug("Consumer not found: consumerKey={}", consumerKey);
                return false;
            }

            if (consumer.getCurrentOffset() < latestOffset) {
                log.debug("Consumer {} not caught up: {} < {} (need {} more messages)",
                         clientId, consumer.getCurrentOffset(), latestOffset, latestOffset - consumer.getCurrentOffset());
                return false;
            }
        }

        log.info("✓ All consumers caught up for topic {} at offset {} (from metadata)", topic, latestOffset);
        return true;
    }

    /**
     * Get consumer group:topic identifier for a client (for DataRefresh)
     * Maps dynamic clientId to stable "group:topic" identifier
     */
    public String getConsumerGroupTopic(String clientId, String topic) {
        String consumerKey = clientId + ":" + topic;
        RemoteConsumer consumer = consumers.get(consumerKey);

        if (consumer == null) {
            log.warn("Consumer not found for clientId={}, topic={}", clientId, topic);
            return null;
        }

        return consumer.topic;
    }

    /**
     * Shutdown registry
     */
    public void shutdown() {
        log.info("Shutting down RemoteConsumerRegistry...");

        for (RemoteConsumer consumer : consumers.values()) {
            if (consumer.deliveryTask != null) {
                consumer.deliveryTask.cancel(false);
            }
        }

        scheduler.shutdown();
        storageExecutor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            storageExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("RemoteConsumerRegistry shutdown complete");
    }


    /**
     * Get all consumer client IDs for a topic (for DataRefresh replay)
     * Returns ALL consumers subscribed to the topic, not just one
     */
    public java.util.List<String> getAllConsumerIds(String topic) {
        return this.consumers.entrySet()
                .stream()
                .filter(e -> e.getValue().topic.equals(topic))
                .peek(e -> log.debug("Found consumer for topic {}: key={}, clientId={}",
                        topic, e.getKey(), e.getValue().clientId))
                .map(e -> e.getValue().clientId)
                .collect(Collectors.toList());
    }

    /**
     * @deprecated Use getAllConsumerIds() instead - this only returns ONE consumer!
     */
    @Deprecated
    public String getRemoteConsumers(String topic) {
        RemoteConsumer remoteConsumer = this.consumers.entrySet()
                .stream()
                .filter(e -> e.getKey().contains(topic))
                .peek(e -> log.debug("Consumer key in registry: {}", e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
        return remoteConsumer != null ? remoteConsumer.clientId : null;
    }

    /**
     * Check if exception is a connection error (Broken pipe, Connection reset, etc.)
     */
    private boolean isConnectionError(Exception e) {
        // Check for IOException with connection-related messages
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof java.io.IOException) {
                String message = cause.getMessage();
                if (message != null) {
                    message = message.toLowerCase();
                    if (message.contains("broken pipe") ||
                        message.contains("connection reset") ||
                        message.contains("connection refused") ||
                        message.contains("socket closed") ||
                        message.contains("stream closed")) {
                        return true;
                    }
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Remote consumer metadata
     */
    public static class RemoteConsumer {
        final String clientId;
        final String topic;
        final String group;
        volatile long currentOffset;
        volatile Future<?> deliveryTask;
        volatile long lastDeliveryAttempt; // Rate limiting: timestamp of last delivery attempt

        RemoteConsumer(String clientId, String topic, String group) {
            this.clientId = clientId;
            this.topic = topic;
            this.group = group;
            this.currentOffset = 0;
            this.lastDeliveryAttempt = 0; // Allow immediate first delivery
        }

        long getCurrentOffset() {
            return currentOffset;
        }

        void setCurrentOffset(long offset) {
            this.currentOffset = offset;
        }
    }
}
