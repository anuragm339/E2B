package com.messaging.broker.consumer;

import com.messaging.broker.metrics.BrokerMetrics;
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

/**
 * Registry for TCP-connected remote consumers
 */
@Singleton
public class RemoteConsumerRegistry {
    private static final Logger log = LoggerFactory.getLogger(RemoteConsumerRegistry.class);
    private static final long POLL_INTERVAL_MS = 100;

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService storageExecutor; // Separate executor for storage operations to prevent deadlock
    private final long maxMessageSizePerConsumer;
    private final int readBatchSize;

    // Map: "clientId:topic" -> RemoteConsumer (composite key to support multiple topic subscriptions per client)
    private final Map<String, RemoteConsumer> consumers;

    // ACK-based flow control tracking
    // Map: "clientId:topic" -> in-flight delivery status (true = delivery in progress)
    private final Map<String, AtomicBoolean> inFlightDeliveries;

    // Map: clientId -> currently in-flight topic (ensures only ONE topic delivers to a consumer at a time)
    private final Map<String, String> consumerCurrentTopic;

    // Map: "clientId:topic:pending" -> next offset to commit on ACK
    private final Map<String, Long> pendingOffsets;

    @Inject
    public RemoteConsumerRegistry(StorageEngine storage, NetworkServer server,
                                  ConsumerOffsetTracker offsetTracker, BrokerMetrics metrics,
                                  @Value("${broker.consumer.max-message-size-per-consumer}") long maxMessageSizePerConsumer,  // Reduced from 1MB to 512KB
                                  @Value("${broker.consumer.max-batch-size-per-consumer}") int readBatchSize) {  // Reduced from 100 to 50
        this.storage = storage;
        this.server = server;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.maxMessageSizePerConsumer = maxMessageSizePerConsumer;
        this.readBatchSize = readBatchSize;
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
        this.consumerCurrentTopic = new ConcurrentHashMap<>();
        this.pendingOffsets = new ConcurrentHashMap<>();
        log.info("RemoteConsumerRegistry initialized with maxMessageSize={}bytes per consumer, readBatchSize={}",
                 maxMessageSizePerConsumer, readBatchSize);
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

//        // Start delivery task
//        startDelivery(consumer);

        log.debug("Registered remote consumer: consumerKey={}, clientId={}, topic={}, group={}, startOffset={}",
                 consumerKey, clientId, topic, group, startOffset);
    }

    /**
     * Unregister consumer when it disconnects
     * Removes all topic subscriptions for this client
     */
    public void unregisterConsumer(String clientId) {
        // Find and remove all consumers for this clientId (all topics)
        List<String> keysToRemove = new ArrayList<>();
        for (Map.Entry<String, RemoteConsumer> entry : consumers.entrySet()) {
            if (entry.getValue().clientId.equals(clientId)) {
                keysToRemove.add(entry.getKey());
            }
        }

        for (String key : keysToRemove) {
            RemoteConsumer consumer = consumers.remove(key);
            if (consumer != null) {
                if (consumer.deliveryTask != null) {
                    consumer.deliveryTask.cancel(false);
                }
                // Clean up metrics
                metrics.removeConsumerMetrics(clientId, consumer.topic);
                log.info("Unregistered remote consumer: consumerKey={}, clientId={}, topic={}",
                         key, clientId, consumer.topic);
            }
        }
    }

    /**
     * Start message delivery for a consumer
     */
    private void startDelivery(RemoteConsumer consumer) {
        Future<?> task = scheduler.scheduleWithFixedDelay(
                () -> deliverMessages(consumer,null),
                500, // Wait 500ms before first delivery to allow ACK to complete
                POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        consumer.deliveryTask = task;
    }

    /**
     * Deliver messages to a remote consumer
     */
    private void deliverMessages(RemoteConsumer consumer,Long offset) {
        String deliveryKey = consumer.clientId + ":" + consumer.topic;

        // Try to acquire in-flight slot for this topic+consumer
        AtomicBoolean inFlight = inFlightDeliveries.computeIfAbsent(
            deliveryKey,
            k -> new AtomicBoolean(false)
        );

        if (!inFlight.compareAndSet(false, true)) {
            log.debug("Skipping delivery - topic {} already in-flight for consumer {}",
                     consumer.topic, consumer.clientId);
            return;
        }

        // Check if ANOTHER topic is already being delivered to this consumer
        String previousTopic = consumerCurrentTopic.putIfAbsent(consumer.clientId, consumer.topic);
        if (previousTopic != null && !previousTopic.equals(consumer.topic)) {
            inFlight.set(false);
            log.debug("Consumer {} is busy with topic {}, skipping topic {}",
                     consumer.clientId, previousTopic, consumer.topic);
            return;
        }

        try {
            long currentOffset = consumer.getCurrentOffset();

            log.info("Attempting to deliver messages to consumer {}: and topic {}: offset={}",
                     consumer.clientId,consumer.topic, currentOffset);

            // Read next batch of messages from storage using zero-copy
            Segment.BatchFileRegion batchRegion;
            try {
                log.debug("About to call storage.getZeroCopyBatch() for consumer {}: topic={}, partition=0, offset={}",
                         consumer.clientId, consumer.topic, currentOffset);

                // Use a CompletableFuture with timeout to prevent indefinite blocking
                final long offsetToRead = currentOffset; // Must be final for lambda
                Timer.Sample readSample = metrics.startStorageReadTimer();

                // Call getZeroCopyBatch on appropriate storage engine
                batchRegion = CompletableFuture.supplyAsync(() -> {
                    try {
                       if (storage instanceof com.messaging.storage.filechannel.FileChannelStorageEngine) {
                            return ((com.messaging.storage.filechannel.FileChannelStorageEngine) storage).getZeroCopyBatch(consumer.topic, 0, offsetToRead,
                                    readBatchSize, maxMessageSizePerConsumer);
                        } else {
                            throw new IllegalStateException("Unknown storage engine type: " + storage.getClass().getName());
                        }
                    } catch (Exception e) {
                        log.error("Exception in storage.getZeroCopyBatch(): ", e);
                        throw new RuntimeException(e);
                    }
                }, storageExecutor).get(10, TimeUnit.SECONDS);

                metrics.stopStorageReadTimer(readSample);
                metrics.recordStorageRead();

                log.debug("storage.getZeroCopyBatch() returned successfully: recordCount={}, bytes={}",
                         batchRegion.recordCount, batchRegion.totalBytes);
            } catch (TimeoutException e) {
                log.error("TIMEOUT: storage.getZeroCopyBatch() did not complete within 10 seconds for consumer {}: topic={}, offset={}",
                         consumer.clientId, consumer.topic, currentOffset);
                return;
            } catch (Exception e) {
                log.error("Failed to read from storage for consumer {}: topic={}, offset={}",
                         consumer.clientId, consumer.topic, currentOffset, e);
                return;
            }

            if (batchRegion.recordCount == 0 || batchRegion.fileRegion == null) {
                return; // No new messages
            }

            metrics.recordBatchSize(batchRegion.recordCount);

            // Send the batch to consumer using zero-copy
            try {
                // Start timing for per-consumer delivery
                Timer.Sample deliverySample = metrics.startConsumerDeliveryTimer();
                log.info("Sending zero-copy batch of {} messages ({} bytes) to consumer {} for topic {} starting at offset {}",
                         batchRegion.recordCount, batchRegion.totalBytes, consumer.clientId, consumer.topic, currentOffset);

                sendBatchToConsumer(consumer, batchRegion, currentOffset);

                // Record per-consumer metrics
                metrics.recordConsumerMessageSent(consumer.clientId, consumer.topic, consumer.group, batchRegion.totalBytes);
                metrics.stopConsumerDeliveryTimer(deliverySample, consumer.clientId, consumer.topic, consumer.group);

                // Record global metrics - efficient batch method
                metrics.recordBatchMessagesSent(batchRegion.recordCount, batchRegion.totalBytes);

                // Update offset to after the last message sent
                currentOffset = batchRegion.lastOffset + 1;

                log.debug("Sent {} messages ({} bytes) to consumer {}, next offset={}",
                         batchRegion.recordCount, batchRegion.totalBytes, consumer.clientId, currentOffset);

                // Store pending offset (DON'T update properties file yet - wait for ACK!)
                String pendingKey = deliveryKey + ":pending";
                pendingOffsets.put(pendingKey, currentOffset);
                log.debug("Stored pending offset for ACK: deliveryKey={}, offset={}", deliveryKey, currentOffset);

            } catch (Exception e) {
                log.error("Failed to send batch to consumer {}: recordCount={}, firstOffset={}",
                         consumer.clientId, batchRegion.recordCount, currentOffset, e);
                // Record failure
                metrics.recordConsumerFailure(consumer.clientId, consumer.topic, consumer.group);
                // Clear in-flight status on error
                inFlight.set(false);
                consumerCurrentTopic.remove(consumer.clientId, consumer.topic);
                return; // Stop on error
            }

            // NOTE: Offset will be committed to properties file only after receiving BATCH_ACK
            // Update consumer offset metric to pending value
            metrics.updateConsumerOffset(consumer.clientId, consumer.topic, consumer.group, currentOffset);

            // Calculate and update lag (difference between latest message and consumer offset)
            // Get head offset from storage (getCurrentOffset returns the highest offset written)
            try {
                long headOffset = storage.getCurrentOffset(consumer.topic, 0);
                // headOffset is the last written offset, so next offset to write would be headOffset + 1
                // lag is (headOffset + 1) - currentOffset
                long lag = (headOffset + 1) - currentOffset;
                metrics.updateConsumerLag(consumer.clientId, consumer.topic, consumer.group, Math.max(0, lag));
            } catch (Exception e) {
                log.debug("Could not calculate lag for consumer {}: {}", consumer.clientId, e.getMessage());
            }

        } catch (Exception e) {
            log.error("FATAL: Error delivering messages to consumer {}, task will continue",
                     consumer.clientId, e);
            // Clear in-flight status on fatal error
            inFlight.set(false);
            consumerCurrentTopic.remove(consumer.clientId, consumer.topic);
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

        // Header: [recordCount:4][totalBytes:8][lastOffset:8][topicLen:4][topic:var]
        ByteBuffer headerBuffer = ByteBuffer.allocate(20 + 4 + topicLen);
        headerBuffer.putInt(batchRegion.recordCount);
        headerBuffer.putLong(batchRegion.totalBytes);
        headerBuffer.putLong(batchRegion.lastOffset);
        headerBuffer.putInt(topicLen);
        headerBuffer.put(topicBytes);
        headerBuffer.flip();

        byte[] header = new byte[20 + 4 + topicLen];
        headerBuffer.get(header);

        // Send header using BATCH_HEADER message type
        BrokerMessage headerMsg = new BrokerMessage(
            BrokerMessage.MessageType.BATCH_HEADER,
            System.currentTimeMillis(),
            header
        );

        log.debug("Sending BATCH_HEADER to consumer {}: topic={}, recordCount={}, totalBytes={}, lastOffset={}",
                 consumer.clientId, consumer.topic, batchRegion.recordCount, batchRegion.totalBytes, batchRegion.lastOffset);

        server.send(consumer.clientId, headerMsg).get(1, TimeUnit.SECONDS);

        // Step 2: Send FileRegion for true zero-copy transfer (Kafka-style sendfile)
        // This uses OS sendfile() syscall - data goes from file → kernel → socket
        // NO user-space copies, NO heap allocations!
        log.info("Sending zero-copy FileRegion of {} messages ({} bytes) to consumer {} for topic {} using sendfile()",
                 batchRegion.recordCount, batchRegion.totalBytes, consumer.clientId, consumer.topic);

        server.sendFileRegion(consumer.clientId, batchRegion.fileRegion)
              .get(5, TimeUnit.SECONDS);

        log.info("Sent zero-copy batch to consumer {}: recordCount={}, bytes={}, startOffset={}, lastOffset={}",
                 consumer.clientId, batchRegion.recordCount, batchRegion.totalBytes, startOffset, batchRegion.lastOffset);
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

        log.debug("Received BATCH_ACK from clientId={}, topic={}", clientId, topic);

        // Get pending offset for this delivery
        Long offsetToCommit = pendingOffsets.remove(pendingKey);

        if (offsetToCommit != null) {
            // ✅ NOW update offset in properties file (source of truth)
            RemoteConsumer consumer = consumers.get(deliveryKey);
            if (consumer != null) {
                String consumerId = consumer.group + ":" + topic;
                offsetTracker.updateOffset(consumerId, offsetToCommit);
                consumer.setCurrentOffset(offsetToCommit);

                log.info("ACK received from clientId={}, topic={}, committed offset {} to properties file",
                        clientId, topic, offsetToCommit);
            } else {
                log.warn("Received ACK for unknown consumer: deliveryKey={}", deliveryKey);
            }
        } else {
            log.warn("Received ACK but no pending offset found for deliveryKey={}", deliveryKey);
        }

        // Clear in-flight status to allow next delivery
        AtomicBoolean inFlight = inFlightDeliveries.get(deliveryKey);
        if (inFlight != null) {
            inFlight.set(false);
            log.debug("Cleared in-flight status for deliveryKey={}", deliveryKey);
        }

        // Remove from consumerCurrentTopic to allow other topics to deliver
        consumerCurrentTopic.remove(clientId, topic);
        log.debug("Removed consumer {} from current topic tracking", clientId);
    }

    /**
     * Notify consumers about new message (PUSH model)
     * This immediately triggers delivery instead of waiting for scheduler
     */
    public void notifyNewMessage(String topic, long offset) {
        log.debug("New message notification: topic={}, offset={}", topic, offset);

        long now = System.currentTimeMillis();
        // Find all consumers subscribed to this topic and trigger immediate delivery
        for (RemoteConsumer consumer : consumers.values()) {
            if (consumer.topic.equals(topic)) {
                // Rate limiting: Only submit delivery task if last attempt was > 200ms ago (increased from 100ms)
                if (now - consumer.lastDeliveryAttempt >= 200) {
                    consumer.lastDeliveryAttempt = now;
                    scheduler.submit(() -> deliverMessages(consumer,offset));
                } else {
                    log.debug("Skipping delivery for consumer {} - rate limited", consumer.clientId);
                }
            }
        }
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
     * Remote consumer metadata
     */
    private static class RemoteConsumer {
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
