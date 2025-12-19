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
                        if (storage instanceof MMapStorageEngine) {
                            return ((MMapStorageEngine) storage).getZeroCopyBatch(consumer.topic, 0, offsetToRead,
                                    readBatchSize, maxMessageSizePerConsumer);
                        } else if (storage instanceof com.messaging.storage.filechannel.FileChannelStorageEngine) {
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
            } catch (Exception e) {
                log.error("Failed to send batch to consumer {}: recordCount={}, firstOffset={}",
                         consumer.clientId, batchRegion.recordCount, currentOffset, e);
                // Record failure
                metrics.recordConsumerFailure(consumer.clientId, consumer.topic, consumer.group);
                return; // Stop on error
            }

            // Update offset in memory
            consumer.setCurrentOffset(currentOffset);

            // CRITICAL: Persist offset to property file after successful delivery
            // Property file is the ONLY source of truth for consumer offsets
            String consumerId = consumer.group + ":" + consumer.topic;
            offsetTracker.updateOffset(consumerId, currentOffset);
            log.debug("Persisted offset to property file after successful delivery: consumerId={}, offset={}",
                     consumerId, currentOffset);

            // Update consumer offset metric
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
        // Header: [recordCount:4][totalBytes:8][lastOffset:8]
        ByteBuffer headerBuffer = ByteBuffer.allocate(20);
        headerBuffer.putInt(batchRegion.recordCount);
        headerBuffer.putLong(batchRegion.totalBytes);
        headerBuffer.putLong(batchRegion.lastOffset);
        headerBuffer.flip();

        byte[] header = new byte[20];
        headerBuffer.get(header);

        // Send header using BATCH_HEADER message type
        BrokerMessage headerMsg = new BrokerMessage(
            BrokerMessage.MessageType.BATCH_HEADER,
            System.currentTimeMillis(),
            header
        );

        log.debug("Sending BATCH_HEADER to consumer {}: recordCount={}, totalBytes={}, lastOffset={}",
                 consumer.clientId, batchRegion.recordCount, batchRegion.totalBytes, batchRegion.lastOffset);

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
