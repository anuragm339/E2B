package com.messaging.broker.consumer;

import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final long maxMessageSizePerConsumer;
    private final int readBatchSize;

    // Map: clientId -> RemoteConsumer
    private final Map<String, RemoteConsumer> consumers;

    @Inject
    public RemoteConsumerRegistry(StorageEngine storage, NetworkServer server,
                                  ConsumerOffsetTracker offsetTracker, BrokerMetrics metrics,
                                  @Value("${broker.consumer.max-message-size-per-consumer:1048576}") long maxMessageSizePerConsumer,
                                  @Value("${broker.consumer.max-batch-size-per-consumer:100}") int readBatchSize) {
        this.storage = storage;
        this.server = server;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.maxMessageSizePerConsumer = maxMessageSizePerConsumer;
        this.readBatchSize = readBatchSize;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules(); // Register JSR310 module for Java 8 date/time
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),runnable -> {
            Thread t = new Thread(runnable);
            t.setName("RemoteConsumerRegistry" + t.getId());
            return t;
        } );
        this.consumers = new ConcurrentHashMap<>();
        log.info("RemoteConsumerRegistry initialized with maxMessageSize={}bytes per consumer, readBatchSize={}",
                 maxMessageSizePerConsumer, readBatchSize);
    }

    /**
     * Register a remote consumer when it subscribes
     */
    public void registerConsumer(String clientId, String topic, String group) {
        RemoteConsumer consumer = new RemoteConsumer(clientId, topic, group);

        // Load persisted offset
        String consumerId = group + ":" + topic;
        long persistedOffset = offsetTracker.getOffset(consumerId);

        // Get earliest available offset from storage
        // If persisted offset is 0 (new consumer), use the earliest available offset in the topic
        // This handles cases where the topic has been compacted or messages have been deleted
        long earliestOffset = storage.getEarliestOffset(topic, 0);
        long startOffset = persistedOffset;

        if (persistedOffset < earliestOffset) {
            startOffset = earliestOffset;
            log.info("Adjusting consumer offset from {} to earliest available offset {} for topic={}",
                     persistedOffset, earliestOffset, topic);
        }

        consumer.setCurrentOffset(startOffset);

        // Initialize consumer metrics
        metrics.updateConsumerOffset(clientId, topic, group, startOffset);
        metrics.updateConsumerLag(clientId, topic, group, 0);

        consumers.put(clientId, consumer);

        // Start delivery task
        startDelivery(consumer);

        log.debug("Registered remote consumer: clientId={}, topic={}, group={}, startOffset={}",
                 clientId, topic, group, startOffset);
    }

    /**
     * Unregister consumer when it disconnects
     */
    public void unregisterConsumer(String clientId) {
        RemoteConsumer consumer = consumers.remove(clientId);
        if (consumer != null) {
            if (consumer.deliveryTask != null) {
                consumer.deliveryTask.cancel(false);
            }
            // Clean up metrics
            metrics.removeConsumerMetrics(clientId, consumer.topic);
            log.info("Unregistered remote consumer: {}", clientId);
        }
    }

    /**
     * Start message delivery for a consumer
     */
    private void startDelivery(RemoteConsumer consumer) {
        Future<?> task = scheduler.scheduleWithFixedDelay(
                () -> deliverMessages(consumer),
                500, // Wait 500ms before first delivery to allow ACK to complete
                POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        consumer.deliveryTask = task;
    }

    /**
     * Deliver messages to a remote consumer
     */
    private void deliverMessages(RemoteConsumer consumer) {
        try {
            long currentOffset = consumer.getCurrentOffset();

            log.info("Attempting to deliver messages to consumer {}: and topic {}: offset={}",
                     consumer.clientId,consumer.topic, currentOffset);

            // Read next batch of messages from storage
            List<MessageRecord> records;
            try {
                log.debug("About to call storage.read() for consumer {}: topic={}, partition=0, offset={}",
                         consumer.clientId, consumer.topic, currentOffset);

                // Use a CompletableFuture with timeout to prevent indefinite blocking
                final long offsetToRead = currentOffset; // Must be final for lambda
                Timer.Sample readSample = metrics.startStorageReadTimer();

                records = CompletableFuture.supplyAsync(() -> {
                    try {
                        return storage.read(consumer.topic, 0, offsetToRead, readBatchSize);
                    } catch (Exception e) {
                        log.error("Exception in storage.read(): ", e);
                        throw new RuntimeException(e);
                    }
                }, scheduler).get(10, TimeUnit.MINUTES);

                metrics.stopStorageReadTimer(readSample);
                metrics.recordStorageRead();

                log.debug("storage.read() returned successfully with {} records",
                         records != null ? records.size() : "null");
            } catch (TimeoutException e) {
                log.error("TIMEOUT: storage.read() did not complete within 5 seconds for consumer {}: topic={}, offset={}",
                         consumer.clientId, consumer.topic, currentOffset);
                return;
            } catch (Exception e) {
                log.error("Failed to read from storage for consumer {}: topic={}, offset={}",
                         consumer.clientId, consumer.topic, currentOffset, e);
                return;
            }

//            log.info("Read {} messages from storage for consumer {}",
//                     records.size(), consumer.clientId);

            if (records.isEmpty()) {
                return; // No new messages
            }

            metrics.recordBatchSize(records.size());

            // Send messages to consumer as batch with size limit enforcement
            long totalBytesSent = 0;
            int messagesSent = 0;
            List<MessageRecord> batchToSend = new ArrayList<>();

            for (MessageRecord record : records) {
                // Calculate message size before adding to batch
                long messageBytes = (record.getMsgKey() != null ? record.getMsgKey().length() : 0) +
                                    (record.getData() != null ? record.getData().length() : 0) +
                                    50; // metadata overhead

                // Check if adding this message would exceed size limit
                if (totalBytesSent + messageBytes > maxMessageSizePerConsumer && !batchToSend.isEmpty()) {
                    log.debug("Consumer {} would exceed max size limit ({}bytes) with next message ({}bytes total), sending current batch of {} messages",
                             consumer.clientId, maxMessageSizePerConsumer, totalBytesSent + messageBytes, batchToSend.size());
                    break;
                }

                batchToSend.add(record);
                totalBytesSent += messageBytes;
                messagesSent++;
            }

            // Send the batch to consumer if not empty
            if (!batchToSend.isEmpty()) {
                try {
                    // Start timing for per-consumer delivery
                    Timer.Sample deliverySample = metrics.startConsumerDeliveryTimer();
                    log.info("Sending batch of {} messages ({} bytes) to consumer {} for topics {} starting at offset {}",
                             batchToSend.size(), totalBytesSent, consumer.clientId,consumer.topic, currentOffset);
                    sendBatchToConsumer(consumer, batchToSend, currentOffset);

                    // Record per-consumer metrics
                    metrics.recordConsumerMessageSent(consumer.clientId, consumer.topic, consumer.group, totalBytesSent);
                    metrics.stopConsumerDeliveryTimer(deliverySample, consumer.clientId, consumer.topic, consumer.group);

                    // Record global metrics
                    metrics.recordMessageSent(totalBytesSent);

                    // Update offset to after the last message sent
                    currentOffset = batchToSend.get(batchToSend.size() - 1).getOffset()+1;
                } catch (Exception e) {
                    log.error("Failed to send batch to consumer {}: batchSize={}, firstOffset={}",
                             consumer.clientId, batchToSend.size(), currentOffset, e);
                    // Record failure
                    metrics.recordConsumerFailure(consumer.clientId, consumer.topic, consumer.group);
                    return; // Stop on error
                }
            }

            if (messagesSent > 0) {
                log.debug("Sent {} messages ({} bytes) to consumer {}",
                         messagesSent, totalBytesSent, consumer.clientId);
            }

            // Update offset
            consumer.setCurrentOffset(currentOffset);

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

            // Persist offset periodically (every 100 messages or every 5 seconds)
            if (currentOffset % 100 == 0 ||
                System.currentTimeMillis() - consumer.lastOffsetPersist > 5000) {
                String consumerId = consumer.group + ":" + consumer.topic;
                offsetTracker.updateOffset(consumerId, currentOffset);
                consumer.lastOffsetPersist = System.currentTimeMillis();
            }

        } catch (Exception e) {
            log.error("FATAL: Error delivering messages to consumer {}, task will continue",
                     consumer.clientId, e);
        }
    }

    /**
     * Send a batch of messages to consumer
     */
    private void sendBatchToConsumer(RemoteConsumer consumer, List<MessageRecord> records, long startOffset)
            throws Exception {

        // Create list of ConsumerRecords (offset is tracked separately by broker)
        List<ConsumerRecord> consumerRecords = new ArrayList<>(records.size());
        for (MessageRecord record : records) {
            consumerRecords.add(new ConsumerRecord(
                record.getMsgKey(),
                record.getEventType(),
                record.getData(),
                record.getCreatedAt()
            ));
        }

        // Serialize batch to JSON
        String json = objectMapper.writeValueAsString(consumerRecords);

        // Create DATA message
        BrokerMessage message = new BrokerMessage(
            BrokerMessage.MessageType.DATA,
            System.currentTimeMillis(),
            json.getBytes(StandardCharsets.UTF_8)
        );

        // Send batch to consumer
        server.send(consumer.clientId, message).get(5, TimeUnit.SECONDS);

        log.debug("Sent batch of {} messages to consumer {}: startOffset={}, endOffset={}",
                 records.size(), consumer.clientId, startOffset, startOffset + records.size() - 1);
    }

    /**
     * Notify consumers about new message (PUSH model)
     * This immediately triggers delivery instead of waiting for scheduler
     */
    public void notifyNewMessage(String topic, long offset) {
        log.debug("New message notification: topic={}, offset={}", topic, offset);

        // Find all consumers subscribed to this topic and trigger immediate delivery
        for (RemoteConsumer consumer : consumers.values()) {
            if (consumer.topic.equals(topic)) {
                // DO NOT update consumer offset here - let consumer maintain its own offset
                // based on what it has successfully delivered
                // Submit immediate delivery task (non-blocking)
                scheduler.submit(() -> deliverMessages(consumer));
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
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
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
        volatile long lastOffsetPersist;
        volatile Future<?> deliveryTask;

        RemoteConsumer(String clientId, String topic, String group) {
            this.clientId = clientId;
            this.topic = topic;
            this.group = group;
            this.currentOffset = 0;
            this.lastOffsetPersist = System.currentTimeMillis();
        }

        long getCurrentOffset() {
            return currentOffset;
        }

        void setCurrentOffset(long offset) {
            this.currentOffset = offset;
        }
    }
}
