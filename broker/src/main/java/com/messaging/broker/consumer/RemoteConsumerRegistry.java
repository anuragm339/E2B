package com.messaging.broker.consumer;

import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.MessageRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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
    private static final int BATCH_SIZE = 10;

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;

    // Map: clientId -> RemoteConsumer
    private final Map<String, RemoteConsumer> consumers;

    @Inject
    public RemoteConsumerRegistry(StorageEngine storage, NetworkServer server,
                                  ConsumerOffsetTracker offsetTracker, BrokerMetrics metrics) {
        this.storage = storage;
        this.server = server;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules(); // Register JSR310 module for Java 8 date/time
        this.scheduler = Executors.newScheduledThreadPool(10);
        this.consumers = new ConcurrentHashMap<>();
        log.info("RemoteConsumerRegistry initialized");
    }

    /**
     * Register a remote consumer when it subscribes
     */
    public void registerConsumer(String clientId, String topic, String group) {
        RemoteConsumer consumer = new RemoteConsumer(clientId, topic, group);

        // Load persisted offset
        String consumerId = group + ":" + topic;
        long persistedOffset = offsetTracker.getOffset(consumerId);
        consumer.setCurrentOffset(persistedOffset);

        consumers.put(clientId, consumer);

        // Start delivery task
        startDelivery(consumer);

        log.info("Registered remote consumer: clientId={}, topic={}, group={}, startOffset={}",
                 clientId, topic, group, persistedOffset);
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

            log.info("Attempting to deliver messages to consumer {}: offset={}",
                     consumer.clientId, currentOffset);

            // Read next batch of messages from storage
            List<MessageRecord> records;
            try {
                log.info("About to call storage.read() for consumer {}: topic={}, partition=0, offset={}",
                         consumer.clientId, consumer.topic, currentOffset);

                // Use a CompletableFuture with timeout to prevent indefinite blocking
                final long offsetToRead = currentOffset; // Must be final for lambda
                Timer.Sample readSample = metrics.startStorageReadTimer();

                records = CompletableFuture.supplyAsync(() -> {
                    try {
                        return storage.read(consumer.topic, 0, offsetToRead, BATCH_SIZE);
                    } catch (Exception e) {
                        log.error("Exception in storage.read(): ", e);
                        throw new RuntimeException(e);
                    }
                }).get(5, TimeUnit.SECONDS);

                metrics.stopStorageReadTimer(readSample);
                metrics.recordStorageRead();

                log.info("storage.read() returned successfully with {} records",
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

            log.info("Read {} messages from storage for consumer {}",
                     records.size(), consumer.clientId);

            if (records.isEmpty()) {
                return; // No new messages
            }

            metrics.recordBatchSize(records.size());

            // Send each message to consumer
            for (MessageRecord record : records) {
                try {
                    Timer.Sample deliverySample = metrics.startMessageDeliveryTimer();
                    sendMessageToConsumer(consumer, record, currentOffset);
                    metrics.stopMessageDeliveryTimer(deliverySample);
                    metrics.recordMessageSent();
                    currentOffset++;
                } catch (Exception e) {
                    log.error("Failed to send message to consumer {}: offset={}",
                             consumer.clientId, currentOffset, e);
                    break; // Stop on error
                }
            }

            // Update offset
            consumer.setCurrentOffset(currentOffset);

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
     * Send a single message to consumer
     */
    private void sendMessageToConsumer(RemoteConsumer consumer, MessageRecord record, long offset)
            throws Exception {

        // Create ConsumerRecord (offset is tracked separately by broker)
        ConsumerRecord consumerRecord = new ConsumerRecord(
            record.getMsgKey(),
            record.getEventType(),
            record.getData(),
            record.getCreatedAt()
        );

        // Serialize to JSON
        String json = objectMapper.writeValueAsString(consumerRecord);

        // Create DATA message
        BrokerMessage message = new BrokerMessage(
            BrokerMessage.MessageType.DATA,
            System.currentTimeMillis(),
            json.getBytes(StandardCharsets.UTF_8)
        );

        // Send to consumer
        server.send(consumer.clientId, message).get(5, TimeUnit.SECONDS);

        log.debug("Sent message to consumer {}: offset={}, key={}",
                 consumer.clientId, offset, record.getMsgKey());
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
