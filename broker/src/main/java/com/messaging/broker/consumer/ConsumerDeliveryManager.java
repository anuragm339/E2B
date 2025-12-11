package com.messaging.broker.consumer;

import com.messaging.common.annotation.RetryPolicy;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.MessageRecord;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Manages message delivery to local consumers
 */
@Singleton
public class ConsumerDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeliveryManager.class);
    private static final long POLL_INTERVAL_MS = 100L;

    private final StorageEngine storage;
    private final ConsumerAnnotationProcessor processor;
    private final ConsumerOffsetTracker offsetTracker;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Future<?>> deliveryTasks;

    @Inject
    public ConsumerDeliveryManager(StorageEngine storage, ConsumerAnnotationProcessor processor,
                                  ConsumerOffsetTracker offsetTracker) {
        this.storage = storage;
        this.processor = processor;
        this.offsetTracker = offsetTracker;
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), r -> {
            Thread t = new Thread(r);
            t.setName("ConsumerDeliveryScheduler-" + t.getId());
            return t;
        });
        this.deliveryTasks = new ConcurrentHashMap<>();
        log.info("ConsumerDeliveryManager initialized");
    }

    public void startDelivery() {
        log.info("Starting message delivery for all consumers...");
        for (ConsumerContext context : processor.getAllConsumers()) {
            startConsumerDelivery(context);
        }
        log.info("Message delivery started for {} consumers", deliveryTasks.size());
    }

    public void startConsumerDelivery(ConsumerContext context) {
        String consumerId = context.getConsumerId();
        if (deliveryTasks.containsKey(consumerId)) {
            log.warn("Delivery already started for consumer: {}", consumerId);
            return;
        }

        long persistedOffset = offsetTracker.getOffset(consumerId);
        long earliestOffset = storage.getEarliestOffset(context.getTopic(), 0);
        long startOffset = Math.max(persistedOffset, earliestOffset);

        if (startOffset > persistedOffset) {
            log.info("Consumer {} start offset adjusted from {} to {} to align with earliest available offset",
                    consumerId, persistedOffset, startOffset);
        }

        context.setCurrentOffset(startOffset);

        log.info("Starting delivery for consumer: {} from offset: {}", context, startOffset);

        ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                () -> deliverMessages(context),
                0L,
                POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        deliveryTasks.put(consumerId, task);
    }

    public void stopConsumerDelivery(String consumerId) {
        Future<?> task = deliveryTasks.remove(consumerId);
        if (task != null) {
            task.cancel(false);
            log.info("Stopped delivery for consumer: {}", consumerId);
        }
    }

    private void deliverMessages(ConsumerContext context) {
        try {
            if (context.isPaused()) {
                return;
            }

            if (context.getConsecutiveFailures() > 0) {
                long retryDelay = context.calculateRetryDelay();
                long timeSinceFailure = System.currentTimeMillis() - context.getLastFailureTime();
                if (timeSinceFailure < retryDelay) {
                    return;
                }
            }

            long currentOffset = context.getCurrentOffset();

            // Use dynamic batch size from context
            int batchSize = context.getCurrentBatchSize();
            List<MessageRecord> records = storage.read(context.getTopic(), 0, currentOffset, batchSize);

            if (records.isEmpty()) {
                return;
            }

            // Deliver entire batch at once
            boolean success = deliverBatch(context, records);
            if (success) {
                // Update offset to last message in batch + 1
                long lastOffset = records.get(records.size() - 1).getOffset();
                long newOffset = lastOffset + 1L;
                context.setCurrentOffset(newOffset);
                context.resetFailures();
                offsetTracker.updateOffset(context.getConsumerId(), newOffset);

                // Update average message size for adaptive batch sizing
                int totalBytes = calculateBatchSize(records);
                context.updateAverageMessageSize(totalBytes, records.size());

                log.debug("Delivered batch of {} messages to consumer {}, new offset={}, next batch size={}",
                         records.size(), context.getConsumerId(), newOffset, context.getCurrentBatchSize());
            } else {
                // Handle batch failure
                handleBatchFailure(context, records);
            }
        } catch (Exception e) {
            log.error("Error in delivery loop for consumer: {}", context.getConsumerId(), e);
        }
    }

    /**
     * Deliver a batch of messages to the consumer
     */
    private boolean deliverBatch(ConsumerContext context, List<MessageRecord> records) {
        try {
            // Convert MessageRecords to ConsumerRecords
            List<ConsumerRecord> consumerRecords = new ArrayList<>(records.size());
            for (MessageRecord record : records) {
                consumerRecords.add(new ConsumerRecord(
                    record.getMsgKey(),
                    record.getEventType(),
                    record.getData(),
                    record.getCreatedAt()
                ));
            }

            // Deliver entire batch
            context.getHandler().handleBatch(consumerRecords);

            log.debug("Delivered batch of {} messages to consumer {}: offsets {}-{}",
                     records.size(), context.getConsumerId(),
                     records.get(0).getOffset(),
                     records.get(records.size() - 1).getOffset());
            return true;

        } catch (Exception e) {
            log.error("Failed to deliver batch to consumer {}: batch size={}, first offset={}",
                     context.getConsumerId(), records.size(),
                     records.isEmpty() ? "N/A" : records.get(0).getOffset(), e);

            // Call error handler with first record in failed batch
            try {
                if (!records.isEmpty()) {
                    MessageRecord firstRecord = records.get(0);
                    ConsumerRecord errorRecord = new ConsumerRecord(
                        firstRecord.getMsgKey(),
                        firstRecord.getEventType(),
                        firstRecord.getData(),
                        firstRecord.getCreatedAt()
                    );
                    context.getErrorHandler().onError(errorRecord, e);
                }
            } catch (Exception errorHandlerEx) {
                log.error("Error handler threw exception", errorHandlerEx);
            }
            return false;
        }
    }

    /**
     * Calculate total size of batch in bytes (approximate)
     */
    private int calculateBatchSize(List<MessageRecord> records) {
        int totalBytes = 0;
        for (MessageRecord record : records) {
            // Approximate: key + data + overhead
            totalBytes += record.getMsgKey().length();
            if (record.getData() != null) {
                totalBytes += record.getData().length();
            }
            totalBytes += 100; // Overhead for metadata, offset, etc.
        }
        return totalBytes;
    }

    /**
     * Handle batch delivery failure
     */
    private void handleBatchFailure(ConsumerContext context, List<MessageRecord> records) {
        context.incrementFailures();
        RetryPolicy policy = context.getRetryPolicy();
        int failures = context.getConsecutiveFailures();

        // Reduce batch size for retry
        context.reduceBatchSizeForRetry();

        MessageRecord firstRecord = records.get(0);

        switch (policy) {
            case EXPONENTIAL_THEN_FIXED:
                long retryDelay = context.calculateRetryDelay();
                log.info("Consumer {} failed (attempt {}), will retry in {}ms with reduced batch size {}: batch offsets {}-{}",
                        context.getConsumerId(), failures, retryDelay, context.getCurrentBatchSize(),
                        firstRecord.getOffset(), records.get(records.size() - 1).getOffset());
                break;

            case SKIP_ON_ERROR:
                log.info("Consumer {} failed, SKIPPING batch: offsets {}-{}",
                        context.getConsumerId(),
                        firstRecord.getOffset(), records.get(records.size() - 1).getOffset());
                long lastOffset = records.get(records.size() - 1).getOffset();
                long newOffset = lastOffset + 1L;
                context.setCurrentOffset(newOffset);
                context.resetFailures();
                offsetTracker.updateOffset(context.getConsumerId(), newOffset);
                break;

            case PAUSE_ON_ERROR:
                log.error("Consumer {} failed, PAUSING consumer: batch offsets {}-{}",
                        context.getConsumerId(),
                        firstRecord.getOffset(), records.get(records.size() - 1).getOffset());
                context.pause();
                break;

            default:
                log.error("Unknown retry policy: {}", policy);
        }
    }

    public void resumeConsumer(String consumerId) {
        ConsumerContext context = processor.getConsumer(consumerId);
        if (context != null && context.isPaused()) {
            context.resume();
            context.resetFailures();
            log.info("Resumed consumer: {}", consumerId);
        }
    }

    public void pauseConsumer(String consumerId) {
        ConsumerContext context = processor.getConsumer(consumerId);
        if (context != null) {
            context.pause();
            log.info("Paused consumer: {}", consumerId);
        }
    }

    public void resetConsumerOffset(String consumerId, long offset) {
        ConsumerContext context = processor.getConsumer(consumerId);
        if (context != null) {
            context.setCurrentOffset(offset);
            context.resetFailures();
            offsetTracker.updateOffset(consumerId, offset);
            log.info("Reset consumer {} to offset: {}", consumerId, offset);
        }
    }

    public ConsumerStats getConsumerStats(String consumerId) {
        ConsumerContext context = processor.getConsumer(consumerId);
        if (context == null) {
            return null;
        }
        return new ConsumerStats(
            consumerId,
            context.getTopic(),
            context.getGroup(),
            context.getCurrentOffset(),
            context.getConsecutiveFailures(),
            context.isPaused()
        );
    }

    public void shutdown() {
        log.info("Shutting down ConsumerDeliveryManager...");

        for (Future<?> task : deliveryTasks.values()) {
            task.cancel(false);
        }
        deliveryTasks.clear();

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5L, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("ConsumerDeliveryManager shutdown complete");
    }

    /**
     * Consumer statistics
     */
    public static class ConsumerStats {
        public final String consumerId;
        public final String topic;
        public final String group;
        public final long currentOffset;
        public final int consecutiveFailures;
        public final boolean paused;

        public ConsumerStats(String consumerId, String topic, String group,
                           long currentOffset, int consecutiveFailures, boolean paused) {
            this.consumerId = consumerId;
            this.topic = topic;
            this.group = group;
            this.currentOffset = currentOffset;
            this.consecutiveFailures = consecutiveFailures;
            this.paused = paused;
        }

        @Override
        public String toString() {
            return String.format("ConsumerStats{id=%s, topic=%s, group=%s, offset=%d, failures=%d, paused=%b}",
                    consumerId, topic, group, currentOffset, consecutiveFailures, paused);
        }
    }
}
