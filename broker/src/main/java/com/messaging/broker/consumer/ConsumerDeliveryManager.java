package com.messaging.broker.consumer;

import com.messaging.common.annotation.RetryPolicy;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.MessageRecord;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Manages message delivery to local consumers
 */
@Singleton
public class ConsumerDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeliveryManager.class);
    private static final int BATCH_SIZE = 100;
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
        this.scheduler = Executors.newScheduledThreadPool(10);
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
        context.setCurrentOffset(persistedOffset);

        log.info("Starting delivery for consumer: {} from offset: {}", context, persistedOffset);

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
            List<MessageRecord> records = storage.read(context.getTopic(), 0, currentOffset, BATCH_SIZE);

            if (records.isEmpty()) {
                return;
            }

            for (MessageRecord record : records) {
                boolean success = deliverSingleMessage(context, record);
                if (!success) {
                    handleDeliveryFailure(context, record);
                    break;
                }

                long newOffset = record.getOffset() + 1L;
                context.setCurrentOffset(newOffset);
                context.resetFailures();
                offsetTracker.updateOffset(context.getConsumerId(), newOffset);
            }
        } catch (Exception e) {
            log.error("Error in delivery loop for consumer: {}", context.getConsumerId(), e);
        }
    }

    private boolean deliverSingleMessage(ConsumerContext context, MessageRecord record) {
        try {
            ConsumerRecord consumerRecord = new ConsumerRecord(
                record.getMsgKey(),
                record.getEventType(),
                record.getData(),
                record.getCreatedAt()
            );
            context.getHandler().handle(consumerRecord);

            log.debug("Delivered message to consumer {}: offset={}, key={}",
                     context.getConsumerId(), record.getOffset(), record.getMsgKey());
            return true;
        } catch (Exception e) {
            log.error("Failed to deliver message to consumer {}: offset={}, key={}",
                     context.getConsumerId(), record.getOffset(), record.getMsgKey(), e);

            try {
                ConsumerRecord errorRecord = new ConsumerRecord(
                    record.getMsgKey(),
                    record.getEventType(),
                    record.getData(),
                    record.getCreatedAt()
                );
                context.getErrorHandler().onError(errorRecord, e);
            } catch (Exception errorHandlerEx) {
                log.error("Error handler threw exception", errorHandlerEx);
            }
            return false;
        }
    }

    private void handleDeliveryFailure(ConsumerContext context, MessageRecord record) {
        context.incrementFailures();
        RetryPolicy policy = context.getRetryPolicy();
        int failures = context.getConsecutiveFailures();

        switch (policy) {
            case EXPONENTIAL_THEN_FIXED:
                long retryDelay = context.calculateRetryDelay();
                log.warn("Consumer {} failed (attempt {}), will retry in {}ms: offset={}, key={}",
                        context.getConsumerId(), failures, retryDelay, record.getOffset(), record.getMsgKey());
                break;

            case SKIP_ON_ERROR:
                log.warn("Consumer {} failed, SKIPPING message: offset={}, key={}",
                        context.getConsumerId(), record.getOffset(), record.getMsgKey());
                long newOffset = record.getOffset() + 1L;
                context.setCurrentOffset(newOffset);
                context.resetFailures();
                offsetTracker.updateOffset(context.getConsumerId(), newOffset);
                break;

            case PAUSE_ON_ERROR:
                log.error("Consumer {} failed, PAUSING consumer: offset={}, key={}",
                        context.getConsumerId(), record.getOffset(), record.getMsgKey());
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
