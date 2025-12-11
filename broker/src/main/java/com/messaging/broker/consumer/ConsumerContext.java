package com.messaging.broker.consumer;

import com.messaging.common.annotation.Consumer;
import com.messaging.common.annotation.RetryPolicy;
import com.messaging.common.api.ErrorHandler;
import com.messaging.common.api.MessageHandler;

/**
 * Consumer metadata and state tracking
 */
public class ConsumerContext {
    private final String consumerId;
    private final String topic;
    private final String group;
    private final MessageHandler handler;
    private final ErrorHandler errorHandler;
    private final RetryPolicy retryPolicy;
    private final int maxExponentialRetries;
    private final long initialRetryDelayMs;
    private final long maxRetryDelayMs;
    private final long fixedRetryIntervalMs;

    private volatile long currentOffset;
    private volatile int consecutiveFailures;
    private volatile long lastFailureTime;
    private volatile boolean paused;

    // Batch size tracking for dynamic batch sizing (target: 1MB per batch)
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final long TARGET_BATCH_BYTES = 1024 * 1024; // 1MB
    private volatile int averageMessageSizeBytes = 1024; // Start with 1KB estimate
    private volatile int currentBatchSize = DEFAULT_BATCH_SIZE;

    public ConsumerContext(String consumerId, Consumer annotation, MessageHandler handler, ErrorHandler errorHandler) {
        this.consumerId = consumerId;
        this.topic = annotation.topic();
        this.group = annotation.group();
        this.handler = handler;
        this.errorHandler = errorHandler;
        this.retryPolicy = annotation.retryPolicy();
        this.maxExponentialRetries = annotation.maxExponentialRetries();
        this.initialRetryDelayMs = annotation.initialRetryDelayMs();
        this.maxRetryDelayMs = annotation.maxRetryDelayMs();
        this.fixedRetryIntervalMs = annotation.fixedRetryIntervalMs();
        this.currentOffset = 0L;
        this.consecutiveFailures = 0;
        this.lastFailureTime = 0L;
        this.paused = false;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroup() {
        return group;
    }

    public MessageHandler getHandler() {
        return handler;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public int getMaxExponentialRetries() {
        return maxExponentialRetries;
    }

    public long getInitialRetryDelayMs() {
        return initialRetryDelayMs;
    }

    public long getMaxRetryDelayMs() {
        return maxRetryDelayMs;
    }

    public long getFixedRetryIntervalMs() {
        return fixedRetryIntervalMs;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public void incrementFailures() {
        ++consecutiveFailures;
        lastFailureTime = System.currentTimeMillis();
    }

    public void resetFailures() {
        consecutiveFailures = 0;
        lastFailureTime = 0L;
    }

    public long getLastFailureTime() {
        return lastFailureTime;
    }

    public boolean isPaused() {
        return paused;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    public long calculateRetryDelay() {
        switch (retryPolicy) {
            case EXPONENTIAL_THEN_FIXED:
                if (consecutiveFailures <= maxExponentialRetries) {
                    long delay = initialRetryDelayMs * (long) Math.pow(2.0, consecutiveFailures - 1);
                    return Math.min(delay, maxRetryDelayMs);
                }
                return fixedRetryIntervalMs;

            case SKIP_ON_ERROR:
                return 0L;

            case PAUSE_ON_ERROR:
                return Long.MAX_VALUE;
        }
        return fixedRetryIntervalMs;
    }

    public int getCurrentBatchSize() {
        return currentBatchSize;
    }

    public void updateAverageMessageSize(int totalBytesInBatch, int recordCount) {
        if (recordCount == 0) {
            return;
        }

        int batchAverage = totalBytesInBatch / recordCount;

        // Rolling average: 80% old, 20% new (smooth out spikes)
        this.averageMessageSizeBytes = (int) (averageMessageSizeBytes * 0.8 + batchAverage * 0.2);

        // Recalculate batch size: target 1MB per batch
        this.currentBatchSize = Math.max(1, (int) (TARGET_BATCH_BYTES / averageMessageSizeBytes));
    }

    public void reduceBatchSizeForRetry() {
        // Halve the batch size for retry, minimum 1
        this.currentBatchSize = Math.max(1, currentBatchSize / 2);
    }

    @Override
    public String toString() {
        return String.format("ConsumerContext{id=%s, topic=%s, group=%s, offset=%d, failures=%d, paused=%b}",
                consumerId, topic, group, currentOffset, consecutiveFailures, paused);
    }
}
