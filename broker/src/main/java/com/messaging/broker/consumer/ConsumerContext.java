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

    @Override
    public String toString() {
        return String.format("ConsumerContext{id=%s, topic=%s, group=%s, offset=%d, failures=%d, paused=%b}",
                consumerId, topic, group, currentOffset, consecutiveFailures, paused);
    }
}
