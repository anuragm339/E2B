package com.messaging.broker.consumer;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a remote consumer connected via TCP.
 *
 * Tracks consumer state, offset, delivery status, and failure backoff.
 */
public class RemoteConsumer {
    private final String clientId;
    private final String topic;
    private final String group;
    private final boolean isLegacy; // True if this consumer uses legacy Event protocol

    public volatile long currentOffset;
    public volatile Future<?> deliveryTask;
    public volatile long lastDeliveryAttempt; // Rate limiting: timestamp of last delivery attempt
    // AtomicInteger prevents lost-update races when delivery scheduler and ACK-timeout threads
    // both call recordFailure() concurrently (volatile int + ++ is not an atomic operation).
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    public volatile long lastFailureTime; // Timestamp of last failure

    public RemoteConsumer(String clientId, String topic, String group) {
        this(clientId, topic, group, false);
    }

    public RemoteConsumer(String clientId, String topic, String group, boolean isLegacy) {
        this.clientId = clientId;
        this.topic = topic;
        this.group = group;
        this.isLegacy = isLegacy;
        this.currentOffset = 0;
        this.lastDeliveryAttempt = 0; // Allow immediate first delivery
        this.lastFailureTime = 0;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isLegacy() {
        return isLegacy;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public long getLastFailureTime() {
        return lastFailureTime;
    }

    public Future<?> getDeliveryTask() {
        return deliveryTask;
    }

    public void setDeliveryTask(Future<?> task) {
        this.deliveryTask = task;
    }

    /**
     * Calculate backoff delay based on consecutive failures (exponential backoff).
     *
     * @return delay in milliseconds before next retry
     */
    public long getBackoffDelay() {
        int failures = consecutiveFailures.get();
        if (failures == 0) {
            return 0; // No delay on first attempt
        }
        // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, max 5000ms
        return Math.min(100L * (1L << (failures - 1)), 5000L);
    }

    public void recordFailure() {
        consecutiveFailures.incrementAndGet(); // atomic: prevents lost-update under concurrent calls
        lastFailureTime = System.currentTimeMillis();
    }

    public void resetFailures() {
        consecutiveFailures.set(0);
        lastFailureTime = 0;
    }
}
