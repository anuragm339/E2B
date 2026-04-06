package com.messaging.broker.consumer;

import com.messaging.broker.consumer.RemoteConsumer;

/**
 * Service for consumer batch delivery orchestration.
 *
 * Handles batch delivery workflow with flow control, metrics, and error handling.
 */
public interface ConsumerDeliveryService {

    /**
     * Delivery result indicating whether batch was delivered.
     */
    record DeliveryResult(boolean delivered, String reason) {
        public static DeliveryResult success() {
            return new DeliveryResult(true, null);
        }

        public static DeliveryResult blocked(String reason) {
            return new DeliveryResult(false, reason);
        }

        public static DeliveryResult failure(String reason) {
            return new DeliveryResult(false, reason);
        }
    }

    /**
     * Attempt to deliver batch to consumer.
     *
     * Flow control gates:
     * 1. In-flight check (only one delivery per consumer:topic at a time)
     * 2. Pending ACK check (wait for previous batch ACK)
     * 3. Max consecutive failures check (unregister on threshold)
     * 4. Exponential backoff check (delay retry after failures)
     *
     * @param consumer Consumer to deliver to
     * @param batchSizeBytes Max batch size in bytes
     * @return Delivery result (success or blocked/failed with reason)
     */
    DeliveryResult deliverBatch(RemoteConsumer consumer, long batchSizeBytes);

    /**
     * Check if consumer can receive delivery.
     *
     * Performs all flow control gate checks without attempting delivery.
     *
     * @param consumer Consumer to check
     * @return true if delivery is allowed, false if blocked
     */
    boolean canDeliver(RemoteConsumer consumer);

    /**
     * Reset delivery state for a consumer.
     *
     * Clears in-flight status, pending offsets, and failure counters.
     *
     * @param consumer Consumer to reset
     */
    void resetDeliveryState(RemoteConsumer consumer);

    /**
     * Handle delivery success (called after ACK received).
     *
     * Clears pending state, updates metrics, resets failure counters.
     *
     * @param consumer Consumer that ACKed
     */
    void handleDeliverySuccess(RemoteConsumer consumer);

    /**
     * Handle delivery failure.
     *
     * Records failure, increments counter, starts exponential backoff.
     *
     * @param consumer Consumer that failed
     * @param error Error that caused failure
     */
    void handleDeliveryFailure(RemoteConsumer consumer, Throwable error);
}
