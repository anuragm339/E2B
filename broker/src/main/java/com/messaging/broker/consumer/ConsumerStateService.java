package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for consumer flow control state management.
 *
 * Manages in-flight delivery tracking, pending offsets, and ACK timeouts.
 */
public interface ConsumerStateService {

    /**
     * Mark delivery as in-flight and return status tracker.
     *
     * @param key Delivery key
     * @return AtomicBoolean for tracking delivery status
     */
    AtomicBoolean markInFlight(DeliveryKey key);

    /**
     * Check if delivery is currently in-flight.
     *
     * @param key Delivery key
     * @return True if delivery is in progress
     */
    boolean isInFlight(DeliveryKey key);

    /**
     * Clear in-flight status.
     *
     * @param key Delivery key
     */
    void clearInFlight(DeliveryKey key);

    /**
     * Set pending offset to be committed on ACK.
     *
     * @param key Delivery key
     * @param offset Pending offset
     */
    void setPendingOffset(DeliveryKey key, long offset);

    /**
     * Get pending offset.
     *
     * @param key Delivery key
     * @return Pending offset, or null if not set
     */
    Long getPendingOffset(DeliveryKey key);

    /**
     * Clear pending offset.
     *
     * @param key Delivery key
     */
    void clearPendingOffset(DeliveryKey key);

    /**
     * Record timestamp when batch was sent.
     *
     * @param key Delivery key
     * @param timestamp Timestamp in milliseconds
     */
    void recordBatchSendTime(DeliveryKey key, long timestamp);

    /**
     * Get batch send timestamp.
     *
     * @param key Delivery key
     * @return Timestamp, or null if not set
     */
    Long getBatchSendTime(DeliveryKey key);

    /**
     * Record trace id for the active delivery flow.
     *
     * @param key Delivery key
     * @param traceId Correlation id for send/ack/timeout logs
     */
    void recordTraceId(DeliveryKey key, String traceId);

    /**
     * Get trace id for the active delivery flow.
     *
     * @param key Delivery key
     * @return Trace id, or null if none is recorded
     */
    String getTraceId(DeliveryKey key);

    /**
     * Clear trace id for the active delivery flow.
     *
     * @param key Delivery key
     */
    void clearTraceId(DeliveryKey key);

    /**
     * Schedule ACK timeout task.
     *
     * @param key Delivery key
     * @param task Scheduled timeout task
     */
    void scheduleTimeout(DeliveryKey key, ScheduledFuture<?> task);

    /**
     * Cancel ACK timeout task.
     *
     * @param key Delivery key
     */
    void cancelTimeout(DeliveryKey key);

    /**
     * Remove all state for a delivery.
     *
     * @param key Delivery key
     */
    void removeDeliveryState(DeliveryKey key);

    /**
     * Persist the start offset of the batch that was sent, so it can be
     * re-read from storage on ACK for per-msgKey RocksDB writes.
     *
     * @param key        Delivery key
     * @param fromOffset First offset of the batch (inclusive)
     */
    void setFromOffset(DeliveryKey key, long fromOffset);

    /**
     * Retrieve the stored fromOffset for the active batch.
     *
     * @param key Delivery key
     * @return fromOffset, or null if not set
     */
    Long getFromOffset(DeliveryKey key);

    /**
     * Clear the fromOffset after it has been consumed (ACK, timeout, or send failure).
     *
     * @param key Delivery key
     */
    void clearFromOffset(DeliveryKey key);

}
