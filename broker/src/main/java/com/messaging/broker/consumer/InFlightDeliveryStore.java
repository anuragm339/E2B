package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Store for in-flight delivery tracking.
 *
 * Manages delivery state, pending offsets, timeouts, and ACK tracking.
 */
public interface InFlightDeliveryStore {

    /**
     * Mark delivery as in-flight.
     *
     * @param key Delivery key
     * @return AtomicBoolean for tracking delivery status
     */
    AtomicBoolean markInFlight(DeliveryKey key);

    /**
     * Check if delivery is in-flight.
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
     * Set pending offset (to be committed on ACK).
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
     * Record batch send timestamp.
     *
     * @param key Delivery key
     * @param timestamp Timestamp when batch was sent
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
     * Clear batch send timestamp.
     *
     * @param key Delivery key
     */
    void clearBatchSendTime(DeliveryKey key);

    /**
     * Schedule ACK timeout task.
     *
     * @param key Delivery key
     * @param task Scheduled timeout task
     */
    void scheduleTimeout(DeliveryKey key, ScheduledFuture<?> task);

    /**
     * Cancel and remove timeout task.
     *
     * @param key Delivery key
     */
    void cancelTimeout(DeliveryKey key);

    /**
     * Remove all state for a delivery key.
     *
     * @param key Delivery key
     */
    void removeAll(DeliveryKey key);

}
