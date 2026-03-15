package com.messaging.broker.consumer;

import com.messaging.broker.legacy.MergedBatch;
import io.micrometer.core.instrument.Timer;

/**
 * Store for pending ACK tracking (legacy batches).
 *
 * Manages legacy batch metadata and timing for ACK latency metrics.
 */
public interface PendingAckStore {

    /**
     * Store pending legacy batch.
     *
     * @param clientId Client connection identifier
     * @param batch Merged batch awaiting ACK
     */
    void putPendingBatch(String clientId, MergedBatch batch);

    /**
     * Get pending legacy batch.
     *
     * @param clientId Client connection identifier
     * @return Merged batch, or null if not present
     */
    MergedBatch getPendingBatch(String clientId);

    /**
     * Remove pending legacy batch.
     *
     * @param clientId Client connection identifier
     * @return Removed batch, or null if not present
     */
    MergedBatch removePendingBatch(String clientId);

    /**
     * Start timer for legacy batch delivery.
     *
     * @param clientId Client connection identifier
     * @param timerSample Timer sample
     */
    void startTimer(String clientId, Timer.Sample timerSample);

    /**
     * Get and remove timer for legacy batch.
     *
     * @param clientId Client connection identifier
     * @return Timer sample, or null if not present
     */
    Timer.Sample removeTimer(String clientId);

    /**
     * Remove all state for a client.
     *
     * @param clientId Client connection identifier
     */
    void removeClient(String clientId);

    /**
     * Clear all pending ACK state.
     */
    void clear();
}
