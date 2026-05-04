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
     * Atomically store batch only if no batch is already pending for this client.
     * Used to replace the synchronized gate check without holding a lock during network I/O.
     *
     * @param clientId Client connection identifier
     * @param batch    Merged batch to store
     * @return true if the batch was stored (slot was free), false if a batch was already pending
     */
    boolean putPendingBatchIfAbsent(String clientId, MergedBatch batch);

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
     * Record the wall-clock time at which a batch was sent, for ACK latency calculation.
     *
     * @param clientId   Client connection identifier
     * @param sendTimeMs System.currentTimeMillis() at send time
     */
    void recordSendTime(String clientId, long sendTimeMs);

    /**
     * Get the wall-clock send time for a client's pending batch.
     *
     * @param clientId Client connection identifier
     * @return send time in ms, or -1 if not recorded
     */
    long getSendTime(String clientId);

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
