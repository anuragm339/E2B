package com.messaging.broker.consumer;

import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.consumer.PendingAckStore;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of PendingAckStore.
 *
 * Thread-safe concurrent storage for legacy batch ACK tracking.
 */
@Singleton
public class InMemoryPendingAckStore implements PendingAckStore {

    // Map: clientId -> MergedBatch awaiting ACK
    private final ConcurrentHashMap<String, MergedBatch> pendingLegacyBatches = new ConcurrentHashMap<>();

    // Map: clientId -> Timer.Sample for delivery latency tracking
    private final ConcurrentHashMap<String, Timer.Sample> pendingLegacyTimers = new ConcurrentHashMap<>();

    // Map: clientId -> wall-clock send time (ms) for ACK latency calculation
    private final ConcurrentHashMap<String, Long> pendingLegacySendTimes = new ConcurrentHashMap<>();

    @Override
    public void putPendingBatch(String clientId, MergedBatch batch) {
        pendingLegacyBatches.put(clientId, batch);
    }

    @Override
    public boolean putPendingBatchIfAbsent(String clientId, MergedBatch batch) {
        return pendingLegacyBatches.putIfAbsent(clientId, batch) == null;
    }

    @Override
    public MergedBatch getPendingBatch(String clientId) {
        return pendingLegacyBatches.get(clientId);
    }

    @Override
    public MergedBatch removePendingBatch(String clientId) {
        return pendingLegacyBatches.remove(clientId);
    }

    @Override
    public void startTimer(String clientId, Timer.Sample timerSample) {
        pendingLegacyTimers.put(clientId, timerSample);
    }

    @Override
    public Timer.Sample removeTimer(String clientId) {
        return pendingLegacyTimers.remove(clientId);
    }

    @Override
    public void recordSendTime(String clientId, long sendTimeMs) {
        pendingLegacySendTimes.put(clientId, sendTimeMs);
    }

    @Override
    public long getSendTime(String clientId) {
        Long time = pendingLegacySendTimes.get(clientId);
        return time != null ? time : -1L;
    }

    @Override
    public void removeClient(String clientId) {
        pendingLegacyBatches.remove(clientId);
        pendingLegacyTimers.remove(clientId);
        pendingLegacySendTimes.remove(clientId);
    }

    @Override
    public void clear() {
        pendingLegacyBatches.clear();
        pendingLegacyTimers.clear();
        pendingLegacySendTimes.clear();
    }
}
