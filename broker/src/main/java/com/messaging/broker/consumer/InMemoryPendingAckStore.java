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

    @Override
    public void putPendingBatch(String clientId, MergedBatch batch) {
        pendingLegacyBatches.put(clientId, batch);
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
    public void removeClient(String clientId) {
        pendingLegacyBatches.remove(clientId);
        pendingLegacyTimers.remove(clientId);
    }

    @Override
    public void clear() {
        pendingLegacyBatches.clear();
        pendingLegacyTimers.clear();
    }
}
