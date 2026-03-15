package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.InFlightDeliveryStore;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory implementation of InFlightDeliveryStore.
 *
 * Thread-safe concurrent storage for delivery tracking.
 */
@Singleton
public class InMemoryInFlightDeliveryStore implements InFlightDeliveryStore {

    // Map: DeliveryKey -> in-flight status
    private final ConcurrentHashMap<DeliveryKey, AtomicBoolean> inFlightDeliveries = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> pending offset to commit on ACK
    private final ConcurrentHashMap<DeliveryKey, Long> pendingOffsets = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> timestamp when batch was sent
    private final ConcurrentHashMap<DeliveryKey, Long> batchSendTimestamps = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> correlation id for the current send/ack/timeout flow
    private final ConcurrentHashMap<DeliveryKey, String> traceIds = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> timeout task
    private final ConcurrentHashMap<DeliveryKey, ScheduledFuture<?>> pendingTimeouts = new ConcurrentHashMap<>();

    @Override
    public AtomicBoolean markInFlight(DeliveryKey key) {
        return inFlightDeliveries.computeIfAbsent(key, k -> new AtomicBoolean(false));
    }

    @Override
    public boolean isInFlight(DeliveryKey key) {
        AtomicBoolean status = inFlightDeliveries.get(key);
        return status != null && status.get();
    }

    @Override
    public void clearInFlight(DeliveryKey key) {
        AtomicBoolean status = inFlightDeliveries.get(key);
        if (status != null) {
            status.set(false);
        }
    }

    @Override
    public void setPendingOffset(DeliveryKey key, long offset) {
        pendingOffsets.put(key, offset);
    }

    @Override
    public Long getPendingOffset(DeliveryKey key) {
        return pendingOffsets.get(key);
    }

    @Override
    public void clearPendingOffset(DeliveryKey key) {
        pendingOffsets.remove(key);
    }

    @Override
    public void recordBatchSendTime(DeliveryKey key, long timestamp) {
        batchSendTimestamps.put(key, timestamp);
    }

    @Override
    public Long getBatchSendTime(DeliveryKey key) {
        return batchSendTimestamps.get(key);
    }

    @Override
    public void recordTraceId(DeliveryKey key, String traceId) {
        traceIds.put(key, traceId);
    }

    @Override
    public String getTraceId(DeliveryKey key) {
        return traceIds.get(key);
    }

    @Override
    public void clearTraceId(DeliveryKey key) {
        traceIds.remove(key);
    }

    @Override
    public void clearBatchSendTime(DeliveryKey key) {
        batchSendTimestamps.remove(key);
    }

    @Override
    public void scheduleTimeout(DeliveryKey key, ScheduledFuture<?> task) {
        // Cancel existing timeout if present
        ScheduledFuture<?> existing = pendingTimeouts.put(key, task);
        if (existing != null && !existing.isDone()) {
            existing.cancel(false);
        }
    }

    @Override
    public void cancelTimeout(DeliveryKey key) {
        ScheduledFuture<?> task = pendingTimeouts.remove(key);
        if (task != null && !task.isDone()) {
            task.cancel(false);
        }
    }

    @Override
    public void removeAll(DeliveryKey key) {
        inFlightDeliveries.remove(key);
        pendingOffsets.remove(key);
        batchSendTimestamps.remove(key);
        traceIds.remove(key);
        cancelTimeout(key);
    }

}
