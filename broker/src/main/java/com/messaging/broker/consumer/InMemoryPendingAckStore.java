package com.messaging.broker.consumer;

import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.consumer.PendingAckStore;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory implementation of PendingAckStore.
 *
 * Thread-safe concurrent storage for legacy batch ACK tracking.
 */
@Singleton
public class InMemoryPendingAckStore implements PendingAckStore {

    private final AtomicLong nextGeneration = new AtomicLong();
    private final ConcurrentHashMap<String, PendingLegacyDelivery> pendingDeliveries =
            new ConcurrentHashMap<>();

    @Override
    public long reservePendingBatch(
            String clientId,
            MergedBatch batch,
            Timer.Sample timerSample,
            long sendTimeMs) {
        long generation = nextGeneration.incrementAndGet();
        PendingLegacyDelivery delivery =
                new PendingLegacyDelivery(generation, batch, timerSample, sendTimeMs);
        return pendingDeliveries.putIfAbsent(clientId, delivery) == null ? generation : -1L;
    }

    @Override
    public PendingLegacyDelivery claimPendingDelivery(String clientId) {
        return pendingDeliveries.remove(clientId);
    }

    @Override
    public PendingLegacyDelivery claimPendingDelivery(String clientId, long expectedGeneration) {
        AtomicReference<PendingLegacyDelivery> claimed = new AtomicReference<>();
        pendingDeliveries.computeIfPresent(clientId, (ignored, delivery) -> {
            if (delivery.generation() != expectedGeneration) {
                return delivery;
            }
            claimed.set(delivery);
            return null;
        });
        return claimed.get();
    }

    @Override
    public void putPendingBatch(String clientId, MergedBatch batch) {
        long generation = nextGeneration.incrementAndGet();
        pendingDeliveries.put(
                clientId,
                new PendingLegacyDelivery(generation, batch, null, -1L));
    }

    @Override
    public boolean putPendingBatchIfAbsent(String clientId, MergedBatch batch) {
        return reservePendingBatch(clientId, batch, null, -1L) >= 0;
    }

    @Override
    public MergedBatch getPendingBatch(String clientId) {
        PendingLegacyDelivery delivery = pendingDeliveries.get(clientId);
        return delivery == null ? null : delivery.batch();
    }

    @Override
    public MergedBatch removePendingBatch(String clientId) {
        AtomicReference<MergedBatch> removed = new AtomicReference<>();
        pendingDeliveries.computeIfPresent(clientId, (ignored, delivery) -> {
            removed.set(delivery.batch());
            return new PendingLegacyDelivery(
                    delivery.generation(), null, delivery.timer(), delivery.sendTime());
        });
        return removed.get();
    }

    @Override
    public void startTimer(String clientId, Timer.Sample timerSample) {
        pendingDeliveries.computeIfPresent(clientId, (ignored, delivery) ->
                new PendingLegacyDelivery(
                        delivery.generation(), delivery.batch(), timerSample, delivery.sendTime()));
    }

    @Override
    public Timer.Sample removeTimer(String clientId) {
        AtomicReference<Timer.Sample> removed = new AtomicReference<>();
        pendingDeliveries.computeIfPresent(clientId, (ignored, delivery) -> {
            removed.set(delivery.timer());
            return new PendingLegacyDelivery(
                    delivery.generation(), delivery.batch(), null, delivery.sendTime());
        });
        return removed.get();
    }

    @Override
    public void recordSendTime(String clientId, long sendTimeMs) {
        pendingDeliveries.computeIfPresent(clientId, (ignored, delivery) ->
                new PendingLegacyDelivery(
                        delivery.generation(), delivery.batch(), delivery.timer(), sendTimeMs));
    }

    @Override
    public long getSendTime(String clientId) {
        PendingLegacyDelivery delivery = pendingDeliveries.get(clientId);
        return delivery == null ? -1L : delivery.sendTime();
    }

    @Override
    public void removeClient(String clientId) {
        pendingDeliveries.remove(clientId);
    }

    @Override
    public void clear() {
        pendingDeliveries.clear();
    }
}
