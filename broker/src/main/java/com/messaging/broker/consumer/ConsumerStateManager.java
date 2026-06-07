package com.messaging.broker.consumer;

import com.messaging.broker.consumer.ConsumerStateService;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.InFlightDeliveryStore;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates in-flight and pending delivery state.
 */
@Singleton
public class ConsumerStateManager implements ConsumerStateService {

    private final InFlightDeliveryStore deliveryStore;

    @Inject
    public ConsumerStateManager(InFlightDeliveryStore deliveryStore) {
        this.deliveryStore = deliveryStore;
    }

    @Override
    public long beginDelivery(DeliveryKey key) {
        return deliveryStore.beginDelivery(key);
    }

    @Override
    public void setOriginalOffset(DeliveryKey key, long generation, long originalOffset) {
        deliveryStore.setOriginalOffset(key, generation, originalOffset);
    }

    @Override
    public PendingDelivery claimPendingDelivery(DeliveryKey key) {
        return deliveryStore.claimPendingDelivery(key);
    }

    @Override
    public PendingDelivery claimPendingDelivery(DeliveryKey key, long expectedGeneration) {
        return deliveryStore.claimPendingDelivery(key, expectedGeneration);
    }

    @Override
    public boolean scheduleTimeout(
            DeliveryKey key,
            long expectedGeneration,
            ScheduledFuture<?> task) {
        return deliveryStore.scheduleTimeout(key, expectedGeneration, task);
    }

    @Override
    public boolean completeDelivery(DeliveryKey key, long expectedGeneration) {
        return deliveryStore.completeDelivery(key, expectedGeneration);
    }

    @Override
    public AtomicBoolean markInFlight(DeliveryKey key) {
        return deliveryStore.markInFlight(key);
    }

    @Override
    public boolean isInFlight(DeliveryKey key) {
        return deliveryStore.isInFlight(key);
    }

    @Override
    public void clearInFlight(DeliveryKey key) {
        deliveryStore.clearInFlight(key);
    }

    @Override
    public void setPendingOffset(DeliveryKey key, long offset) {
        deliveryStore.setPendingOffset(key, offset);
    }

    @Override
    public Long getPendingOffset(DeliveryKey key) {
        return deliveryStore.getPendingOffset(key);
    }

    @Override
    public void clearPendingOffset(DeliveryKey key) {
        deliveryStore.clearPendingOffset(key);
    }

    @Override
    public Long removePendingOffset(DeliveryKey key) {
        return deliveryStore.removePendingOffset(key);
    }

    @Override
    public void recordBatchSendTime(DeliveryKey key, long timestamp) {
        deliveryStore.recordBatchSendTime(key, timestamp);
    }

    @Override
    public Long getBatchSendTime(DeliveryKey key) {
        return deliveryStore.getBatchSendTime(key);
    }

    @Override
    public void recordTraceId(DeliveryKey key, String traceId) {
        deliveryStore.recordTraceId(key, traceId);
    }

    @Override
    public String getTraceId(DeliveryKey key) {
        return deliveryStore.getTraceId(key);
    }

    @Override
    public void clearTraceId(DeliveryKey key) {
        deliveryStore.clearTraceId(key);
    }

    @Override
    public void scheduleTimeout(DeliveryKey key, ScheduledFuture<?> task) {
        deliveryStore.scheduleTimeout(key, task);
    }

    @Override
    public void cancelTimeout(DeliveryKey key) {
        deliveryStore.cancelTimeout(key);
    }

    @Override
    public void removeDeliveryState(DeliveryKey key) {
        deliveryStore.removeAll(key);
    }

    @Override
    public void setFromOffset(DeliveryKey key, long fromOffset) {
        deliveryStore.setFromOffset(key, fromOffset);
    }

    @Override
    public Long getFromOffset(DeliveryKey key) {
        return deliveryStore.getFromOffset(key);
    }

    @Override
    public void clearFromOffset(DeliveryKey key) {
        deliveryStore.clearFromOffset(key);
    }

}
