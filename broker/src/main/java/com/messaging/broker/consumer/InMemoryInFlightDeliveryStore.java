package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.InFlightDeliveryStore;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory implementation of InFlightDeliveryStore.
 *
 * Thread-safe concurrent storage for delivery tracking.
 */
@Singleton
public class InMemoryInFlightDeliveryStore implements InFlightDeliveryStore {
    private static final int LOCK_STRIPES = 64;

    private record TimeoutRegistration(long generation, ScheduledFuture<?> task) {
    }

    private final AtomicLong nextGeneration = new AtomicLong();
    private final Object[] stateLocks = createStateLocks();
    private final ConcurrentHashMap<DeliveryKey, Long> activeGenerations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<DeliveryKey, Long> originalOffsets = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> in-flight status
    private final ConcurrentHashMap<DeliveryKey, AtomicBoolean> inFlightDeliveries = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> pending offset to commit on ACK
    private final ConcurrentHashMap<DeliveryKey, Long> pendingOffsets = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> timestamp when batch was sent
    private final ConcurrentHashMap<DeliveryKey, Long> batchSendTimestamps = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> correlation id for the current send/ack/timeout flow
    private final ConcurrentHashMap<DeliveryKey, String> traceIds = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> timeout task
    private final ConcurrentHashMap<DeliveryKey, TimeoutRegistration> pendingTimeouts = new ConcurrentHashMap<>();

    // Map: DeliveryKey -> batch start offset stored at send-time for ACK-time msgKey lookup
    private final ConcurrentHashMap<DeliveryKey, Long> fromOffsets = new ConcurrentHashMap<>();

    @Override
    public long beginDelivery(DeliveryKey key) {
        synchronized (lockFor(key)) {
            AtomicBoolean inFlight = inFlightDeliveries.get(key);
            if (inFlight == null || !inFlight.get()) {
                throw new MessagingException(ErrorCode.BROKER_INVALID_STATE,
                        "Delivery generation requires an in-flight claim for " + key);
            }
            long generation = nextGeneration.incrementAndGet();
            activeGenerations.put(key, generation);
            return generation;
        }
    }

    @Override
    public void setOriginalOffset(DeliveryKey key, long generation, long originalOffset) {
        synchronized (lockFor(key)) {
            if (isActiveGeneration(key, generation)) {
                originalOffsets.put(key, originalOffset);
            }
        }
    }

    @Override
    public PendingDelivery claimPendingDelivery(DeliveryKey key) {
        synchronized (lockFor(key)) {
            Long generation = activeGenerations.get(key);
            return generation == null ? null : claimPendingDeliveryLocked(key, generation);
        }
    }

    @Override
    public PendingDelivery claimPendingDelivery(DeliveryKey key, long expectedGeneration) {
        synchronized (lockFor(key)) {
            if (!isActiveGeneration(key, expectedGeneration)) {
                return null;
            }
            return claimPendingDeliveryLocked(key, expectedGeneration);
        }
    }

    @Override
    public boolean scheduleTimeout(
            DeliveryKey key,
            long expectedGeneration,
            ScheduledFuture<?> task) {
        synchronized (lockFor(key)) {
            if (!isActiveGeneration(key, expectedGeneration)
                    || !pendingOffsets.containsKey(key)) {
                task.cancel(false);
                return false;
            }
            TimeoutRegistration existing =
                    pendingTimeouts.put(key, new TimeoutRegistration(expectedGeneration, task));
            cancel(existing);
            return true;
        }
    }

    @Override
    public boolean completeDelivery(DeliveryKey key, long expectedGeneration) {
        synchronized (lockFor(key)) {
            if (!isActiveGeneration(key, expectedGeneration)) {
                return false;
            }
            clearStateLocked(key);
            activeGenerations.remove(key);
            AtomicBoolean status = inFlightDeliveries.get(key);
            if (status != null) {
                status.set(false);
            }
            return true;
        }
    }

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
        synchronized (lockFor(key)) {
            pendingOffsets.put(key, offset);
        }
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
    public Long removePendingOffset(DeliveryKey key) {
        return pendingOffsets.remove(key);
    }

    @Override
    public void recordBatchSendTime(DeliveryKey key, long timestamp) {
        synchronized (lockFor(key)) {
            batchSendTimestamps.put(key, timestamp);
        }
    }

    @Override
    public Long getBatchSendTime(DeliveryKey key) {
        return batchSendTimestamps.get(key);
    }

    @Override
    public void recordTraceId(DeliveryKey key, String traceId) {
        synchronized (lockFor(key)) {
            traceIds.put(key, traceId);
        }
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
        synchronized (lockFor(key)) {
            Long generation = activeGenerations.get(key);
            if (generation == null) {
                task.cancel(false);
                return;
            }
            scheduleTimeout(key, generation, task);
        }
    }

    @Override
    public void cancelTimeout(DeliveryKey key) {
        synchronized (lockFor(key)) {
            cancel(pendingTimeouts.remove(key));
        }
    }

    @Override
    public void removeAll(DeliveryKey key) {
        synchronized (lockFor(key)) {
            clearStateLocked(key);
            activeGenerations.remove(key);
            inFlightDeliveries.remove(key);
        }
    }

    @Override
    public void setFromOffset(DeliveryKey key, long fromOffset) {
        synchronized (lockFor(key)) {
            fromOffsets.put(key, fromOffset);
        }
    }

    @Override
    public Long getFromOffset(DeliveryKey key) {
        return fromOffsets.get(key);
    }

    @Override
    public void clearFromOffset(DeliveryKey key) {
        fromOffsets.remove(key);
    }

    private Object lockFor(DeliveryKey key) {
        int index = (key.hashCode() & Integer.MAX_VALUE) % stateLocks.length;
        return stateLocks[index];
    }

    private static Object[] createStateLocks() {
        Object[] locks = new Object[LOCK_STRIPES];
        for (int index = 0; index < locks.length; index++) {
            locks[index] = new Object();
        }
        return locks;
    }

    private boolean isActiveGeneration(DeliveryKey key, long expectedGeneration) {
        Long activeGeneration = activeGenerations.get(key);
        return activeGeneration != null && activeGeneration == expectedGeneration;
    }

    private PendingDelivery claimPendingDeliveryLocked(DeliveryKey key, long generation) {
        Long pendingOffset = pendingOffsets.remove(key);
        if (pendingOffset == null) {
            return null;
        }

        cancel(pendingTimeouts.remove(key));
        return new PendingDelivery(
                generation,
                originalOffsets.getOrDefault(key, pendingOffset),
                pendingOffset,
                fromOffsets.get(key),
                batchSendTimestamps.get(key),
                traceIds.get(key));
    }

    private void clearStateLocked(DeliveryKey key) {
        pendingOffsets.remove(key);
        originalOffsets.remove(key);
        batchSendTimestamps.remove(key);
        traceIds.remove(key);
        fromOffsets.remove(key);
        cancel(pendingTimeouts.remove(key));
    }

    private void cancel(TimeoutRegistration registration) {
        if (registration != null && !registration.task().isDone()) {
            registration.task().cancel(false);
        }
    }

}
