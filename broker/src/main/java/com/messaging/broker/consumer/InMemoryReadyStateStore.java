package com.messaging.broker.consumer;

import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.ReadyStateStore;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * In-memory implementation of ReadyStateStore.
 *
 * Thread-safe concurrent storage for READY_ACK state.
 */
@Singleton
public class InMemoryReadyStateStore implements ReadyStateStore {

    // Map: clientId -> ready status (legacy consumers)
    private final ConcurrentHashMap<String, Boolean> legacyConsumerReady = new ConcurrentHashMap<>();

    // Map: clientId -> Set of subscriptions that are ready (modern consumers)
    private final ConcurrentHashMap<String, Set<DeliveryKey>> modernConsumerTopicsReady = new ConcurrentHashMap<>();

    // Map: retry key -> READY retry task
    private final ConcurrentHashMap<String, ScheduledFuture<?>> pendingReadyRetries = new ConcurrentHashMap<>();

    @Override
    public void markLegacyConsumerReady(String clientId) {
        legacyConsumerReady.put(clientId, true);
    }

    @Override
    public boolean isLegacyConsumerReady(String clientId) {
        return legacyConsumerReady.getOrDefault(clientId, false);
    }

    @Override
    public void markModernConsumerTopicReady(String clientId, DeliveryKey key) {
        modernConsumerTopicsReady
            .computeIfAbsent(clientId, k -> ConcurrentHashMap.newKeySet())
            .add(key);
    }

    @Override
    public boolean isModernConsumerTopicReady(String clientId, DeliveryKey key) {
        Set<DeliveryKey> readyTopics = modernConsumerTopicsReady.get(clientId);
        return readyTopics != null && readyTopics.contains(key);
    }

    @Override
    public Set<DeliveryKey> getModernConsumerReadyTopics(String clientId) {
        Set<DeliveryKey> readyTopics = modernConsumerTopicsReady.get(clientId);
        return readyTopics != null ? Set.copyOf(readyTopics) : Collections.emptySet();
    }

    @Override
    public void scheduleReadyRetry(String key, ScheduledFuture<?> task) {
        // Cancel existing task if present
        ScheduledFuture<?> existing = pendingReadyRetries.put(key, task);
        if (existing != null && !existing.isDone()) {
            existing.cancel(false);
        }
    }

    @Override
    public void cancelReadyRetry(String key) {
        ScheduledFuture<?> task = pendingReadyRetries.remove(key);
        if (task != null && !task.isDone()) {
            task.cancel(false);
        }
    }

    @Override
    public void removeClient(String clientId) {
        legacyConsumerReady.remove(clientId);
        modernConsumerTopicsReady.remove(clientId);

        // Cancel all retry tasks for this client
        pendingReadyRetries.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            boolean matches = key.equals(clientId) || key.startsWith(clientId + ":");
            if (matches) {
                ScheduledFuture<?> task = entry.getValue();
                if (task != null && !task.isDone()) {
                    task.cancel(false);
                }
            }
            return matches;
        });
    }

    @Override
    public void clear() {
        // Cancel all pending tasks
        pendingReadyRetries.values().forEach(task -> {
            if (task != null && !task.isDone()) {
                task.cancel(false);
            }
        });

        legacyConsumerReady.clear();
        modernConsumerTopicsReady.clear();
        pendingReadyRetries.clear();
    }
}
