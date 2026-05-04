package com.messaging.broker.consumer;

import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.consumer.ConsumerSessionStore;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * In-memory implementation of ConsumerSessionStore.
 *
 * Thread-safe concurrent storage for consumer sessions.
 */
@Singleton
public class InMemoryConsumerSessionStore implements ConsumerSessionStore {

    // Map: ConsumerKey -> RemoteConsumer
    private final ConcurrentHashMap<ConsumerKey, RemoteConsumer> consumers = new ConcurrentHashMap<>();

    @Override
    public void put(ConsumerKey key, RemoteConsumer consumer) {
        consumers.put(key, consumer);
    }

    @Override
    public Optional<RemoteConsumer> putIfAbsent(ConsumerKey key, RemoteConsumer consumer) {
        return Optional.ofNullable(consumers.putIfAbsent(key, consumer));
    }

    @Override
    public Optional<RemoteConsumer> get(ConsumerKey key) {
        return Optional.ofNullable(consumers.get(key));
    }

    @Override
    public void remove(ConsumerKey key) {
        consumers.remove(key);
    }

    @Override
    public Collection<RemoteConsumer> getAll() {
        return List.copyOf(consumers.values());
    }

    @Override
    public Collection<RemoteConsumer> getByClientId(String clientId) {
        return consumers.values().stream()
            .filter(c -> c.getClientId().equals(clientId))
            .collect(Collectors.toList());
    }

    @Override
    public Collection<RemoteConsumer> getByTopic(String topic) {
        return consumers.values().stream()
            .filter(c -> c.getTopic().equals(topic))
            .collect(Collectors.toList());
    }

    @Override
    public boolean contains(ConsumerKey key) {
        return consumers.containsKey(key);
    }

    @Override
    public int size() {
        return consumers.size();
    }

    @Override
    public int removeByClientId(String clientId) {
        // ConcurrentHashMap.entrySet().removeIf() removes each matching entry atomically using
        // the internal remove(key, value) operation, avoiding the snapshot-then-delete TOCTOU
        // where a re-subscribing client could arrive between the stream snapshot and the removes
        // and either have its new entry deleted or leave a stale entry in the map.
        AtomicInteger count = new AtomicInteger(0);
        consumers.entrySet().removeIf(entry -> {
            if (entry.getValue().getClientId().equals(clientId)) {
                count.incrementAndGet();
                return true;
            }
            return false;
        });
        return count.get();
    }
}
