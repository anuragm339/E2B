package com.messaging.broker.consumer;

import com.messaging.broker.consumer.RemoteConsumer;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.consumer.ConsumerSessionStore;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
        List<ConsumerKey> toRemove = consumers.entrySet().stream()
            .filter(entry -> entry.getValue().getClientId().equals(clientId))
            .map(entry -> entry.getKey())
            .collect(Collectors.toList());

        toRemove.forEach(consumers::remove);
        return toRemove.size();
    }
}
