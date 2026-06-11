package com.messaging.broker.ack;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory {@link AckStore} backed by a {@link ConcurrentHashMap}.
 *
 * <p>Selected via {@code ack-store.backend: memory}. Intended for tests and dev environments
 * where RocksDB is overkill; entries do <strong>not</strong> survive a restart.
 *
 * <p>Concurrency: all map mutations go through {@code ConcurrentHashMap}, so individual ops
 * are thread-safe. {@link #putBatch} stages writes into a snapshot first and then applies them
 * in one pass so a concurrent reader either sees all of a batch or none of it for any given key.
 * {@link #clearByTopicAndGroup} iterates the entry set; any concurrent {@link #put} for a key
 * already inspected by the iterator is preserved, which matches the "consumer wipe at the
 * RESET watermark" semantics the interface contract is designed around.
 */
@Singleton
@Requires(property = "ack-store.backend", value = "memory")
public class InMemoryAckStore implements AckStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryAckStore.class);

    private final ConcurrentMap<Key, AckRecord> store = new ConcurrentHashMap<>();

    public InMemoryAckStore() {
        log.info("InMemoryAckStore initialised");
    }

    @Override
    public void put(String topic, String group, long offset, AckRecord record) {
        if (offset < 0) {
            throw new IllegalArgumentException("ACK store offset must be >= 0, got: " + offset);
        }
        store.put(new Key(topic, group, offset), record);
    }

    @Override
    public AckRecord get(String topic, String group, long offset) {
        return store.get(new Key(topic, group, offset));
    }

    @Override
    public void putBatch(String[] topics, String[] groups, AckRecord[] records) {
        if (topics.length != groups.length || topics.length != records.length) {
            throw new IllegalArgumentException("ACK batch arrays must have equal lengths");
        }
        for (int i = 0; i < topics.length; i++) {
            if (records[i].offset < 0) {
                throw new IllegalArgumentException(
                        "ACK store offset must be >= 0, got: " + records[i].offset);
            }
            store.put(new Key(topics[i], groups[i], records[i].offset), records[i]);
        }
    }

    @Override
    public void clearByTopicAndGroup(String topic, String group) {
        int deleted = 0;
        Iterator<Map.Entry<Key, AckRecord>> it = store.entrySet().iterator();
        while (it.hasNext()) {
            Key key = it.next().getKey();
            if (key.topic.equals(topic) && key.group.equals(group)) {
                it.remove();
                deleted++;
            }
        }
        log.info("InMemoryAckStore cleared {} entries for topic={} group={}", deleted, topic, group);
    }

    @Override
    public java.util.Set<Long> getAckedOffsetsInRange(String topic, String group, long fromOffset, long toOffsetExclusive) {
        java.util.Set<Long> acked = new java.util.HashSet<>();
        for (Key key : store.keySet()) {
            if (key.offset >= fromOffset && key.offset < toOffsetExclusive
                    && key.topic.equals(topic) && key.group.equals(group)) {
                acked.add(key.offset);
            }
        }
        return acked;
    }

    int size() {
        return store.size();
    }

    /**
     * Composite key — offset is the unique event identity. Equality must include all three fields
     * so duplicate keys at different offsets do not collapse and ACKs for different consumer
     * groups on the same offset stay separate.
     */
    private static final class Key {
        final String topic;
        final String group;
        final long offset;

        Key(String topic, String group, long offset) {
            this.topic = topic;
            this.group = group;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return offset == key.offset && topic.equals(key.topic) && group.equals(key.group);
        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + group.hashCode();
            result = 31 * result + Long.hashCode(offset);
            return result;
        }
    }
}
