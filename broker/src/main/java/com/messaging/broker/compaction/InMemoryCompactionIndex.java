package com.messaging.broker.compaction;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory {@link CompactionIndex} backed by {@link ConcurrentHashMap}s.
 *
 * <p>Selected via {@code compaction-index.backend: memory}. Intended for tests and dev
 * environments — entries do <strong>not</strong> survive a restart, so the broker will lose
 * track of which records have already been superseded and must re-derive them from storage.
 *
 * <p>Concurrency: {@link #updateKey} uses {@link ConcurrentMap#compute} so the read-then-write
 * advance check happens atomically per key — the same "no offset regression" invariant as the
 * Rocks impl. The per-topic max-stale-offset tracking is in a second map and uses {@code merge}
 * with {@code Math::max} so concurrent updates also can't regress it.
 */
@Singleton
@Requires(property = "compaction-index.backend", value = "memory")
public class InMemoryCompactionIndex implements CompactionIndex {

    private static final Logger log = LoggerFactory.getLogger(InMemoryCompactionIndex.class);

    // (topic, msgKey) -> [latestOffset, latestTimestampMs]
    private final ConcurrentMap<TopicKey, long[]> index = new ConcurrentHashMap<>();
    // topic -> highest offset that has been superseded but not yet compacted away.
    // -1 means "nothing stale" → shouldFilterDelivery short-circuits to false.
    private final ConcurrentMap<String, Long> maxSupersededOffset = new ConcurrentHashMap<>();

    public InMemoryCompactionIndex() {
        log.info("InMemoryCompactionIndex initialised");
    }

    @Override
    public void updateKey(String topic, String msgKey, long newOffset, long newTimestampMs) {
        if (msgKey == null) return;
        TopicKey key = new TopicKey(topic, msgKey);
        index.compute(key, (k, existing) -> {
            if (existing != null && newOffset <= existing[0]) {
                return existing; // out-of-order — do not regress
            }
            if (existing != null) {
                // Previous offset is now stale; record under topic.
                maxSupersededOffset.merge(topic, existing[0], Math::max);
            }
            return new long[]{ newOffset, newTimestampMs };
        });
    }

    @Override
    public boolean isSuperseded(String topic, String msgKey, long recordOffset) {
        if (msgKey == null) return false;
        long[] latest = index.get(new TopicKey(topic, msgKey));
        return latest != null && latest[0] > recordOffset;
    }

    @Override
    public long[] getLatestOffsetAndTimestamp(String topic, String msgKey) {
        long[] stored = index.get(new TopicKey(topic, msgKey));
        if (stored == null) return null;
        // Defensive copy — callers should not be able to mutate the index entry.
        return new long[]{ stored[0], stored[1] };
    }

    @Override
    public boolean hasIndexedKeysForTopic(String topic) {
        for (TopicKey key : index.keySet()) {
            if (key.topic.equals(topic)) return true;
        }
        return false;
    }

    @Override
    public boolean shouldFilterDelivery(String topic, long deliveryStartOffset) {
        Long max = maxSupersededOffset.get(topic);
        return max != null && deliveryStartOffset <= max;
    }

    @Override
    public void markCompactedThrough(String topic, long compactedThroughOffset) {
        if (compactedThroughOffset < 0) return;
        maxSupersededOffset.computeIfPresent(topic, (k, current) ->
                current <= compactedThroughOffset ? null : current);
    }

    @Override
    public Map<String, long[]> getLatestOffsetsForTopic(String topic) {
        Map<String, long[]> result = new HashMap<>();
        for (Iterator<Map.Entry<TopicKey, long[]>> it = index.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<TopicKey, long[]> entry = it.next();
            if (entry.getKey().topic.equals(topic)) {
                long[] v = entry.getValue();
                result.put(entry.getKey().msgKey, new long[]{ v[0], v[1] });
            }
        }
        return result;
    }

    @Override
    public void forEachEntry(String topic, IndexEntryConsumer consumer) {
        for (Map.Entry<TopicKey, long[]> entry : index.entrySet()) {
            if (entry.getKey().topic.equals(topic)) {
                long[] v = entry.getValue();
                consumer.accept(entry.getKey().msgKey, v[0], v[1]);
            }
        }
    }

    int indexSize() {
        return index.size();
    }

    private static final class TopicKey {
        final String topic;
        final String msgKey;

        TopicKey(String topic, String msgKey) {
            this.topic = topic;
            this.msgKey = msgKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TopicKey k)) return false;
            return topic.equals(k.topic) && msgKey.equals(k.msgKey);
        }

        @Override
        public int hashCode() {
            return 31 * topic.hashCode() + msgKey.hashCode();
        }
    }
}
