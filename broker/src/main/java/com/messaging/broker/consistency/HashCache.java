package com.messaging.broker.consistency;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Bounded LRU cache of computed range hashes plus in-flight coalescing so that
 * concurrent identical requests collapse into one computation. Used both by the
 * outbound UpstreamConsistencyClient (cache upstream responses) and by the
 * broker's PipeServer consistency endpoint (cache local computations).
 */
@Singleton
public class HashCache {

    private final int maxEntries;
    private final Map<Key, CachedHash> store;
    private final ConcurrentHashMap<Key, CompletableFuture<CachedHash>> inFlight = new ConcurrentHashMap<>();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    public HashCache(@Value("${pipe.consistency.hash-cache.max-entries:10000}") int maxEntries) {
        this.maxEntries = Math.max(64, maxEntries);
        this.store = Collections.synchronizedMap(new LinkedHashMap<>(this.maxEntries, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Key, CachedHash> eldest) {
                return size() > HashCache.this.maxEntries;
            }
        });
    }

    public CachedHash getOrCompute(Key key, Supplier<CachedHash> compute) {
        CachedHash cached = store.get(key);
        if (cached != null) {
            hits.incrementAndGet();
            return cached;
        }
        CompletableFuture<CachedHash> mine = new CompletableFuture<>();
        CompletableFuture<CachedHash> existing = inFlight.putIfAbsent(key, mine);
        if (existing != null) {
            try {
                return existing.get();
            } catch (Exception e) {
                throw new RuntimeException("Coalesced hash computation failed for " + key, e);
            }
        }
        try {
            CachedHash result = compute.get();
            store.put(key, result);
            misses.incrementAndGet();
            mine.complete(result);
            return result;
        } catch (RuntimeException e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, mine);
        }
    }

    public void invalidateOverlapping(String topic, long fromOffset, long toOffset) {
        if (toOffset < fromOffset) return;
        store.keySet().removeIf(k ->
                Objects.equals(k.topic, topic) && k.fromOffsetInclusive <= toOffset && k.toOffsetInclusive >= fromOffset);
    }

    public long hits() { return hits.get(); }
    public long misses() { return misses.get(); }
    public int size() { return store.size(); }

    public static final class Key {
        public final String topic;
        public final long fromOffsetInclusive;
        public final long toOffsetInclusive;
        public final String projection;

        public Key(String topic, long fromOffsetInclusive, long toOffsetInclusive, String projection) {
            this.topic = topic;
            this.fromOffsetInclusive = fromOffsetInclusive;
            this.toOffsetInclusive = toOffsetInclusive;
            this.projection = projection;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) return false;
            Key k = (Key) o;
            return fromOffsetInclusive == k.fromOffsetInclusive
                    && toOffsetInclusive == k.toOffsetInclusive
                    && Objects.equals(topic, k.topic)
                    && Objects.equals(projection, k.projection);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, fromOffsetInclusive, toOffsetInclusive, projection);
        }

        @Override
        public String toString() {
            return "Key{" + topic + "," + fromOffsetInclusive + ".." + toOffsetInclusive + "," + projection + "}";
        }
    }

    public static final class CachedHash {
        public final byte[] hash;
        public final long recordCount;
        public final String projection;

        public CachedHash(byte[] hash, long recordCount, String projection) {
            this.hash = hash;
            this.recordCount = recordCount;
            this.projection = projection;
        }
    }
}
