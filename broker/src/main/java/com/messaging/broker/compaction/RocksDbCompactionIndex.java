package com.messaging.broker.compaction;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RocksDB-backed compaction index tracking the latest offset and timestamp per (topic, msgKey).
 *
 * <p>Uses the {@code compaction} column family of the shared {@link SharedRocksDb} instance —
 * no separate DB process or file lock is required.
 *
 * <p>Key format : {@code "topic|msgKey"}  (UTF-8)
 * <p>Value format: {@code [latestOffset:8B][latestTimestampMs:8B]} = 16 bytes fixed
 *
 * <p>A record is considered <em>superseded</em> when a newer record for the same (topic, msgKey)
 * has been appended — i.e. {@code latestOffset > recordOffset}. Superseded records are filtered
 * out during consumer delivery and are eligible for physical deletion by the
 * {@link CompactionScheduler} once they exceed the configured retention period.
 */
@Singleton
@Requires(property = "compaction-index.backend", value = "rocks", defaultValue = "rocks")
public class RocksDbCompactionIndex implements CompactionIndex {

    private static final Logger log = LoggerFactory.getLogger(RocksDbCompactionIndex.class);
    private static final String TOPIC_STALE_MAX_PREFIX = "__meta__|stale|";

    private final RocksDB db;
    private final ColumnFamilyHandle cf;
    private final WriteOptions writeOptions;
    // Guards the read-then-write sequence in updateKey so two concurrent callers for the
    // same (topic, msgKey) cannot both read the old value and then race to write, leaving
    // the lower offset as the winner if the slower write fires last.
    private final ReentrantLock updateLock = new ReentrantLock();

    public RocksDbCompactionIndex(SharedRocksDb sharedDb) {
        this.db           = sharedDb.getDb();
        this.cf           = sharedDb.getCompactionHandle();
        this.writeOptions = sharedDb.getWriteOptions();
    }

    /**
     * Record that {@code newOffset} is now the latest for {@code (topic, msgKey)}.
     * Only advances the index — an out-of-order call with a lower offset is ignored.
     *
     * <p>Synchronized via {@code updateLock} to prevent two concurrent callers for the same key
     * from both reading the old value and then racing to write (the slower write would regress
     * the index if it carries a lower offset).
     */
    @Override
    public void updateKey(String topic, String msgKey, long newOffset, long newTimestampMs) {
        if (msgKey == null) return;  // records without a key are not tracked for compaction
        byte[] key = buildKey(topic, msgKey);
        updateLock.lock();
        try {
            byte[] existing = db.get(cf, key);
            if (existing != null) {
                long existingOffset = ByteBuffer.wrap(existing).getLong();
                if (newOffset <= existingOffset) {
                    return;  // out-of-order — do not regress the index
                }
                recordSupersededOffset(topic, existingOffset);
            }
            db.put(cf, writeOptions, key, encode(newOffset, newTimestampMs));
        } catch (RocksDBException e) {
            log.error("CompactionIndex updateKey failed for topic={} key={}", topic, msgKey, e);
        } finally {
            updateLock.unlock();
        }
    }

    /**
     * Returns {@code true} if a newer record exists for {@code (topic, msgKey)} — i.e. the record
     * at {@code recordOffset} has been superseded and must not be delivered to consumers.
     *
     * <p>Returns {@code false} for records without a key (msgKey == null) — they are never superseded.
     */
    @Override
    public boolean isSuperseded(String topic, String msgKey, long recordOffset) {
        if (msgKey == null) return false;
        byte[] key = buildKey(topic, msgKey);
        try {
            byte[] value = db.get(cf, key);
            if (value == null) return false;
            long latestOffset = ByteBuffer.wrap(value).getLong();
            return latestOffset > recordOffset;
        } catch (RocksDBException e) {
            log.error("CompactionIndex isSuperseded failed for topic={} key={}", topic, msgKey, e);
            return false;
        }
    }

    /**
     * Returns {@code [latestOffset, latestTimestampMs]} for a specific (topic, msgKey),
     * or {@code null} if the key is not in the index.
     */
    @Override
    public long[] getLatestOffsetAndTimestamp(String topic, String msgKey) {
        byte[] key = buildKey(topic, msgKey);
        try {
            byte[] value = db.get(cf, key);
            if (value == null) return null;
            return decode(value);
        } catch (RocksDBException e) {
            log.error("CompactionIndex getLatestOffsetAndTimestamp failed for topic={} key={}", topic, msgKey, e);
            return null;
        }
    }

    /**
     * O(1) check: returns {@code true} if the index contains at least one entry for {@code topic}.
     *
     * <p>Uses a single RocksDB seek + prefix comparison — no map allocation. Callers in the
     * delivery hot-path should use this before deciding whether to decode the batch at all.
     */
    @Override
    public boolean hasIndexedKeysForTopic(String topic) {
        byte[] prefix = (topic + "|").getBytes(StandardCharsets.UTF_8);
        try (RocksIterator iter = db.newIterator(cf)) {
            iter.seek(prefix);
            return iter.isValid() && startsWith(iter.key(), prefix);
        } catch (Exception e) {
            log.warn("CompactionIndex hasIndexedKeysForTopic failed for topic={}, assuming no entries: {}", topic, e.getMessage());
            return false;
        }
    }

    /**
     * Returns {@code true} only when the requested delivery offset could still contain
     * superseded records that have not yet been physically compacted away.
     *
     * <p>We track the highest offset that has become stale for each topic. Any delivery
     * that starts strictly after that offset cannot contain an old version and can stay
     * on the zero-copy path.
     */
    @Override
    public boolean shouldFilterDelivery(String topic, long deliveryStartOffset) {
        long maxSupersededOffset = getMaxSupersededOffset(topic);
        return maxSupersededOffset >= 0 && deliveryStartOffset <= maxSupersededOffset;
    }

    /**
     * Mark all stale offsets up to {@code compactedThroughOffset} as physically removed.
     * When the compaction sweep has covered the highest known stale offset for the topic,
     * the delivery-time filter can be disabled again until a newer duplicate arrives.
     */
    @Override
    public void markCompactedThrough(String topic, long compactedThroughOffset) {
        if (compactedThroughOffset < 0) {
            return;
        }

        updateLock.lock();
        try {
            byte[] metaKey = buildTopicStaleKey(topic);
            byte[] existing = db.get(cf, metaKey);
            if (existing == null) {
                return;
            }

            long maxSupersededOffset = decodeLong(existing);
            if (maxSupersededOffset <= compactedThroughOffset) {
                db.delete(cf, writeOptions, metaKey);
            }
        } catch (RocksDBException e) {
            log.error("CompactionIndex markCompactedThrough failed for topic={} compactedThrough={}",
                    topic, compactedThroughOffset, e);
        } finally {
            updateLock.unlock();
        }
    }

    /**
     * Prefix-scans the compaction column family and returns all entries for {@code topic}.
     *
     * @return map of {@code msgKey -> [latestOffset, latestTimestampMs]}
     */
    @Override
    public Map<String, long[]> getLatestOffsetsForTopic(String topic) {
        byte[] prefix = (topic + "|").getBytes(StandardCharsets.UTF_8);
        Map<String, long[]> result = new HashMap<>();

        try (RocksIterator iter = db.newIterator(cf)) {
            iter.seek(prefix);
            while (iter.isValid()) {
                byte[] rawKey = iter.key();
                if (!startsWith(rawKey, prefix)) break;

                String fullKey = new String(rawKey, StandardCharsets.UTF_8);
                String msgKey  = fullKey.substring(topic.length() + 1);  // strip "topic|"
                result.put(msgKey, decode(iter.value()));
                iter.next();
            }
        }
        return result;
    }

    /**
     * Streaming prefix-scan over all entries for {@code topic} — O(1) memory.
     * Meta keys ({@code __meta__|...}) live outside the {@code topic|} prefix and are
     * never visited.
     */
    @Override
    public void forEachEntry(String topic, IndexEntryConsumer consumer) {
        byte[] prefix = (topic + "|").getBytes(StandardCharsets.UTF_8);
        try (RocksIterator iter = db.newIterator(cf)) {
            iter.seek(prefix);
            while (iter.isValid()) {
                byte[] rawKey = iter.key();
                if (!startsWith(rawKey, prefix)) break;

                String msgKey = new String(rawKey, prefix.length, rawKey.length - prefix.length,
                        StandardCharsets.UTF_8);
                long[] value = decode(iter.value());
                consumer.accept(msgKey, value[0], value[1]);
                iter.next();
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private byte[] buildKey(String topic, String msgKey) {
        return (topic + "|" + msgKey).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] buildTopicStaleKey(String topic) {
        return (TOPIC_STALE_MAX_PREFIX + topic).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encode(long offset, long timestampMs) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(offset);
        buf.putLong(timestampMs);
        return buf.array();
    }

    private byte[] encodeLong(long value) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(value);
        return buf.array();
    }

    private long[] decode(byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        return new long[]{ buf.getLong(), buf.getLong() };
    }

    private long decodeLong(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }

    private long getMaxSupersededOffset(String topic) {
        try {
            byte[] value = db.get(cf, buildTopicStaleKey(topic));
            return value == null ? -1L : decodeLong(value);
        } catch (RocksDBException e) {
            log.error("CompactionIndex getMaxSupersededOffset failed for topic={}", topic, e);
            return -1L;
        }
    }

    private void recordSupersededOffset(String topic, long staleOffset) throws RocksDBException {
        byte[] metaKey = buildTopicStaleKey(topic);
        byte[] existing = db.get(cf, metaKey);
        if (existing != null) {
            long currentMax = decodeLong(existing);
            if (staleOffset <= currentMax) {
                return;
            }
        }
        db.put(cf, writeOptions, metaKey, encodeLong(staleOffset));
    }

    private boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }
}
