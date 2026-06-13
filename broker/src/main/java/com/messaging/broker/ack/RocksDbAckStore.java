package com.messaging.broker.ack;

import com.messaging.broker.compaction.SharedRocksDb;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * RocksDB-backed store for per-(topic, group, offset) ACK records.
 *
 * Key format : "{topic}|{group}|{offset-20digits}"  (UTF-8, zero-padded offset for lex ordering)
 * Value format: AckRecord binary — fixed 16 bytes [offset:8B][ackedAtMs:8B]
 *
 * Offset is the true unique event identity in this system. Keying by offset prevents
 * duplicate msgKeys at different offsets from collapsing to a single row under RocksDB
 * LSM compaction. It also ensures records with null msgKey are tracked correctly.
 *
 * Uses the default column family of the shared {@link SharedRocksDb} instance.
 * Lifecycle (open/close) is managed by {@link SharedRocksDb}.
 */
@Singleton
@Requires(property = "ack-store.backend", value = "rocks", defaultValue = "rocks")
public class RocksDbAckStore implements AckStore {

    private static final Logger log = LoggerFactory.getLogger(RocksDbAckStore.class);

    private final RocksDB db;
    private final ColumnFamilyHandle cf;
    private final WriteOptions writeOptions;

    public RocksDbAckStore(SharedRocksDb sharedDb) {
        this.db           = sharedDb.getDb();
        this.cf           = sharedDb.getDefaultHandle();
        this.writeOptions = sharedDb.getWriteOptions();
        log.info("RocksDbAckStore initialised (shared DB, default column family)");
    }

    // ── Single record operations ──────────────────────────────────────────────

    /**
     * Write or overwrite the ACK record for a (topic, group, offset) triple.
     */
    @Override
    public void put(String topic, String group, long offset, AckRecord record) {
        byte[] key = buildKey(topic, group, offset);
        try {
            db.put(cf, writeOptions, key, record.toBytes());
        } catch (RocksDBException | IllegalStateException e) {
            log.error("RocksDB put failed for topic={} group={} offset={}", topic, group, offset, e);
            throw new AckStoreException(
                    "RocksDB ACK put failed for topic=" + topic + " group=" + group + " offset=" + offset,
                    e);
        }
    }

    /**
     * Retrieve the ACK record for a (topic, group, offset) triple.
     *
     * @return AckRecord if found, null otherwise
     */
    @Override
    public AckRecord get(String topic, String group, long offset) {
        byte[] key = buildKey(topic, group, offset);
        try {
            byte[] value = db.get(cf, key);
            return value != null ? AckRecord.fromBytes(value) : null;
        } catch (RocksDBException | IllegalStateException e) {
            log.error("RocksDB get failed for topic={} group={} offset={}", topic, group, offset, e);
            throw new AckStoreException(
                    "RocksDB ACK get failed for topic=" + topic + " group=" + group + " offset=" + offset,
                    e);
        }
    }

    // ── Batch write ───────────────────────────────────────────────────────────

    /**
     * Write multiple ACK records atomically via RocksDB WriteBatch.
     *
     * Arrays are parallel: topics[i], groups[i], records[i] form one entry.
     * The key is derived from records[i].offset — every record is written regardless of msgKey.
     */
    @Override
    public void putBatch(String[] topics, String[] groups, AckRecord[] records) {
        if (topics.length != groups.length || topics.length != records.length) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "ACK batch arrays must have equal lengths");
        }
        if (topics.length == 0) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            for (int i = 0; i < topics.length; i++) {
                batch.put(cf, buildKey(topics[i], groups[i], records[i].offset), records[i].toBytes());
            }
            db.write(writeOptions, batch);
            log.debug("RocksDB ACK: wrote {} records", topics.length);
        } catch (RocksDBException | IllegalStateException e) {
            log.error("RocksDB putBatch failed (size={})", topics.length, e);
            throw new AckStoreException("RocksDB ACK batch write failed for size=" + topics.length, e);
        }
    }

    // ── Prefix-delete (data refresh) ─────────────────────────────────────────

    /**
     * Delete all ACK records for every offset belonging to a (topic, group) pair.
     *
     * Uses RocksIterator prefix scan + WriteBatch for atomic bulk delete.
     * Called when a data refresh starts (RESET_SENT) so stale ACK data does not
     * persist across a consumer state wipe.
     */
    @Override
    public void clearByTopicAndGroup(String topic, String group) {
        byte[] prefix = (topic + "|" + group + "|").getBytes(StandardCharsets.UTF_8);
        try (WriteBatch batch = new WriteBatch();
             RocksIterator iter = db.newIterator(cf)) {
            iter.seek(prefix);
            int deleted = 0;
            while (iter.isValid()) {
                byte[] key = iter.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                batch.delete(cf, key);
                deleted++;
                iter.next();
            }
            if (deleted > 0) {
                db.write(writeOptions, batch);
            }
            log.info("RocksDB ACK cleared {} entries for topic={} group={}", deleted, topic, group);
        } catch (RocksDBException | IllegalStateException e) {
            log.error("Failed to clear RocksDB ACK for topic={} group={}", topic, group, e);
            throw new AckStoreException(
                    "RocksDB ACK clear failed for topic=" + topic + " group=" + group,
                    e);
        }
    }

    // ── Range scan (seeding / reconciliation) ────────────────────────────────

    /**
     * Collect all acked offsets in {@code [fromOffset, toOffsetExclusive)} with a single
     * prefix iteration. Keys are zero-padded so lexicographic order equals numeric order —
     * seek directly to the first candidate and stop at the range end.
     */
    @Override
    public java.util.Set<Long> getAckedOffsetsInRange(String topic, String group, long fromOffset, long toOffsetExclusive) {
        java.util.Set<Long> acked = new java.util.HashSet<>();
        if (toOffsetExclusive <= fromOffset) {
            return acked;
        }
        byte[] prefix = (topic + "|" + group + "|").getBytes(StandardCharsets.UTF_8);
        byte[] seekKey = buildKey(topic, group, Math.max(0, fromOffset));
        try (RocksIterator iter = db.newIterator(cf)) {
            iter.seek(seekKey);
            while (iter.isValid()) {
                byte[] key = iter.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                long offset = parseOffset(key, prefix.length);
                if (offset >= toOffsetExclusive) {
                    break;
                }
                acked.add(offset);
                iter.next();
            }
        } catch (IllegalStateException e) {
            throw new AckStoreException(
                    "RocksDB ACK range scan failed for topic=" + topic + " group=" + group, e);
        }
        return acked;
    }

    private static long parseOffset(byte[] key, int offsetStart) {
        long value = 0;
        for (int i = offsetStart; i < key.length; i++) {
            value = value * 10 + (key[i] - '0');
        }
        return value;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private byte[] buildKey(String topic, String group, long offset) {
        if (offset < 0) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_OFFSET,
                    "ACK store offset must be >= 0, got: " + offset);
        }
        return (topic + "|" + group + "|" + String.format("%020d", offset))
                .getBytes(StandardCharsets.UTF_8);
    }

    private boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
