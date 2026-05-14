package com.messaging.broker.compaction;

import jakarta.inject.Singleton;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Persists the compaction checkpoint (base offset of the last compacted segment) per
 * topic-partition. Used by {@link CompactionScheduler} to track incremental progress so
 * each run only reprocesses segments that were written after the previous run.
 *
 * <p>Uses the {@code compaction} column family of the shared {@link SharedRocksDb}.
 *
 * <p>Key format : {@code "__ckpt__|topic|partition"} (UTF-8)
 * <p>Value format: {@code [lastCompactedBaseOffset:8B]}
 */
@Singleton
public class CompactionCheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(CompactionCheckpointStore.class);
    private static final String KEY_PREFIX = "__ckpt__|";

    private final RocksDB db;
    private final ColumnFamilyHandle cf;
    private final WriteOptions writeOptions;

    public CompactionCheckpointStore(SharedRocksDb sharedDb) {
        this.db           = sharedDb.getDb();
        this.cf           = sharedDb.getCompactionHandle();
        this.writeOptions = sharedDb.getWriteOptions();
    }

    /**
     * Persist the base offset of the last successfully compacted segment for {@code topic-partition}.
     */
    public void saveCheckpoint(String topic, int partition, long baseOffset) {
        byte[] key   = buildKey(topic, partition);
        byte[] value = ByteBuffer.allocate(8).putLong(baseOffset).array();
        try {
            db.put(cf, writeOptions, key, value);
        } catch (RocksDBException e) {
            log.error("Failed to save compaction checkpoint for {}-{}", topic, partition, e);
        }
    }

    /**
     * Load the last known compaction checkpoint for {@code topic-partition}.
     *
     * @return the base offset of the last compacted segment, or {@code -1L} if no checkpoint exists
     */
    public long loadCheckpoint(String topic, int partition) {
        byte[] key = buildKey(topic, partition);
        try {
            byte[] value = db.get(cf, key);
            if (value == null) return -1L;
            return ByteBuffer.wrap(value).getLong();
        } catch (RocksDBException e) {
            log.error("Failed to load compaction checkpoint for {}-{}", topic, partition, e);
            return -1L;
        }
    }

    private byte[] buildKey(String topic, int partition) {
        return (KEY_PREFIX + topic + "|" + partition).getBytes(StandardCharsets.UTF_8);
    }
}
