package com.messaging.broker.ack;

import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * RocksDB-backed store for per-(topic, group, msgKey) ACK records.
 *
 * Key format : "{topic}|{group}|{msgKey}"  (UTF-8, '|' is safe in topic/group names)
 * Value format: AckRecord binary — fixed 16 bytes [offset:8B][ackedAtMs:8B]
 *
 * LSM compaction automatically retains only the latest value per key, giving
 * "latest-ACK-wins" semantics for free.
 */
@Singleton
public class RocksDbAckStore {

    private static final Logger log = LoggerFactory.getLogger(RocksDbAckStore.class);

    private final String dbPath;
    private final long blockCacheBytes;

    private RocksDB db;
    private Options options;
    private WriteOptions writeOptions;

    public RocksDbAckStore(
            @Value("${ack-store.rocksdb.path}") String dbPath,
            @Value("${ack-store.rocksdb.block-cache-bytes:33554432}") long blockCacheBytes) {
        this.dbPath = dbPath;
        this.blockCacheBytes = blockCacheBytes;
    }

    @PostConstruct
    public void init() throws RocksDBException {
        RocksDB.loadLibrary();

        // Block-based table config: LRU cache + Bloom filter for point lookups
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(new LRUCache(blockCacheBytes))
                .setFilterPolicy(new BloomFilter(10, false));

        options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(16 * 1024 * 1024)          // 16 MB memtable
                .setMaxWriteBufferNumber(2)                      // 1 active + 1 flushing
                .setMaxBackgroundJobs(2)                         // 1 compaction + 1 flush
                .setCompressionType(CompressionType.LZ4_COMPRESSION)       // L0-L5
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION) // L6
                .setTableFormatConfig(tableConfig)
                .optimizeForPointLookup(blockCacheBytes);

        // Ensure parent directories exist
        new File(dbPath).mkdirs();

        writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);

        db = RocksDB.open(options, dbPath);
        log.info("RocksDbAckStore opened at {}", dbPath);
    }

    @PreDestroy
    public void close() {
        if (db != null) {
            db.close();
            log.info("RocksDbAckStore closed");
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (options != null) {
            options.close();
        }
    }

    // ── Single record operations ──────────────────────────────────────────────

    /**
     * Write or overwrite the ACK record for a (topic, group, msgKey) triple.
     */
    public void put(String topic, String group, String msgKey, AckRecord record) {
        byte[] key = buildKey(topic, group, msgKey);
        try {
            db.put(writeOptions, key, record.toBytes());
        } catch (RocksDBException e) {
            log.error("RocksDB put failed for topic={} group={} msgKey={}", topic, group, msgKey, e);
        }
    }

    /**
     * Retrieve the ACK record for a (topic, group, msgKey) triple.
     *
     * @return AckRecord if found, null otherwise
     */
    public AckRecord get(String topic, String group, String msgKey) {
        byte[] key = buildKey(topic, group, msgKey);
        try {
            byte[] value = db.get(key);
            return value != null ? AckRecord.fromBytes(value) : null;
        } catch (RocksDBException e) {
            log.error("RocksDB get failed for topic={} group={} msgKey={}", topic, group, msgKey, e);
            return null;
        }
    }

    // ── Batch write ───────────────────────────────────────────────────────────

    /**
     * Write multiple ACK records atomically via RocksDB WriteBatch.
     *
     * Arrays are parallel: topics[i], groups[i], msgKeys[i], records[i] form one entry.
     * Null msgKeys are silently skipped.
     */
    public void putBatch(String[] topics, String[] groups, String[] msgKeys, AckRecord[] records) {
        if (topics.length == 0) {
            return;
        }
        try (WriteBatch batch = new WriteBatch()) {
            int written = 0;
            for (int i = 0; i < topics.length; i++) {
                if (msgKeys[i] == null) {
                    continue;
                }
                batch.put(buildKey(topics[i], groups[i], msgKeys[i]), records[i].toBytes());
                written++;
            }
            if (written > 0) {
                db.write(writeOptions, batch);
                log.debug("RocksDB ACK: wrote {} records (skipped {} null msgKeys)",
                        written, topics.length - written);
            }
        } catch (RocksDBException e) {
            log.error("RocksDB putBatch failed (size={})", topics.length, e);
        }
    }

    // ── Prefix-delete (data refresh) ─────────────────────────────────────────

    /**
     * Delete all ACK records for every msgKey belonging to a (topic, group) pair.
     *
     * Uses RocksIterator prefix scan + WriteBatch for atomic bulk delete.
     * Called when a data refresh starts (RESET_SENT) so stale ACK data does not
     * persist across a consumer state wipe.
     */
    public void clearByTopicAndGroup(String topic, String group) {
        byte[] prefix = (topic + "|" + group + "|").getBytes(StandardCharsets.UTF_8);
        try (WriteBatch batch = new WriteBatch();
             RocksIterator iter = db.newIterator()) {
            iter.seek(prefix);
            int deleted = 0;
            while (iter.isValid()) {
                byte[] key = iter.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                batch.delete(key);
                deleted++;
                iter.next();
            }
            if (deleted > 0) {
                db.write(writeOptions, batch);
            }
            log.info("RocksDB ACK cleared {} entries for topic={} group={}", deleted, topic, group);
        } catch (RocksDBException e) {
            log.error("Failed to clear RocksDB ACK for topic={} group={}", topic, group, e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private byte[] buildKey(String topic, String group, String msgKey) {
        return (topic + "|" + group + "|" + msgKey).getBytes(StandardCharsets.UTF_8);
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
