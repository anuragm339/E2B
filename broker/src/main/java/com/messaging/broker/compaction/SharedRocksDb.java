package com.messaging.broker.compaction;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.StorageException;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Shared RocksDB instance used by both the ACK store and the compaction index.
 *
 * Opens the DB at {@code ack-store.rocksdb.path} with two column families:
 * <ul>
 *   <li>{@code default} — used by {@link com.messaging.broker.ack.RocksDbAckStore}</li>
 *   <li>{@code compaction} — used by {@link RocksDbCompactionIndex}</li>
 * </ul>
 *
 * Existing installations that have a single-CF DB are migrated automatically:
 * {@code setCreateMissingColumnFamilies(true)} creates the {@code compaction} CF on first open.
 *
 * <h2>Block cache sharing</h2>
 * <p>A <em>single</em> {@link LRUCache} instance is shared across both column families.
 * The original code created one independent cache per CF, silently doubling the configured
 * size (2 × 32 MB = 64 MB).  With a shared cache the total cost equals exactly
 * {@code blockCacheBytes} (default 16 MB — see {@code application.yml}).
 */
@Singleton
public class SharedRocksDb {

    private static final Logger log = LoggerFactory.getLogger(SharedRocksDb.class);
    static final byte[] COMPACTION_CF_NAME = "compaction".getBytes(StandardCharsets.UTF_8);

    private final String dbPath;
    private final long blockCacheBytes;

    private RocksDB db;
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle compactionHandle;
    private WriteOptions writeOptions;
    private DBOptions dbOptions;
    private List<ColumnFamilyOptions> cfOptionsList;
    // Single shared cache — closed explicitly in @PreDestroy to release off-heap memory promptly.
    private LRUCache sharedBlockCache;

    public SharedRocksDb(
            @Value("${ack-store.rocksdb.path}") String dbPath,
            @Value("${ack-store.rocksdb.block-cache-bytes:16777216}") long blockCacheBytes) {
        this.dbPath = dbPath;
        this.blockCacheBytes = blockCacheBytes;
    }

    @PostConstruct
    public void init() throws RocksDBException {
        RocksDB.loadLibrary();

        // One LRUCache shared by both CFs — prevents the "two independent caches" memory doubling.
        sharedBlockCache = new LRUCache(blockCacheBytes);

        cfOptionsList = new ArrayList<>();

        ColumnFamilyOptions defaultCfOptions    = buildCfOptions(sharedBlockCache);
        ColumnFamilyOptions compactionCfOptions = buildCfOptions(sharedBlockCache);
        cfOptionsList.add(defaultCfOptions);
        cfOptionsList.add(compactionCfOptions);

        List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCfOptions),
                new ColumnFamilyDescriptor(COMPACTION_CF_NAME, compactionCfOptions)
        );

        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundJobs(2);

        new File(dbPath).mkdirs();

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);

        defaultHandle    = cfHandles.get(0);
        compactionHandle = cfHandles.get(1);

        writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);

        log.info("SharedRocksDb opened at {} with column families: default, compaction " +
                 "(shared block cache: {} MB)", dbPath, blockCacheBytes / (1024 * 1024));
    }

    /**
     * Empty both column families IN PLACE, keeping the open DB handle valid.
     *
     * <p>The bootstrap wipe used to delete the {@code ack-store/} directory on disk
     * ({@code LocalStateCleaner.clearState}). But this DB is a long-lived {@code @Singleton} opened
     * once at startup and held (with cached column-family handles) by
     * {@link com.messaging.broker.ack.RocksDbAckStore} and {@link RocksDbCompactionIndex}. Deleting
     * its files out from under the open handle corrupted it — every subsequent write threw
     * {@code "While open a file for appending: NNN.log: No such file or directory"} — so the first
     * pipe record ingested after a wipe failed its compaction-index write and the pipe stalled on
     * that record forever. Clearing via range tombstones produces the same empty end-state while the
     * handle (and every cached handle) stays valid; the index then repopulates as data is re-sourced.
     */
    public void clearCompactionAndAck() {
        try {
            clearColumnFamily(defaultHandle);
            clearColumnFamily(compactionHandle);
            log.info("event=ack_store.cleared_in_place path={}", dbPath);
        } catch (RocksDBException e) {
            throw ExceptionLogger.logAndThrow(log, new StorageException(
                    ErrorCode.STORAGE_METADATA_ERROR,
                    "Failed to clear ack-store/compaction RocksDB for bootstrap", e)
                    .withContext("dbPath", dbPath));
        }
    }

    /** Delete every key in {@code handle} without dropping the column family (handle stays valid). */
    private void clearColumnFamily(ColumnFamilyHandle handle) throws RocksDBException {
        byte[] first;
        byte[] last;
        try (RocksIterator it = db.newIterator(handle)) {
            it.seekToFirst();
            if (!it.isValid()) {
                return; // already empty
            }
            first = it.key();
            it.seekToLast();
            last = it.key();
        }
        // deleteRange covers [first, last) — exclusive of the last key, so remove that one explicitly.
        db.deleteRange(handle, first, last);
        db.delete(handle, writeOptions, last);
    }

    @PreDestroy
    public void close() {
        if (compactionHandle != null) compactionHandle.close();
        if (defaultHandle    != null) defaultHandle.close();
        if (db               != null) db.close();
        if (writeOptions     != null) writeOptions.close();
        if (dbOptions        != null) dbOptions.close();
        if (cfOptionsList    != null) cfOptionsList.forEach(ColumnFamilyOptions::close);
        // Release off-heap block cache memory promptly — important inside a 600 MB container.
        if (sharedBlockCache != null) sharedBlockCache.close();
        log.info("SharedRocksDb closed");
    }

    public RocksDB getDb()                          { return db; }
    public ColumnFamilyHandle getDefaultHandle()    { return defaultHandle; }
    public ColumnFamilyHandle getCompactionHandle() { return compactionHandle; }
    public WriteOptions getWriteOptions()           { return writeOptions; }

    /**
     * Build {@link ColumnFamilyOptions} that reference the provided shared block cache.
     * Using a shared cache ensures that the total cache footprint is bounded by
     * {@code blockCacheBytes} regardless of how many CFs are opened.
     */
    private ColumnFamilyOptions buildCfOptions(LRUCache sharedCache) {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(sharedCache)
                .setFilterPolicy(new BloomFilter(10, false));

        return new ColumnFamilyOptions()
                .setWriteBufferSize(8 * 1024 * 1024)   // 8 MB per CF (was 16 MB) — saves ~16 MB
                .setMaxWriteBufferNumber(2)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(tableConfig);
    }
}
