package com.messaging.broker.compaction;

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

    public SharedRocksDb(
            @Value("${ack-store.rocksdb.path}") String dbPath,
            @Value("${ack-store.rocksdb.block-cache-bytes:33554432}") long blockCacheBytes) {
        this.dbPath = dbPath;
        this.blockCacheBytes = blockCacheBytes;
    }

    @PostConstruct
    public void init() throws RocksDBException {
        RocksDB.loadLibrary();

        cfOptionsList = new ArrayList<>();

        ColumnFamilyOptions defaultCfOptions = buildCfOptions();
        ColumnFamilyOptions compactionCfOptions = buildCfOptions();
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

        log.info("SharedRocksDb opened at {} with column families: default, compaction", dbPath);
    }

    @PreDestroy
    public void close() {
        if (compactionHandle != null) compactionHandle.close();
        if (defaultHandle    != null) defaultHandle.close();
        if (db               != null) db.close();
        if (writeOptions     != null) writeOptions.close();
        if (dbOptions        != null) dbOptions.close();
        if (cfOptionsList    != null) cfOptionsList.forEach(ColumnFamilyOptions::close);
        log.info("SharedRocksDb closed");
    }

    public RocksDB getDb()                          { return db; }
    public ColumnFamilyHandle getDefaultHandle()    { return defaultHandle; }
    public ColumnFamilyHandle getCompactionHandle() { return compactionHandle; }
    public WriteOptions getWriteOptions()           { return writeOptions; }

    private ColumnFamilyOptions buildCfOptions() {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(new LRUCache(blockCacheBytes))
                .setFilterPolicy(new BloomFilter(10, false));

        return new ColumnFamilyOptions()
                .setWriteBufferSize(16 * 1024 * 1024)
                .setMaxWriteBufferNumber(2)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(tableConfig);
    }
}
