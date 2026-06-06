package com.messaging.storage.metadata

import com.messaging.common.exception.StorageException
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

class SegmentMetadataStoreSpec extends Specification {

    @TempDir
    Path tempDir

    def "initialization fails when db path is a directory"() {
        given: "a directory at the db file path"
        def topicDir = tempDir.resolve("bad-topic")
        Files.createDirectories(topicDir.resolve("segment_metadata.db"))

        when: "initializing the store"
        new SegmentMetadataStore(topicDir)

        then: "storage exception is thrown"
        thrown(StorageException)
    }

    def "saveSegment throws after connection is closed"() {
        given: "a store with a closed connection"
        def store = new SegmentMetadataStore(tempDir)
        store.close()

        when: "saving metadata"
        store.saveSegment(sampleMetadata())

        then: "storage exception is thrown"
        thrown(StorageException)
    }

    def "getSegments throws after connection is closed"() {
        given: "a store with a closed connection"
        def store = new SegmentMetadataStore(tempDir)
        store.close()

        when: "reading metadata"
        store.getSegments("topic", 0)

        then: "storage exception is thrown"
        thrown(StorageException)
    }

    def "saveSegment round-trips PipeConsistency hash columns"() {
        given:
        def store = new SegmentMetadataStore(tempDir)
        byte[] hash = new byte[16]
        for (int i = 0; i < hash.length; i++) hash[i] = (byte) (i * 7 + 3)
        def md = SegmentMetadata.builder()
                .topic("topic")
                .partition(0)
                .baseOffset(0L)
                .maxOffset(99L)
                .logFilePath("/tmp/s.log")
                .indexFilePath("/tmp/s.index")
                .sizeBytes(2048L)
                .recordCount(100L)
                .createdAt(Instant.now())
                .segmentHash(hash)
                .hashRecordCount(100L)
                .compactionEpoch(0)
                .hashState(SegmentMetadata.HASH_STATE_FINAL)
                .build()

        when:
        store.saveSegment(md)
        def loaded = store.getSegments("topic", 0)

        then:
        loaded.size() == 1
        loaded[0].segmentHash == hash
        loaded[0].hashRecordCount == 100L
        loaded[0].compactionEpoch == 0
        loaded[0].hashState == SegmentMetadata.HASH_STATE_FINAL
        loaded[0].hasFinalHash()
    }

    def "updateSegmentHash mutates only hash columns"() {
        given:
        def store = new SegmentMetadataStore(tempDir)
        store.saveSegment(sampleMetadata())
        byte[] hash = new byte[16]
        hash[0] = 0x42 as byte

        when:
        store.updateSegmentHash("topic", 0, 0L, hash, 11L, 0, SegmentMetadata.HASH_STATE_FINAL)
        def loaded = store.getSegment("topic", 0, 0L)

        then:
        loaded.segmentHash[0] == 0x42 as byte
        loaded.hashState == SegmentMetadata.HASH_STATE_FINAL
        loaded.maxOffset == 10L         // unchanged
        loaded.recordCount == 11L       // unchanged
    }

    def "schema migration adds hash columns to pre-feature DB"() {
        given: "an old-shape database manually created without hash columns"
        def topicDir = tempDir.resolve("legacy")
        java.nio.file.Files.createDirectories(topicDir)
        def dbPath = topicDir.resolve("segment_metadata.db").toString()
        def conn = java.sql.DriverManager.getConnection("jdbc:sqlite:" + dbPath)
        conn.createStatement().with {
            execute("""
                CREATE TABLE segment_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    base_offset BIGINT NOT NULL,
                    max_offset BIGINT NOT NULL,
                    log_file_path TEXT NOT NULL,
                    index_file_path TEXT NOT NULL,
                    size_bytes BIGINT NOT NULL,
                    record_count BIGINT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(topic, partition, base_offset)
                )
            """)
            execute("INSERT INTO segment_metadata(topic, partition, base_offset, max_offset, log_file_path, index_file_path, size_bytes, record_count, created_at, updated_at) VALUES ('t', 0, 0, 10, '/a', '/b', 1024, 11, '${Instant.now()}', '${Instant.now()}')")
        }
        conn.close()

        when: "opening the store triggers ALTER TABLE migration"
        def store = new SegmentMetadataStore(topicDir)
        def loaded = store.getSegments("t", 0)

        then: "pre-existing rows now expose default hash columns"
        loaded.size() == 1
        loaded[0].segmentHash == null
        loaded[0].hashRecordCount == 0L
        loaded[0].compactionEpoch == 0
        loaded[0].hashState == SegmentMetadata.HASH_STATE_PENDING
        !loaded[0].hasFinalHash()
    }

    private static SegmentMetadata sampleMetadata() {
        return SegmentMetadata.builder()
            .topic("topic")
            .partition(0)
            .baseOffset(0L)
            .maxOffset(10L)
            .logFilePath("/tmp/segment.log")
            .indexFilePath("/tmp/segment.index")
            .sizeBytes(1024L)
            .recordCount(11L)
            .createdAt(Instant.now())
            .build()
    }
}
