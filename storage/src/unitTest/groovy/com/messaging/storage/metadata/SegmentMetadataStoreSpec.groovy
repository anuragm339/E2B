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

    def "saveSegment round-trips segment metadata"() {
        given:
        def store = new SegmentMetadataStore(tempDir)
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
                .build()

        when:
        store.saveSegment(md)
        def loaded = store.getSegments("topic", 0)

        then:
        loaded.size() == 1
        loaded[0].topic == "topic"
        loaded[0].partition == 0
        loaded[0].baseOffset == 0L
        loaded[0].maxOffset == 99L
        loaded[0].recordCount == 100L

        cleanup:
        store?.close()
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
