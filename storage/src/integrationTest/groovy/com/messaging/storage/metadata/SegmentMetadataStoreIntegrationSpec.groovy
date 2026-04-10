package com.messaging.storage.metadata

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

@MicronautTest(startApplication = false)
class SegmentMetadataStoreIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    def "save get maxOffset and delete work against the sqlite metadata store"() {
        given:
        def store = new SegmentMetadataStore(tempDir.resolve('prices-v1'))
        def first = metadata(0L, 9L)
        def second = metadata(10L, 19L)

        when:
        store.saveSegment(first)
        store.saveSegment(second)

        then:
        store.getSegments('prices-v1', 0)*.baseOffset == [0L, 10L]
        store.getMaxOffset('prices-v1', 0) == 19L

        when:
        store.deleteSegment('prices-v1', 0, 0L)

        then:
        store.getSegments('prices-v1', 0)*.baseOffset == [10L]

        cleanup:
        store?.close()
    }

    private static SegmentMetadata metadata(long baseOffset, long maxOffset) {
        SegmentMetadata.builder()
            .topic('prices-v1')
            .partition(0)
            .baseOffset(baseOffset)
            .maxOffset(maxOffset)
            .logFilePath("/tmp/${baseOffset}.log")
            .indexFilePath("/tmp/${baseOffset}.index")
            .sizeBytes(1024L)
            .recordCount(10L)
            .createdAt(Instant.parse('2024-01-01T00:00:00Z'))
            .build()
    }
}
