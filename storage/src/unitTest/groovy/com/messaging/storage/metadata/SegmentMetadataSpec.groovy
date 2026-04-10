package com.messaging.storage.metadata

import spock.lang.Specification

import java.time.Instant

class SegmentMetadataSpec extends Specification {

    def "builder populates fields and defaults createdAt when absent"() {
        when:
        def metadata = SegmentMetadata.builder()
            .topic('prices-v1')
            .partition(1)
            .baseOffset(100L)
            .maxOffset(199L)
            .logFilePath('/tmp/100.log')
            .indexFilePath('/tmp/100.index')
            .sizeBytes(2048L)
            .recordCount(100L)
            .build()

        then:
        metadata.topic == 'prices-v1'
        metadata.partition == 1
        metadata.baseOffset == 100L
        metadata.maxOffset == 199L
        metadata.createdAt instanceof Instant
    }
}
