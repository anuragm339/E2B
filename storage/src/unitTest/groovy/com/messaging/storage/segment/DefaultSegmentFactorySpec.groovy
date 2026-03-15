package com.messaging.storage.segment

import com.messaging.storage.segment.SegmentFactory
import com.messaging.storage.metadata.SegmentMetadataStore
import com.messaging.storage.segment.Segment
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class DefaultSegmentFactorySpec extends Specification {

    @TempDir
    Path tempDir

    SegmentFactory segmentFactory
    SegmentMetadataStore metadataStore

    def setup() {
        metadataStore = Mock(SegmentMetadataStore)
        segmentFactory = new DefaultSegmentFactory()
    }

    def "should generate segment filename with 20-digit zero-padded offset"() {
        expect:
        segmentFactory.generateSegmentFilename(0L) == "00000000000000000000.log"
        segmentFactory.generateSegmentFilename(100L) == "00000000000000000100.log"
        segmentFactory.generateSegmentFilename(12345678900L) == "00000000012345678900.log"
        segmentFactory.generateSegmentFilename(Long.MAX_VALUE) == "09223372036854775807.log"
    }

    def "should create segment with correct paths"() {
        when:
        def segment = segmentFactory.createSegment(tempDir, "test-topic", 0, 100L, metadataStore)

        then:
        segment != null
        segment.baseOffset == 100L
        segment.logPath.toString().endsWith("00000000000000000100.log")
        segment.indexPath.toString().endsWith("00000000000000000100.index")
        segment.topic == "test-topic"
        segment.partition == 0
    }

    def "should create segment at offset zero"() {
        when:
        def segment = segmentFactory.createSegment(tempDir, "prices-v1", 0, 0L, metadataStore)

        then:
        segment != null
        segment.baseOffset == 0L
        segment.logPath.toString().endsWith("00000000000000000000.log")
        segment.topic == "prices-v1"
    }

    def "should create segments for different partitions"() {
        when:
        def segment0 = segmentFactory.createSegment(tempDir, "test-topic", 0, 100L, metadataStore)
        def segment1 = segmentFactory.createSegment(tempDir, "test-topic", 1, 100L, metadataStore)

        then:
        segment0.partition == 0
        segment1.partition == 1
        // Both use the same filename pattern (partitions use different directories)
        segment0.logPath.fileName.toString() == segment1.logPath.fileName.toString()
    }

    def "should create segments with different offsets"() {
        when:
        def segment1 = segmentFactory.createSegment(tempDir, "test-topic", 0, 0L, metadataStore)
        def segment2 = segmentFactory.createSegment(tempDir, "test-topic", 0, 1000L, metadataStore)
        def segment3 = segmentFactory.createSegment(tempDir, "test-topic", 0, 2000L, metadataStore)

        then:
        segment1.logPath.fileName.toString() == "00000000000000000000.log"
        segment2.logPath.fileName.toString() == "00000000000000001000.log"
        segment3.logPath.fileName.toString() == "00000000000000002000.log"
    }
}
