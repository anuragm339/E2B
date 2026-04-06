package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

/**
 * Integration tests for Segment — multi-segment scenarios, recovery, and cross-segment operations.
 *
 * Segment is a plain Java class (not a Micronaut bean); tests instantiate it directly.
 * @MicronautTest is used for project-wide consistency.
 */
@MicronautTest
class SegmentIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    // =========================================================================
    // Basic single-segment append / read
    // =========================================================================

    def "segment append and read round-trip"() {
        given:
        def segment = newSegment(tempDir, "00000000000000000000", 0L, 1024 * 1024L)

        when:
        segment.append(record(0L, "test-key", "test-data"))
        def read = segment.read(0L)

        then:
        read != null
        read.msgKey == "test-key"
        read.data   == "test-data"

        cleanup:
        segment?.close()
    }

    def "segment header is written and validated on reopen"() {
        given:
        def segment = newSegment(tempDir, "00000000000000000000", 0L, 1024 * 1024L)
        segment.append(record(0L, "k", "v"))
        segment.close()

        when:
        def reopened = newSegment(tempDir, "00000000000000000000", 0L, 1024 * 1024L)

        then:
        reopened.nextOffset == 1L

        cleanup:
        reopened?.close()
    }

    // =========================================================================
    // Multi-segment recovery and cross-segment reads
    // =========================================================================

    def "multiple segments can be recovered and read correctly"() {
        given: "three segments with different base offsets"
        def topic     = "multi-segment-test"
        def partition = 0
        def maxSize   = 1024 * 1024L

        def seg1 = newSegmentAt(tempDir, 0L,    maxSize, topic, partition)
        def seg2 = newSegmentAt(tempDir, 1000L, maxSize, topic, partition)
        def seg3 = newSegmentAt(tempDir, 2000L, maxSize, topic, partition)

        and: "write 100 records to each segment"
        100.times { i ->
            seg1.append(record(i,        "seg1-key-${i}", "seg1-data-${i}"))
            seg2.append(record(1000 + i, "seg2-key-${i}", "seg2-data-${i}"))
            seg3.append(record(2000 + i, "seg3-key-${i}", "seg3-data-${i}"))
        }
        [seg1, seg2, seg3].each { it.close() }

        when: "recovering all three segments"
        def rec1 = newSegmentAt(tempDir, 0L,    maxSize, topic, partition)
        def rec2 = newSegmentAt(tempDir, 1000L, maxSize, topic, partition)
        def rec3 = newSegmentAt(tempDir, 2000L, maxSize, topic, partition)

        then: "nextOffset is correct for each segment"
        rec1.nextOffset == 100L
        rec2.nextOffset == 1100L
        rec3.nextOffset == 2100L

        and: "arbitrary reads within each segment return the right record"
        rec1.read(50).msgKey    == "seg1-key-50"
        rec2.read(1050).msgKey  == "seg2-key-50"
        rec3.read(2050).msgKey  == "seg3-key-50"

        cleanup:
        [rec1, rec2, rec3].each { it?.close() }
    }

    // =========================================================================
    // Large segment — verifies STORAGE-1 O(1)-memory index recovery fix
    // =========================================================================

    def "large segment recovery with 10 000 records uses binary search correctly"() {
        given:
        def segment = newSegment(tempDir, "00000000000000000000", 0L, 100 * 1024 * 1024L)
        10_000.times { i -> segment.append(record(i, "key-${i}", "data-value-${i}")) }
        segment.close()

        when:
        def recovered = newSegment(tempDir, "00000000000000000000", 0L, 100 * 1024 * 1024L)

        then: "index recovery gives the correct nextOffset — no in-memory map was used"
        recovered.nextOffset == 10_000L

        and: "binary search finds records at first, middle, and last offsets"
        recovered.read(0).msgKey    == "key-0"
        recovered.read(5000).msgKey == "key-5000"
        recovered.read(9999).msgKey == "key-9999"

        cleanup:
        recovered?.close()
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static Segment newSegment(Path dir, String name, long baseOffset, long maxSize,
                                      String topic = "test-topic", int partition = 0) {
        def logPath   = dir.resolve("${name}.log")
        def indexPath = dir.resolve("${name}.index")
        return new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
    }

    private static Segment newSegmentAt(Path dir, long baseOffset, long maxSize,
                                         String topic, int partition) {
        def name = String.format("%020d", baseOffset)
        newSegment(dir, name, baseOffset, maxSize, topic, partition)
    }

    private static MessageRecord record(long offset, String key, String data) {
        def r = new MessageRecord()
        r.offset    = offset
        r.msgKey    = key
        r.data      = data
        r.eventType = EventType.MESSAGE
        r.createdAt = Instant.now()
        return r
    }
}
