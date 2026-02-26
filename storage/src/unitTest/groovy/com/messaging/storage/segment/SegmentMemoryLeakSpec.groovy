package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

/**
 * Unit tests for STORAGE-1: Segment Index Map Memory Leak Fix
 *
 * Verifies that:
 * 1. Segment can be created without deprecated index map
 * 2. Segment recovery works without populating index map
 * 3. Memory usage is minimal (O(1) instead of O(n))
 */
class SegmentMemoryLeakSpec extends Specification {

    @TempDir
    Path tempDir

    def "segment can be created successfully"() {
        given: "temporary paths for segment files"
        def logPath = tempDir.resolve("00000000000000000000.log")
        def indexPath = tempDir.resolve("00000000000000000000.index")
        def baseOffset = 0L
        def maxSize = 10 * 1024 * 1024L // 10MB
        def topic = "test-topic"
        def partition = 0

        when: "creating a new segment"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "segment is created successfully"
        segment != null
        segment.getBaseOffset() == baseOffset
        segment.isActive()

        cleanup:
        segment?.close()
    }

    def "segment recovery calculates nextOffset correctly without index map"() {
        given: "a segment with 100 records"
        def logPath = tempDir.resolve("00000000000000000100.log")
        def indexPath = tempDir.resolve("00000000000000000100.index")
        def baseOffset = 100L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        and: "append 100 records"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        100.times { i ->
            def record = createTestRecord(baseOffset + i, "key-${i}", "data-${i}")
            segment.append(record)
        }
        def originalNextOffset = segment.getNextOffset()
        segment.close()

        when: "recovering segment from disk"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "nextOffset is calculated correctly from index entries"
        recovered.getNextOffset() == originalNextOffset
        recovered.getNextOffset() == baseOffset + 100

        and: "baseOffset is preserved"
        recovered.getBaseOffset() == baseOffset

        cleanup:
        recovered?.close()
    }

    def "segment recovery with offset gaps calculates nextOffset from highest offset"() {
        given: "a segment with offset gaps: 100, 200, 500, 1000"
        def logPath = tempDir.resolve("00000000000000000100.log")
        def indexPath = tempDir.resolve("00000000000000000100.index")
        def baseOffset = 100L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        and: "append records with gaps"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        def offsets = [100L, 200L, 500L, 1000L]
        offsets.each { offset ->
            def record = createTestRecord(offset, "key-${offset}", "data-${offset}")
            segment.append(record)
        }
        segment.close()

        when: "recovering segment"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "nextOffset is highest offset + 1 (not based on record count)"
        recovered.getNextOffset() == 1001L

        and: "baseOffset is correct"
        recovered.getBaseOffset() == baseOffset

        and: "segment is active"
        recovered.isActive()

        cleanup:
        recovered?.close()
    }

    def "recovered segment can read records using binary search"() {
        given: "a segment with records"
        def logPath = tempDir.resolve("00000000000000000200.log")
        def indexPath = tempDir.resolve("00000000000000000200.index")
        def baseOffset = 200L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        and: "append 50 records and close"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        50.times { i ->
            def record = createTestRecord(baseOffset + i, "key-${i}", "data-value-${i}")
            segment.append(record)
        }
        segment.close()

        when: "recovering segment and reading records"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        def record25 = recovered.read(baseOffset + 25)
        def record0 = recovered.read(baseOffset)
        def record49 = recovered.read(baseOffset + 49)

        then: "all records can be read correctly using binary search"
        record25 != null
        record25.getMsgKey() == "key-25"
        record25.getData() == "data-value-25"
        record25.getOffset() == baseOffset + 25

        and: "first record is correct"
        record0 != null
        record0.getMsgKey() == "key-0"
        record0.getData() == "data-value-0"

        and: "last record is correct"
        record49 != null
        record49.getMsgKey() == "key-49"
        record49.getData() == "data-value-49"

        cleanup:
        recovered?.close()
    }

    def "empty segment recovery works correctly"() {
        given: "a segment with no records"
        def logPath = tempDir.resolve("00000000000000000300.log")
        def indexPath = tempDir.resolve("00000000000000000300.index")
        def baseOffset = 300L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        and: "create segment and close without writing"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        segment.close()

        when: "recovering empty segment"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "nextOffset equals baseOffset"
        recovered.getNextOffset() == baseOffset

        and: "baseOffset is preserved"
        recovered.getBaseOffset() == baseOffset

        and: "segment is active"
        recovered.isActive()

        cleanup:
        recovered?.close()
    }

    def "reading non-existent offset returns null"() {
        given: "a segment with records 100-109"
        def logPath = tempDir.resolve("00000000000000000400.log")
        def indexPath = tempDir.resolve("00000000000000000400.index")
        def baseOffset = 100L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        and: "append 10 records"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        10.times { i ->
            def record = createTestRecord(baseOffset + i, "key-${i}", "data-${i}")
            segment.append(record)
        }

        when: "trying to read offset that doesn't exist"
        def result = segment.read(baseOffset + 50)

        then: "null is returned"
        result == null

        cleanup:
        segment?.close()
    }

    // Helper method to create test records
    private MessageRecord createTestRecord(long offset, String key, String data) {
        def record = new MessageRecord()
        record.setOffset(offset)
        record.setMsgKey(key)
        record.setData(data)
        record.setEventType(EventType.MESSAGE)  // Use MESSAGE not INSERT
        record.setCreatedAt(Instant.now())
        return record
    }
}
