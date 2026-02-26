package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

/**
 * Integration tests for Segment - multi-segment scenarios, recovery, and cross-segment operations
 */
class SegmentIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    def "integration test setup verification"() {
        given: "a simple segment"
        def logPath = tempDir.resolve("00000000000000000000.log")
        def indexPath = tempDir.resolve("00000000000000000000.index")
        def baseOffset = 0L
        def maxSize = 1024 * 1024L // 1MB
        def topic = "integration-test-topic"
        def partition = 0

        when: "creating segment and writing a record"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        def record = createTestRecord(0L, "test-key", "test-data")
        segment.append(record)
        def readRecord = segment.read(0L)

        then: "record can be read back"
        readRecord != null
        readRecord.getMsgKey() == "test-key"
        readRecord.getData() == "test-data"

        cleanup:
        segment?.close()
    }

    def "multiple segments can be recovered and read correctly"() {
        given: "three segments with different base offsets"
        def topic = "multi-segment-test"
        def partition = 0
        def maxSize = 1024 * 1024L

        def segment1 = new Segment(
            tempDir.resolve("00000000000000000000.log"),
            tempDir.resolve("00000000000000000000.index"),
            0L, maxSize, topic, partition
        )
        def segment2 = new Segment(
            tempDir.resolve("00000000000000001000.log"),
            tempDir.resolve("00000000000000001000.index"),
            1000L, maxSize, topic, partition
        )
        def segment3 = new Segment(
            tempDir.resolve("00000000000000002000.log"),
            tempDir.resolve("00000000000000002000.index"),
            2000L, maxSize, topic, partition
        )

        and: "write 100 records to each segment"
        100.times { i ->
            segment1.append(createTestRecord(i, "seg1-key-${i}", "seg1-data-${i}"))
            segment2.append(createTestRecord(1000 + i, "seg2-key-${i}", "seg2-data-${i}"))
            segment3.append(createTestRecord(2000 + i, "seg3-key-${i}", "seg3-data-${i}"))
        }

        and: "close all segments"
        segment1.close()
        segment2.close()
        segment3.close()

        when: "recovering all segments"
        def recovered1 = new Segment(
            tempDir.resolve("00000000000000000000.log"),
            tempDir.resolve("00000000000000000000.index"),
            0L, maxSize, topic, partition
        )
        def recovered2 = new Segment(
            tempDir.resolve("00000000000000001000.log"),
            tempDir.resolve("00000000000000001000.index"),
            1000L, maxSize, topic, partition
        )
        def recovered3 = new Segment(
            tempDir.resolve("00000000000000002000.log"),
            tempDir.resolve("00000000000000002000.index"),
            2000L, maxSize, topic, partition
        )

        then: "all segments have correct nextOffset"
        recovered1.getNextOffset() == 100
        recovered2.getNextOffset() == 1100
        recovered3.getNextOffset() == 2100

        and: "can read records from all segments"
        def rec1 = recovered1.read(50)
        rec1 != null
        rec1.getMsgKey() == "seg1-key-50"

        def rec2 = recovered2.read(1050)
        rec2 != null
        rec2.getMsgKey() == "seg2-key-50"

        def rec3 = recovered3.read(2050)
        rec3 != null
        rec3.getMsgKey() == "seg3-key-50"

        cleanup:
        recovered1?.close()
        recovered2?.close()
        recovered3?.close()
    }

    def "large segment recovery with 10000 records verifies STORAGE-1 memory fix"() {
        given: "a segment that will contain many records"
        def logPath = tempDir.resolve("00000000000000000000.log")
        def indexPath = tempDir.resolve("00000000000000000000.index")
        def baseOffset = 0L
        def maxSize = 100 * 1024 * 1024L // 100MB
        def topic = "large-segment-test"
        def partition = 0

        and: "write 10000 records"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        10000.times { i ->
            def record = createTestRecord(i, "key-${i}", "data-value-${i}")
            segment.append(record)
        }
        segment.close()

        when: "recovering segment with many records"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "nextOffset is correct (no memory leak from index map)"
        recovered.getNextOffset() == 10000

        and: "random reads work correctly using binary search"
        def mid = recovered.read(5000)
        mid != null
        mid.getMsgKey() == "key-5000"

        def last = recovered.read(9999)
        last != null
        last.getMsgKey() == "key-9999"

        def first = recovered.read(0)
        first != null
        first.getMsgKey() == "key-0"

        cleanup:
        recovered?.close()
    }

    // Helper method to create test records
    private MessageRecord createTestRecord(long offset, String key, String data) {
        def record = new MessageRecord()
        record.setOffset(offset)
        record.setMsgKey(key)
        record.setData(data)
        record.setEventType(EventType.MESSAGE)
        record.setCreatedAt(Instant.now())
        return record
    }
}
