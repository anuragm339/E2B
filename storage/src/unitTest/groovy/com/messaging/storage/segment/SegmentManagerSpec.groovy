package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStore
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.charset.StandardCharsets
import java.nio.file.Path

class SegmentManagerSpec extends Specification {

    @TempDir
    Path tempDir

    def "readWithSizeLimit respects maxBytes"() {
        given: "a segment manager with records"
        def manager = newManager("size-limit-topic", 10 * 1024 * 1024L)
        def data = "x" * 20
        5.times { i ->
            manager.append(createRecord(i, "key-${i}", data))
        }

        and: "compute record size"
        def recordSize = calculateRecordSize("key-0", data)

        when: "reading with a tight maxBytes limit"
        def records = manager.readWithSizeLimit(0L, 10, (recordSize * 2) + 1)

        then: "only two records are returned"
        records.size() == 2
        records[0].getOffset() == 0L
        records[1].getOffset() == 1L

        cleanup:
        manager?.close()
    }

    def "readWithSizeLimit returns at least one record when maxBytes is too small"() {
        given: "a segment manager with one record"
        def manager = newManager("size-limit-small-topic", 10 * 1024 * 1024L)
        manager.append(createRecord(0L, "key", "data"))
        def recordSize = calculateRecordSize("key", "data")

        when: "reading with maxBytes smaller than one record"
        def records = manager.readWithSizeLimit(0L, 10, recordSize - 1)

        then: "one record is still returned"
        records.size() == 1
        records[0].getOffset() == 0L

        cleanup:
        manager?.close()
    }

    def "readWithSizeLimit crosses segments"() {
        given: "a segment manager that rolls segments"
        def logRecordSize = calculateLogRecordSize("k", "d")
        def maxSize = (LOG_HEADER_SIZE + (logRecordSize * 3)) as long
        def manager = newManager("cross-segment-topic", maxSize)

        and: "append enough records to roll"
        10.times { i ->
            manager.append(createRecord(i, "k", "d"))
        }

        and: "determine highest base offset"
        def highestBase = manager.getAllSegments().collect { it.getBaseOffset() }.max()

        when: "reading with a large size limit"
        def records = manager.readWithSizeLimit(0L, 20, 1024 * 1024)

        then: "records include data from later segment"
        records.size() == 10
        records.last().getOffset() >= highestBase

        cleanup:
        manager?.close()
    }

    def "getZeroCopyBatch reads from active segment at boundary"() {
        given: "a segment manager with a rollover"
        def logRecordSize = calculateLogRecordSize("k", "d")
        def maxSize = (LOG_HEADER_SIZE + (logRecordSize * 3)) as long
        def manager = newManager("zero-copy-boundary-topic", maxSize)

        and: "append enough records to roll"
        10.times { i ->
            manager.append(createRecord(i, "k", "d"))
        }

        and: "find active segment base offset"
        def activeSegment = manager.getAllSegments().find { it.isActive() }
        def activeBase = activeSegment.getBaseOffset()

        when: "requesting zero-copy batch from active base"
        def batch = manager.getZeroCopyBatch(activeBase, 1024 * 1024L)

        then: "batch is returned from active segment"
        batch.recordCount > 0
        batch.lastOffset >= activeBase
        batch.fileRegion != null

        cleanup:
        batch?.fileChannel?.close()
        manager?.close()
    }

    def "getZeroCopyBatch does not span segments"() {
        given: "a segment manager with a rollover"
        def logRecordSize = calculateLogRecordSize("k", "d")
        def maxSize = (LOG_HEADER_SIZE + (logRecordSize * 3)) as long
        def manager = newManager("zero-copy-no-span-topic", maxSize)

        and: "append enough records to roll"
        10.times { i ->
            manager.append(createRecord(i, "k", "d"))
        }

        and: "find base offsets"
        def segments = manager.getAllSegments()
        def activeBase = segments.find { it.isActive() }.getBaseOffset()
        def sealedBase = segments.collect { it.getBaseOffset() }.min()

        when: "reading zero-copy batch from sealed segment"
        def batch = manager.getZeroCopyBatch(sealedBase, 1024 * 1024L)

        then: "batch stays within sealed segment"
        batch.recordCount > 0
        batch.lastOffset < activeBase

        cleanup:
        batch?.fileChannel?.close()
        manager?.close()
    }

    private SegmentManager newManager(String topic, long maxSize) {
        def partitionDir = tempDir.resolve(topic).resolve("partition-0")
        def metadataStore = new SegmentMetadataStore(tempDir.resolve(topic))
        return new SegmentManager(topic, 0, partitionDir, maxSize, metadataStore)
    }

    private MessageRecord createRecord(long offset, String key, String data) {
        def record = new MessageRecord()
        record.setOffset(offset)
        record.setMsgKey(key)
        record.setData(data)
        record.setEventType(EventType.MESSAGE)
        record.setCreatedAt(java.time.Instant.now())
        return record
    }

    private static final int LOG_HEADER_SIZE = 6

    private static int calculateRecordSize(String key, String data) {
        int size = 8 + 4 + key.getBytes(StandardCharsets.UTF_8).length + 1 + 4 + 8 + 4
        if (data != null) {
            size += data.getBytes(StandardCharsets.UTF_8).length
        }
        return size
    }

    private static int calculateLogRecordSize(String key, String data) {
        int size = 4 + key.getBytes(StandardCharsets.UTF_8).length + 1 + 4 + 8
        if (data != null) {
            size += data.getBytes(StandardCharsets.UTF_8).length
        }
        return size
    }
}
