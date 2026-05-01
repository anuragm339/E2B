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

    def "getBatch reads from active segment at boundary"() {
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

        when: "requesting batch from active base"
        def batch = manager.getBatch(activeBase, 1024 * 1024L)

        then: "batch is returned from active segment"
        batch.getRecordCount() > 0
        batch.getLastOffset() >= activeBase
        !batch.isEmpty()

        cleanup:
        batch?.close()
        manager?.close()
    }

    def "getBatch does not span segments"() {
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

        when: "reading batch from sealed segment"
        def batch = manager.getBatch(sealedBase, 1024 * 1024L)

        then: "batch stays within sealed segment"
        batch.getRecordCount() > 0
        batch.getLastOffset() < activeBase

        cleanup:
        batch?.close()
        manager?.close()
    }

    def "readWithSizeLimit returns all records across offset gaps"() {
        given: "a segment manager with records at widely-spaced offsets (1, 100, 1000)"
        def manager = newManager("gap-topic", 10 * 1024 * 1024L)
        manager.append(createRecord(1L,    "sparse-1",    "d1"))
        manager.append(createRecord(100L,  "sparse-100",  "d100"))
        manager.append(createRecord(1000L, "sparse-1000", "d1000"))

        when: "reading from offset 0 — all three records should be returned despite the gaps"
        def records = manager.read(0L, 10)

        then: "all three records are returned, in offset order"
        records.size() == 3
        records[0].getOffset() == 1L
        records[0].getMsgKey() == "sparse-1"
        records[1].getOffset() == 100L
        records[1].getMsgKey() == "sparse-100"
        records[2].getOffset() == 1000L
        records[2].getMsgKey() == "sparse-1000"

        when: "reading from an offset that falls inside a gap (e.g. offset 2)"
        def fromGap = manager.read(2L, 10)

        then: "records after the gap (100 and 1000) are returned — gap is skipped"
        fromGap.size() == 2
        fromGap[0].getOffset() == 100L
        fromGap[1].getOffset() == 1000L

        when: "reading from the exact offset of the last gapped record"
        def fromLast = manager.read(1000L, 10)

        then: "only the last record is returned"
        fromLast.size() == 1
        fromLast[0].getOffset() == 1000L

        cleanup:
        manager?.close()
    }

    // ── Edge cases and missing branches ─────────────────────────────────────

    def "getCurrentOffset returns -1 when manager has no segments"() {
        // Branch: active == null and segments.isEmpty() → return -1
        given: "a fresh segment manager with no data"
        def manager = newManager("empty-offset-topic", 10 * 1024 * 1024L)

        expect:
        manager.getCurrentOffset() == -1L

        cleanup:
        manager?.close()
    }

    def "getEarliestOffset returns 0 when no segments exist"() {
        // Branch: segments.isEmpty() → return 0
        given:
        def manager = newManager("empty-earliest-topic", 10 * 1024 * 1024L)

        expect:
        manager.getEarliestOffset() == 0L

        cleanup:
        manager?.close()
    }

    def "getBatch returns empty batch when fromOffset is beyond storage head"() {
        // Branch: fromOffset > storageHead → return empty BatchFileRegion
        given:
        def manager = newManager("beyond-head-topic", 10 * 1024 * 1024L)
        manager.append(createRecord(0L, "key", "data"))

        when:
        def batch = manager.getBatch(9999L, 1024 * 1024L)

        then:
        batch.getRecordCount() == 0

        cleanup:
        batch?.close()
        manager?.close()
    }

    def "getBatch resets fromOffset to earliestBase when consumer is behind compacted data"() {
        // Branch: fromOffset < earliestBase → log warning and reset fromOffset to earliestBase,
        // then continue delivering from that position (does NOT return empty).
        given:
        def logRecordSize = calculateLogRecordSize("k", "d")
        def maxSize = (LOG_HEADER_SIZE + (logRecordSize * 3)) as long
        def manager = newManager("before-earliest-topic", maxSize)

        and: "force rollover so the first segment base offset is > 0"
        10.times { i -> manager.append(createRecord((long) i, "k", "d")) }
        def earliestBase = manager.getEarliestOffset()

        when: "request a batch from offset = -1, which is below earliestBase"
        def batch = manager.getBatch(-1L, 1024 * 1024L)

        then: "batch starts from earliestBase (not -1), data is returned"
        batch.getFirstOffset() >= earliestBase   // reset to earliest, not empty
        batch.getRecordCount() > 0

        cleanup:
        batch?.close()
        manager?.close()
    }

    def "read returns empty list when fromOffset is beyond all stored data"() {
        // Branch: active != null && fromOffset >= active.getNextOffset() → "No data at or after this offset"
        given:
        def manager = newManager("beyond-data-topic", 10 * 1024 * 1024L)
        manager.append(createRecord(0L, "only-key", "data"))

        when:
        def records = manager.read(9999L, 10)

        then:
        records.isEmpty()

        cleanup:
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
