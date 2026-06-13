package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStore
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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

    def "sparse-offset pipe records roll correctly across segment boundaries"() {
        given: "a segment that holds exactly 1 record, simulating a near-full segment"
        def key  = "sparse-key"
        def data = "sparse-data"
        def logRecordSize = calculateLogRecordSize(key, data)
        def maxSize = (LOG_HEADER_SIZE + logRecordSize) as long
        def manager = newManager("sparse-offset-topic", maxSize)

        when: "three records with large sparse offsets arrive (pipe-style pre-assigned offsets)"
        manager.append(createRecord(1000L,    key, data))
        manager.append(createRecord(100000L,  key, data))
        manager.append(createRecord(3000000L, key, data))

        then: "no exception — all three stored without corruption"
        noExceptionThrown()

        and: "each record is readable at its exact original offset"
        manager.read(1000L,    1).any { it.offset == 1000L    }
        manager.read(100000L,  1).any { it.offset == 100000L  }
        manager.read(3000000L, 1).any { it.offset == 3000000L }

        and: "three distinct segment files exist (one record per segment)"
        def segDir = tempDir.resolve("sparse-offset-topic").resolve("partition-0")
        segDir.toFile().listFiles({ f -> f.name.endsWith(".log") } as java.io.FileFilter).length == 3

        and: "the manager's current offset is the last written record offset"
        manager.getCurrentOffset() == 3000000L

        cleanup:
        manager?.close()
    }

    def "concurrent auto-offset appends allocate unique sequential offsets without mutating inputs"() {
        given:
        int recordCount = 200
        def manager = newManager("concurrent-append-topic", 10 * 1024 * 1024L)
        def records = (0..<recordCount).collect { i ->
            createRecord(0L, "key-${i}", "data-${i}")
        }
        def executor = Executors.newFixedThreadPool(8)
        def start = new CountDownLatch(1)

        when:
        def futures = records.collect { record ->
            executor.submit({
                start.await()
                manager.append(record)
            } as java.util.concurrent.Callable<Long>)
        }
        start.countDown()
        def offsets = futures.collect { it.get(10, TimeUnit.SECONDS) }
        def expectedOffsets = (0..<recordCount).collect { it as long }

        then:
        offsets.toSet().size() == recordCount
        offsets.sort() == expectedOffsets
        records.every { it.offset == 0L }
        manager.read(0L, recordCount)*.offset == expectedOffsets

        cleanup:
        executor?.shutdownNow()
        manager?.close()
    }

    def "restart never exposes the re-activated segment to compaction"() {
        given: 'a manager with data, closed cleanly (simulating a broker restart)'
        def manager = newManager('restart-topic', 10 * 1024 * 1024L)
        3.times { i -> manager.append(createRecord(i, "key-$i", 'data')) }
        manager.close()

        when: 'the manager is reconstructed from disk'
        def reopened = newManager('restart-topic', 10 * 1024 * 1024L)

        then: 'the last segment is active again and is NOT offered to the compaction planner'
        reopened.getActiveSegment() != null
        reopened.getActiveSegment().getBaseOffset() == 0L
        reopened.getInactiveSegments().isEmpty()

        and: 'appends still work after reopen'
        reopened.append(createRecord(3L, 'key-3', 'data')) == 3L

        when: 'compaction (incorrectly) tries to replace the active segment — the bug that bricked ingest'
        reopened.replaceSegments([reopened.getActiveSegment()], reopened.getActiveSegment())

        then: 'it is refused outright instead of sealing+deleting the live write target'
        def e = thrown(com.messaging.common.exception.StorageException)
        e.message.contains('Cannot replace active segment')

        cleanup:
        reopened?.close()
    }

    def "force-rolling an empty active segment is a no-op instead of creating a same-file twin"() {
        given: 'a fresh manager whose active segment holds only the 6-byte file header'
        def manager = newManager('empty-roll-topic', 10 * 1024 * 1024L)

        when: 'force-roll is invoked repeatedly (e.g. compaction trigger on an idle topic)'
        3.times { manager.forceRollActiveSegment() }

        then: 'nothing rolled — no sealed twin sharing the active file path'
        manager.getInactiveSegments().isEmpty()
        manager.getAllSegments().size() == 1

        when: 'data arrives and force-roll runs again'
        manager.append(createRecord(0L, 'k', 'd'))
        manager.forceRollActiveSegment()

        then: 'a real roll happens and the new active segment has a DIFFERENT base'
        manager.getInactiveSegments().size() == 1
        manager.getActiveSegment().getBaseOffset() != manager.getInactiveSegments()[0].getBaseOffset()

        cleanup:
        manager?.close()
    }

    def "same-base compaction replacement never deletes the replacement's file or metadata"() {
        given: 'a manager with one sealed segment at base 0'
        def manager = newManager('samebase-topic', 10 * 1024 * 1024L)
        manager.append(createRecord(0L, 'k0', 'd0'))
        manager.append(createRecord(1L, 'k1', 'd1'))
        manager.forceRollActiveSegment()
        def sealed = manager.getInactiveSegments()[0]

        and: 'a compacted replacement at the SAME base on the .compacted path (first compaction)'
        def dir = sealed.getLogPath().getParent()
        def compacted = new Segment(dir.resolve('00000000000000000000.compacted.log'),
                dir.resolve('00000000000000000000.compacted.index'), 0L, 10 * 1024 * 1024L, 'samebase-topic', 0)
        compacted.append(createRecord(1L, 'k1', 'd1'))
        compacted.seal()

        when:
        manager.replaceSegments([sealed], compacted)

        then: 'old plain files deleted, compacted file intact, metadata row for base 0 survives'
        !java.nio.file.Files.exists(dir.resolve('00000000000000000000.log'))
        java.nio.file.Files.exists(dir.resolve('00000000000000000000.compacted.log'))
        manager.getInactiveSegments()[0].is(compacted)

        when: 'a RE-compaction of the unadvanced window replaces same base AND same path'
        def recompacted = new Segment(dir.resolve('00000000000000000000.compacted.log'),
                dir.resolve('00000000000000000000.compacted.index'), 0L, 10 * 1024 * 1024L, 'samebase-topic', 0)
        recompacted.seal()
        manager.replaceSegments([compacted], recompacted)

        then: 'the shared file is NOT unlinked — the bug deleted the just-installed segment'
        java.nio.file.Files.exists(dir.resolve('00000000000000000000.compacted.log'))
        manager.getInactiveSegments()[0].is(recompacted)

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
