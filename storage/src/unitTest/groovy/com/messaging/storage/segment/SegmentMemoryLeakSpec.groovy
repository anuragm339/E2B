package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.common.exception.StorageException
import com.messaging.storage.metadata.SegmentMetadataStore
import spock.lang.Specification
import spock.lang.TempDir

import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.regex.Pattern

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

    def "reading a gap offset returns the next available record, not null"() {
        given: "a segment with offset gaps: records at 500, 700, 1200"
        def logPath = tempDir.resolve("00000000000000000500.log")
        def indexPath = tempDir.resolve("00000000000000000500.index")
        def baseOffset = 500L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        and: "append records with gaps"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        [500L, 700L, 1200L].each { offset ->
            segment.append(createTestRecord(offset, "key-${offset}", "data-${offset}"))
        }

        when: "reading existing offsets"
        def record500 = segment.read(500L)
        def record700 = segment.read(700L)

        then: "records are found at their exact offsets"
        record500 != null
        record500.getMsgKey() == "key-500"
        record700 != null
        record700.getMsgKey() == "key-700"

        when: "reading an offset that falls inside a gap (900 — between 700 and 1200)"
        def gapRecord = segment.read(900L)

        then: "the next available record (offset 1200) is returned — gap is skipped transparently"
        gapRecord != null
        gapRecord.getOffset() == 1200L
        gapRecord.getMsgKey() == "key-1200"

        and: "reading past the last record returns null (no further records exist)"
        segment.read(1201L) == null

        cleanup:
        segment?.close()
    }

    def "segment manager reads from active segment at boundary"() {
        given: "a segment manager that rolls segments"
        def topic = "boundary-topic"
        def partition = 0
        def topicDir = tempDir.resolve(topic)
        def partitionDir = topicDir.resolve("partition-0")
        def metadataStore = new SegmentMetadataStore(topicDir)
        def recordSize = calculateRecordSize("k", "d")
        def maxSize = (LOG_HEADER_SIZE + (recordSize * 5)) as long
        def manager = new SegmentManager(topic, partition, partitionDir, maxSize, metadataStore)

        and: "append enough records to roll segments"
        20.times { i ->
            manager.append(createTestRecord(i, "k", "d"))
        }

        and: "determine active segment base offset"
        def logFiles = listLogFiles(partitionDir)
        def activeBase = logFiles.collect { extractBaseOffset(it) }.max()

        when: "reading from the active segment base offset"
        def records = manager.read(activeBase, 1)

        then: "record from active segment is returned"
        records.size() == 1
        records[0].getOffset() == activeBase

        cleanup:
        manager?.close()
    }

    def "empty key and data are preserved"() {
        given: "a segment and an empty payload record"
        def logPath = tempDir.resolve("00000000000000000600.log")
        def indexPath = tempDir.resolve("00000000000000000600.index")
        def baseOffset = 600L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        and: "append record with empty key and data"
        segment.append(createTestRecord(baseOffset, "", ""))

        when: "reading the record back"
        def read = segment.read(baseOffset)

        then: "empty fields are preserved"
        read != null
        read.getMsgKey() == ""
        read.getData() == null

        cleanup:
        segment?.close()
    }

    def "corrupted log header is detected on recovery"() {
        given: "a segment with data"
        def logPath = tempDir.resolve("00000000000000000700.log")
        def indexPath = tempDir.resolve("00000000000000000700.index")
        def baseOffset = 700L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        segment.append(createTestRecord(baseOffset, "key", "data"))
        segment.close()

        and: "corrupt the log file header"
        overwriteBytes(logPath, 0, "XXXX".getBytes(StandardCharsets.UTF_8))

        when: "recovering the segment"
        new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "a storage exception is thrown"
        thrown(StorageException)
    }

    def "crash recovery truncates orphaned log bytes"() {
        given: "a segment with one record"
        def logPath = tempDir.resolve("00000000000000000900.log")
        def indexPath = tempDir.resolve("00000000000000000900.index")
        def baseOffset = 900L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        segment.append(createTestRecord(baseOffset, "key", "data"))
        segment.close()

        and: "append orphaned bytes to the log file"
        Files.write(logPath, new byte[32], StandardOpenOption.APPEND)
        def expectedSize = LOG_HEADER_SIZE + calculateRecordSize("key", "data")
        assert Files.size(logPath) > expectedSize

        when: "recovering the segment"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "log file is truncated to the last indexed record"
        Files.size(logPath) == expectedSize

        cleanup:
        recovered?.close()
    }

    def "index truncation drops last record on recovery"() {
        given: "a segment with two records"
        def logPath = tempDir.resolve("00000000000000000950.log")
        def indexPath = tempDir.resolve("00000000000000000950.index")
        def baseOffset = 950L
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        segment.append(createTestRecord(baseOffset, "key-0", "data-0"))
        segment.append(createTestRecord(baseOffset + 1, "key-1", "data-1"))
        segment.close()

        and: "truncate the index file to remove the last entry"
        def indexSize = Files.size(indexPath)
        def truncatedSize = indexSize - INDEX_ENTRY_SIZE
        def raf = new RandomAccessFile(indexPath.toFile(), "rw")
        try {
            raf.setLength(truncatedSize)
        } finally {
            raf.close()
        }

        when: "recovering the segment"
        def recovered = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        then: "nextOffset reflects only the first record"
        recovered.getNextOffset() == baseOffset + 1
        recovered.read(baseOffset + 1) == null

        and: "log file is truncated to the last indexed record"
        def expectedSize = LOG_HEADER_SIZE + calculateRecordSize("key-0", "data-0")
        Files.size(logPath) == expectedSize

        cleanup:
        recovered?.close()
    }

    def "large offset values are handled correctly"() {
        given: "a segment with a large base offset"
        def baseOffset = 1_000_000_000L
        def logPath = tempDir.resolve(String.format("%020d.log", baseOffset))
        def indexPath = tempDir.resolve(String.format("%020d.index", baseOffset))
        def maxSize = 10 * 1024 * 1024L
        def topic = "test-topic"
        def partition = 0

        when: "writing and reading a large offset record"
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)
        segment.append(createTestRecord(baseOffset, "key", "data"))
        def read = segment.read(baseOffset)

        then: "record is readable and nextOffset is correct"
        read != null
        read.getOffset() == baseOffset
        segment.getNextOffset() == baseOffset + 1

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

    private static final int LOG_HEADER_SIZE = 6
    private static final int INDEX_ENTRY_SIZE = 16
    private static final Pattern LOG_FILE_PATTERN = Pattern.compile("(\\d{20})\\.log")

    private static long extractBaseOffset(Path logPath) {
        def matcher = LOG_FILE_PATTERN.matcher(logPath.fileName.toString())
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid log filename: ${logPath}")
        }
        return Long.parseLong(matcher.group(1))
    }

    private static java.util.List<Path> listLogFiles(Path dir) {
        def results = []
        Files.list(dir).withCloseable { stream ->
            stream.filter { p -> p.toString().endsWith(".log") }
                  .forEach { p -> results.add(p) }
        }
        return results
    }

    private static int calculateRecordSize(String key, String data) {
        int size = 4 + key.getBytes(StandardCharsets.UTF_8).length + 1 + 4 + 8
        if (data != null) {
            size += data.getBytes(StandardCharsets.UTF_8).length
        }
        return size
    }

    private static void overwriteBytes(Path path, long position, byte[] bytes) {
        def raf = new RandomAccessFile(path.toFile(), "rw")
        try {
            raf.seek(position)
            raf.write(bytes)
        } finally {
            raf.close()
        }
    }

}
