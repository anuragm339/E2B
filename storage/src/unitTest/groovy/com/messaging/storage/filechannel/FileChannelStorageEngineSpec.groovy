package com.messaging.storage.filechannel

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStoreFactory
import com.messaging.storage.watermark.StorageWatermarkTracker
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.regex.Pattern

class FileChannelStorageEngineSpec extends Specification {

    @TempDir
    Path tempDir

    def "append and read roundtrip"() {
        given: "a file channel storage engine"
        def engine = newEngine(tempDir, 1024 * 1024L)
        def topic = "roundtrip-topic"

        when: "appending a record and reading it back"
        def record = createRecord(0L, "key", "data")
        def offset = engine.append(topic, 0, record)
        def read = engine.read(topic, 0, 0L, 10)

        then: "the record is returned intact"
        offset == 0L
        read.size() == 1
        read[0].getMsgKey() == "key"
        read[0].getData() == "data"

        cleanup:
        engine?.close()
    }

    def "segment rollover creates multiple segments and reads across them"() {
        given: "a small segment size to force rollover"
        def recordSize = calculateRecordSize("k", "d")
        def maxSegmentSize = (LOG_HEADER_SIZE + (recordSize * 5)) as long
        def engine = newEngine(tempDir, maxSegmentSize)
        def topic = "rollover-topic"

        and: "append enough records to roll segments"
        int total = 15
        total.times { i ->
            engine.append(topic, 0, createRecord(i, "k", "d"))
        }

        when: "listing log files in the partition directory"
        def partitionDir = tempDir.resolve(topic).resolve("partition-0")
        def logFiles = listLogFiles(partitionDir)

        then: "multiple segments are created"
        logFiles.size() > 1

        when: "reading from the beginning"
        def records = engine.read(topic, 0, 0L, total)

        then: "all records are returned in order"
        records.size() == total
        records.first().getOffset() == 0L
        records.last().getOffset() == total - 1

        cleanup:
        engine?.close()
    }

    def "recovery loads existing segments"() {
        given: "an engine with persisted data"
        def engine = newEngine(tempDir, 1024 * 1024L)
        def topic = "recovery-topic"
        5.times { i ->
            engine.append(topic, 0, createRecord(i, "key-${i}", "data-${i}"))
        }
        engine.close()

        when: "creating a new engine and recovering"
        def recovered = newEngine(tempDir, 1024 * 1024L)
        recovered.recover()
        def records = recovered.read(topic, 0, 0L, 10)

        then: "records are readable after recovery"
        records.size() == 5
        records[0].getMsgKey() == "key-0"
        records[4].getMsgKey() == "key-4"

        cleanup:
        recovered?.close()
    }

    def "earliest offset reflects remaining segments after deletion"() {
        given: "multiple segments on disk"
        def recordSize = calculateRecordSize("k", "d")
        def maxSegmentSize = (LOG_HEADER_SIZE + (recordSize * 5)) as long
        def engine = newEngine(tempDir, maxSegmentSize)
        def topic = "earliest-offset-topic"
        15.times { i ->
            engine.append(topic, 0, createRecord(i, "k", "d"))
        }
        engine.close()

        and: "delete the earliest segment files"
        def partitionDir = tempDir.resolve(topic).resolve("partition-0")
        def logFiles = listLogFiles(partitionDir).sort { a, b ->
            extractBaseOffset(a) <=> extractBaseOffset(b)
        }
        def earliestLog = logFiles.first()
        def earliestBase = extractBaseOffset(earliestLog)
        def earliestIndex = partitionDir.resolve(String.format("%020d.index", earliestBase))
        Files.deleteIfExists(earliestLog)
        Files.deleteIfExists(earliestIndex)

        and: "determine expected earliest offset"
        def remaining = listLogFiles(partitionDir).sort { a, b ->
            extractBaseOffset(a) <=> extractBaseOffset(b)
        }
        def expectedEarliest = extractBaseOffset(remaining.first())

        when: "recovering the engine"
        def recovered = newEngine(tempDir, 256L)
        recovered.recover()
        def earliest = recovered.getEarliestOffset(topic, 0)

        then: "earliest offset points to remaining segment"
        earliest == expectedEarliest

        cleanup:
        recovered?.close()
    }

    def "max offset from metadata reflects last written record"() {
        given: "an engine with persisted metadata"
        def engine = newEngine(tempDir, 1024 * 1024L)
        def topic = "metadata-topic"
        3.times { i ->
            engine.append(topic, 0, createRecord(i, "key-${i}", "data-${i}"))
        }
        engine.close()

        when: "recovering and reading max offset from metadata"
        def recovered = newEngine(tempDir, 1024 * 1024L)
        recovered.recover()
        def maxOffset = recovered.getMaxOffsetFromMetadata(topic, 0)

        then: "max offset matches the last record"
        maxOffset == 2L

        cleanup:
        recovered?.close()
    }

    def "validate returns a successful result"() {
        given:
        def engine = newEngine(tempDir, 1024 * 1024L)
        def topic = "validate-topic"
        engine.append(topic, 0, createRecord(0L, "key", "data"))

        when:
        def result = engine.validate(topic, 0)

        then:
        result.isValid()
        result.getErrors().isEmpty()

        cleanup:
        engine?.close()
    }

    def "compact is a no-op but does not throw"() {
        given:
        def engine = newEngine(tempDir, 1024 * 1024L)
        def topic = "compact-topic"
        engine.append(topic, 0, createRecord(0L, "key", "data"))

        when:
        engine.compact(topic, 0)

        then:
        noExceptionThrown()

        cleanup:
        engine?.close()
    }

    private static FileChannelStorageEngine newEngine(Path dataDir, long maxSegmentSize) {
        def watermarkTracker = new StorageWatermarkTracker()
        def metadataFactory = new SegmentMetadataStoreFactory(dataDir.toString())
        return new FileChannelStorageEngine(
            dataDir.toString(),
            maxSegmentSize,
            watermarkTracker,
            metadataFactory
        )
    }

    private static MessageRecord createRecord(long offset, String key, String data) {
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
        int size = 4 + key.getBytes("UTF-8").length + 1 + 4 + 8
        if (data != null) {
            size += data.getBytes("UTF-8").length
        }
        return size
    }

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
}
