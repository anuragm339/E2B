package com.messaging.storage.filechannel

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStoreFactory
import com.messaging.storage.segment.Segment
import com.messaging.storage.watermark.StorageWatermarkTracker
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.regex.Pattern

class FileChannelStorageEngineIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    def "engine recovery preserves data and metadata"() {
        given: "a storage engine with multiple segments"
        def maxSegmentSize = 1024 * 1024L
        def engine = newEngine(tempDir, maxSegmentSize)
        def topic = "integration-recovery-topic"
        int total = 15
        total.times { i ->
            engine.append(topic, 0, createRecord(i, "key-${i}", "data-${i}"))
        }
        engine.close()

        when: "recovering a new engine instance"
        def recovered = newEngine(tempDir, maxSegmentSize)
        recovered.recover()
        def records = recovered.read(topic, 0, 0L, total + 5)

        then: "all records are readable after recovery"
        records.size() == total
        records.first().getOffset() == 0L
        records.last().getOffset() == total - 1

        and: "metadata reports the correct max offset"
        recovered.getMaxOffsetFromMetadata(topic, 0) == total - 1

        and: "earliest offset is the first segment base"
        recovered.getEarliestOffset(topic, 0) == 0L

        cleanup:
        recovered?.close()
    }

    def "zero-copy batch read returns expected metadata"() {
        given: "a storage engine with data"
        def engine = newEngine(tempDir, 1024 * 1024L)
        def topic = "integration-batch-topic"
        10.times { i ->
            engine.append(topic, 0, createRecord(i, "key-${i}", "data-${i}"))
        }

        when: "requesting a zero-copy batch"
        def batch = engine.getZeroCopyBatch(topic, 0, 0L, 1024 * 1024L)

        then: "batch metadata is populated"
        batch != null
        batch.recordCount == 10
        batch.totalBytes > 0
        batch.lastOffset == 9L
        batch.fileRegion != null
        batch.fileChannel != null
        batch.filePosition >= LOG_HEADER_SIZE

        cleanup:
        batch?.fileChannel?.close()
        engine?.close()
    }

    def "earliest offset advances after deleting first segment and recovering"() {
        given: "an engine with multiple segments"
        def recordSize = calculateRecordSize("k", "d")
        def maxSegmentSize = (LOG_HEADER_SIZE + (recordSize * 5)) as long
        def engine = newEngine(tempDir, maxSegmentSize)
        def topic = "integration-earliest-topic"
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

        and: "determine expected earliest base offset"
        def remaining = listLogFiles(partitionDir).sort { a, b ->
            extractBaseOffset(a) <=> extractBaseOffset(b)
        }
        def expectedEarliest = extractBaseOffset(remaining.first())

        when: "recovering the engine"
        def recovered = newEngine(tempDir, maxSegmentSize)
        recovered.recover()

        then: "earliest offset reflects remaining segments"
        recovered.getEarliestOffset(topic, 0) == expectedEarliest

        cleanup:
        recovered?.close()
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
