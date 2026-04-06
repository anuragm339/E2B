package com.messaging.storage.filechannel

import com.messaging.common.api.BatchReadableStorage
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.DeliveryBatch
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStoreFactory
import com.messaging.storage.watermark.StorageWatermarkTracker
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Pattern

/**
 * Integration tests for FileChannelStorageEngine.
 *
 * @MicronautTest starts a Micronaut context with broker.storage.type=filechannel.
 * Tests that exercise the full append/read/batch path use @Inject StorageEngine.
 * Recovery and multi-engine tests create engine instances manually (via newEngine())
 * so they can close and reopen without affecting the shared injected bean.
 */
@MicronautTest
class FileChannelStorageEngineIntegrationSpec extends Specification implements TestPropertyProvider {

    // =========================================================================
    // Context setup — data dir created before Micronaut context starts
    // =========================================================================

    @Shared Path sharedDataDir

    @Override
    Map<String, String> getProperties() {
        sharedDataDir = Files.createTempDirectory('storage-fc-it-').toAbsolutePath()
        return [
            'broker.storage.type'        : 'filechannel',
            'broker.storage.data-dir'    : sharedDataDir.toString(),
            'broker.storage.segment-size': String.valueOf(1024 * 1024L),
        ]
    }

    // =========================================================================
    // Injected beans (FileChannelStorageEngine + its dependencies)
    // =========================================================================

    @Inject StorageEngine          storage
    @Inject StorageWatermarkTracker watermarkTracker
    @Inject SegmentMetadataStoreFactory metadataFactory

    // tempDir used by recovery / deletion tests that need their own isolated engine
    @TempDir Path tempDir

    def cleanupSpec() {
        sharedDataDir?.toFile()?.deleteDir()
    }

    // =========================================================================
    // Basic append / read via injected StorageEngine
    // =========================================================================

    def "injected StorageEngine appends and reads records back"() {
        given:
        def topic = "fc-inject-topic"

        when:
        def off0 = storage.append(topic, 0, record(0, "k0", "v0"))
        def off1 = storage.append(topic, 0, record(1, "k1", "v1"))
        def off2 = storage.append(topic, 0, record(2, "k2", "v2"))

        then:
        off0 == 0L
        off1 == 1L
        off2 == 2L

        and:
        def results = storage.read(topic, 0, 0L, 10)
        results.size() == 3
        results[0].msgKey == "k0"
        results[1].msgKey == "k1"
        results[2].msgKey == "k2"
    }

    def "getCurrentOffset reflects the latest appended offset"() {
        given:
        def topic = "fc-offset-topic"
        5.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }

        expect:
        storage.getCurrentOffset(topic, 0) == 4L
    }

    def "getEarliestOffset returns 0 for a freshly written topic"() {
        given:
        def topic = "fc-earliest-topic"
        3.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }

        expect:
        storage.getEarliestOffset(topic, 0) == 0L
    }

    def "read with fromOffset skips earlier records"() {
        given:
        def topic = "fc-fromoffset-topic"
        5.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }

        when:
        def results = storage.read(topic, 0, 3L, 10)

        then:
        results.size() == 2
        results[0].msgKey == "k3"
        results[1].msgKey == "k4"
    }

    // =========================================================================
    // Zero-copy batch read via injected BatchReadableStorage
    // =========================================================================

    def "getBatch returns correct record count, totalBytes and lastOffset"() {
        given:
        def topic = "fc-batch-topic"
        10.times { i -> storage.append(topic, 0, record(i, "key-${i}", "data-${i}")) }
        def batchStorage = storage as BatchReadableStorage

        when:
        DeliveryBatch batch = batchStorage.getBatch(topic, 0, 0L, 1024 * 1024L)

        then:
        batch != null
        batch.recordCount  == 10
        batch.totalBytes   > 0
        batch.lastOffset   == 9L
        !batch.isEmpty()

        cleanup:
        batch?.close()
    }

    def "getBatch with fromOffset returns only records from that offset"() {
        given:
        def topic = "fc-batch-from-topic"
        10.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }
        def batchStorage = storage as BatchReadableStorage

        when:
        DeliveryBatch batch = batchStorage.getBatch(topic, 0, 5L, 1024 * 1024L)

        then:
        batch != null
        batch.recordCount  == 5
        batch.lastOffset   == 9L

        cleanup:
        batch?.close()
    }

    // =========================================================================
    // Recovery — uses manual engine instances with @TempDir for full isolation
    // =========================================================================

    def "engine recovery preserves data and metadata"() {
        given: "a storage engine with multiple segments"
        def maxSegmentSize = 1024 * 1024L
        def engine = newEngine(tempDir, maxSegmentSize)
        def topic  = "integration-recovery-topic"
        int total  = 15
        total.times { i ->
            engine.append(topic, 0, record(i, "key-${i}", "data-${i}"))
        }
        engine.close()

        when: "recovering a new engine instance"
        def recovered = newEngine(tempDir, maxSegmentSize)
        recovered.recover()
        def records = recovered.read(topic, 0, 0L, total + 5)

        then: "all records are readable after recovery"
        records.size() == total
        records.first().offset == 0L
        records.last().offset  == total - 1

        and: "metadata reports the correct max offset"
        recovered.getMaxOffsetFromMetadata(topic, 0) == total - 1

        and: "earliest offset is the first segment base"
        recovered.getEarliestOffset(topic, 0) == 0L

        cleanup:
        recovered?.close()
    }

    def "earliest offset advances after deleting first segment and recovering"() {
        given: "an engine with multiple small segments"
        def recordSize     = calcRecordSize("k", "d")
        def maxSegmentSize = (LOG_HEADER_SIZE + (recordSize * 5)) as long
        def engine         = newEngine(tempDir, maxSegmentSize)
        def topic          = "integration-earliest-topic"
        15.times { i -> engine.append(topic, 0, record(i, "k", "d")) }
        engine.close()

        and: "delete the earliest segment files"
        def partitionDir = tempDir.resolve(topic).resolve("partition-0")
        def logFiles = listLogFiles(partitionDir).sort { a, b ->
            extractBaseOffset(a) <=> extractBaseOffset(b)
        }
        def earliestBase = extractBaseOffset(logFiles.first())
        Files.deleteIfExists(logFiles.first())
        Files.deleteIfExists(partitionDir.resolve(String.format("%020d.index", earliestBase)))

        and: "determine the expected new earliest base"
        def remaining        = listLogFiles(partitionDir).sort { a, b -> extractBaseOffset(a) <=> extractBaseOffset(b) }
        def expectedEarliest = extractBaseOffset(remaining.first())

        when: "recovering the engine"
        def recovered = newEngine(tempDir, maxSegmentSize)
        recovered.recover()

        then: "earliest offset reflects remaining segments"
        recovered.getEarliestOffset(topic, 0) == expectedEarliest

        cleanup:
        recovered?.close()
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static FileChannelStorageEngine newEngine(Path dir, long maxSegmentSize) {
        def watermark = new StorageWatermarkTracker()
        def factory   = new SegmentMetadataStoreFactory(dir.toString())
        return new FileChannelStorageEngine(dir.toString(), maxSegmentSize, watermark, factory)
    }

    private static MessageRecord record(long offset, String key, String data) {
        def r = new MessageRecord()
        r.offset    = offset
        r.msgKey    = key
        r.data      = data
        r.eventType = EventType.MESSAGE
        r.createdAt = java.time.Instant.now()
        return r
    }

    private static final int LOG_HEADER_SIZE = 6

    private static int calcRecordSize(String key, String data) {
        int size = 4 + key.bytes.length + 1 + 4 + 8
        if (data != null) size += data.bytes.length
        return size
    }

    private static final Pattern LOG_FILE_PATTERN = Pattern.compile('(\\d{20})\\.log')

    private static long extractBaseOffset(Path logPath) {
        def m = LOG_FILE_PATTERN.matcher(logPath.fileName.toString())
        if (!m.matches()) throw new IllegalArgumentException("Invalid log filename: ${logPath}")
        return Long.parseLong(m.group(1))
    }

    private static List<Path> listLogFiles(Path dir) {
        def results = []
        Files.list(dir).withCloseable { stream ->
            stream.filter { p -> p.toString().endsWith('.log') }.forEach { p -> results << p }
        }
        return results
    }
}
