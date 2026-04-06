package com.messaging.storage.mmap

import com.messaging.common.api.BatchReadableStorage
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.DeliveryBatch
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files

/**
 * Integration tests for MMapStorageEngine.
 *
 * Uses @MicronautTest with broker.storage.type=mmap so Micronaut activates
 * MMapStorageEngine (via @Requires) instead of FileChannelStorageEngine.
 * Tests are functionally equivalent to FileChannelStorageEngineIntegrationSpec —
 * both engines implement the same StorageEngine + BatchReadableStorage interfaces.
 */
@MicronautTest
class MMapStorageEngineIntegrationSpec extends Specification implements TestPropertyProvider {

    @Shared dataDir = Files.createTempDirectory('storage-mmap-it-').toAbsolutePath()

    @Override
    Map<String, String> getProperties() {
        return [
            'broker.storage.type'        : 'mmap',
            'broker.storage.data-dir'    : dataDir.toString(),
            'broker.storage.segment-size': String.valueOf(1024 * 1024L),
        ]
    }

    @Inject StorageEngine storage

    def cleanupSpec() {
        dataDir?.toFile()?.deleteDir()
    }

    // =========================================================================
    // Append / read
    // =========================================================================

    def "MMapStorageEngine appends and reads records back"() {
        given:
        def topic = "mmap-basic-topic"

        when:
        def off0 = storage.append(topic, 0, record(0, "k0", "v0"))
        def off1 = storage.append(topic, 0, record(1, "k1", "v1"))
        def off2 = storage.append(topic, 0, record(2, "k2", "v2"))

        then:
        off0 == 0L && off1 == 1L && off2 == 2L

        def results = storage.read(topic, 0, 0L, 10)
        results.size() == 3
        results[0].msgKey == "k0"
        results[2].msgKey == "k2"
    }

    def "read from a specific offset skips earlier records"() {
        given:
        def topic = "mmap-skip-topic"
        5.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }

        when:
        def results = storage.read(topic, 0, 3L, 10)

        then:
        results.size() == 2
        results[0].msgKey == "k3"
        results[1].msgKey == "k4"
    }

    def "getCurrentOffset returns the latest appended offset"() {
        given:
        def topic = "mmap-cur-offset-topic"
        4.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }

        expect:
        storage.getCurrentOffset(topic, 0) == 3L
    }

    def "getEarliestOffset returns 0 for a freshly written topic"() {
        given:
        def topic = "mmap-earliest-topic"
        3.times { i -> storage.append(topic, 0, record(i, "k${i}", "v${i}")) }

        expect:
        storage.getEarliestOffset(topic, 0) == 0L
    }

    // =========================================================================
    // Zero-copy batch read
    // =========================================================================

    def "getBatch returns populated DeliveryBatch"() {
        given:
        def topic    = "mmap-batch-topic"
        def batchSto = storage as BatchReadableStorage
        8.times { i -> storage.append(topic, 0, record(i, "key-${i}", "data-${i}")) }

        when:
        DeliveryBatch batch = batchSto.getBatch(topic, 0, 0L, 1024 * 1024L)

        then:
        batch != null
        batch.recordCount == 8
        batch.totalBytes  > 0
        batch.lastOffset  == 7L
        !batch.isEmpty()

        cleanup:
        batch?.close()
    }

    def "getBatch with maxBytes limits the returned records"() {
        given:
        def topic    = "mmap-batch-limit-topic"
        def batchSto = storage as BatchReadableStorage
        20.times { i -> storage.append(topic, 0, record(i, "key-${i}", "data-${i}")) }

        when:
        // Request a very small maxBytes — should return at least one but fewer than 20 records
        DeliveryBatch batch = batchSto.getBatch(topic, 0, 0L, 100L)

        then:
        batch != null
        batch.recordCount >= 1
        batch.recordCount < 20

        cleanup:
        batch?.close()
    }

    // =========================================================================
    // DELETE event type
    // =========================================================================

    def "DELETE event type is stored and read back correctly"() {
        given:
        def topic = "mmap-delete-topic"
        storage.append(topic, 0, record(0, "key-del", null, EventType.DELETE))

        when:
        def results = storage.read(topic, 0, 0L, 5)

        then:
        results.size() == 1
        results[0].eventType == EventType.DELETE
        results[0].msgKey    == "key-del"
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static MessageRecord record(long offset, String key, String data,
                                        EventType type = EventType.MESSAGE) {
        def r = new MessageRecord()
        r.offset    = offset
        r.msgKey    = key
        r.data      = data
        r.eventType = type
        r.createdAt = java.time.Instant.now()
        return r
    }
}
