package com.messaging.storage.mmap

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStoreFactory
import com.messaging.storage.watermark.StorageWatermarkTracker
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class MMapStorageEngineSpec extends Specification {

    @TempDir
    Path tempDir

    def "append read and watermark update roundtrip"() {
        given:
        def engine = newEngine(tempDir, 1024 * 1024L)

        when:
        def offset = engine.append('prices-v1', 0, record(0L, 'key-1', '{"v":1}'))
        def records = engine.read('prices-v1', 0, 0L, 10)

        then:
        offset == 0L
        engine.getCurrentOffset('prices-v1', 0) == 0L
        records*.msgKey == ['key-1']

        cleanup:
        engine?.close()
    }

    def "recover reloads data from disk"() {
        given:
        def engine = newEngine(tempDir, 1024 * 1024L)
        3.times { i ->
            engine.append('orders-v1', 0, record(i, "key-${i}", "data-${i}"))
        }
        engine.close()

        when:
        def recovered = newEngine(tempDir, 1024 * 1024L)
        recovered.recover()

        then:
        recovered.read('orders-v1', 0, 0L, 10)*.msgKey == ['key-0', 'key-1', 'key-2']

        cleanup:
        recovered?.close()
    }

    private static MMapStorageEngine newEngine(Path dir, long maxSegmentSize) {
        def watermark = new StorageWatermarkTracker()
        def factory = new SegmentMetadataStoreFactory(dir.toString())
        new MMapStorageEngine(dir.toString(), maxSegmentSize, watermark, factory)
    }

    private static MessageRecord record(long offset, String key, String data) {
        def record = new MessageRecord()
        record.offset = offset
        record.msgKey = key
        record.data = data
        record.eventType = EventType.MESSAGE
        record.createdAt = java.time.Instant.now()
        record
    }
}
