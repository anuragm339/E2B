package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

class SegmentSpec extends Specification {

    @TempDir
    Path tempDir

    def "append and read returns the stored record"() {
        given:
        def segment = new Segment(tempDir.resolve('00000000000000000000.log'),
            tempDir.resolve('00000000000000000000.index'),
            0L,
            1024 * 1024L,
            'prices-v1',
            0)

        when:
        segment.append(record(0L, 'key-1', '{"v":1}'))
        def read = segment.read(0L)

        then:
        read.msgKey == 'key-1'
        read.data == '{"v":1}'
        segment.nextOffset == 1L

        cleanup:
        segment?.close()
    }

    def "getBatchFileRegion reports batch metadata for appended records"() {
        given:
        def segment = new Segment(tempDir.resolve('00000000000000000100.log'),
            tempDir.resolve('00000000000000000100.index'),
            100L,
            1024 * 1024L,
            'orders-v1',
            0)
        3.times { i ->
            segment.append(record(100L + i, "key-${i}", "data-${i}"))
        }

        when:
        def batch = segment.getBatchFileRegion(100L, 1024 * 1024L)

        then:
        batch.topic == 'orders-v1'
        batch.recordCount == 3
        batch.lastOffset == 102L
        batch.totalBytes > 0

        cleanup:
        batch?.close()
        segment?.close()
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'topic', 0, key, EventType.MESSAGE, data, Instant.now())
    }
}
