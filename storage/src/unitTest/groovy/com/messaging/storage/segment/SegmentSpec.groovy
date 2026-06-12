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

    def "in-flight zero-copy transfer survives a concurrent segment close (deferred channel close)"() {
        given: 'a segment with data and an in-flight batch region'
        def segment = new Segment(tempDir.resolve('00000000000000000000.log'),
                tempDir.resolve('00000000000000000000.index'), 0L, 1024 * 1024L, 'lease-topic', 0)
        3.times { i -> segment.append(record(i, "k-$i", "data-$i")) }
        def batch = segment.getBatchFileRegion(0L, 1024 * 1024L)

        when: 'compaction replaces the segment and closes it mid-transfer'
        segment.close()

        and: 'the network layer transfers the payload AFTER the close'
        def sink = new ByteArrayOutputStream()
        def channel = java.nio.channels.Channels.newChannel(sink)
        long transferred = 0
        long written
        while ((written = batch.transferTo(channel, transferred)) > 0) {
            transferred += written
        }

        then: 'the full payload transferred — no ClosedChannelException'
        transferred == batch.getTotalBytes()

        when: 'the transport releases the batch (Netty deallocate) and tries to use it again'
        batch.close()
        batch.transferTo(java.nio.channels.Channels.newChannel(new ByteArrayOutputStream()), 0)

        then: 'the deferred close has now really closed the channel'
        thrown(java.nio.channels.ClosedChannelException)
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'topic', 0, key, EventType.MESSAGE, data, Instant.now())
    }
}
