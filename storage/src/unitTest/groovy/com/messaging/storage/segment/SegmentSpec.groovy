package com.messaging.storage.segment

import com.messaging.common.hash.RecordHasher
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

    def "fresh segment marks rolling hash trustworthy and folds each append"() {
        given:
        def segment = new Segment(tempDir.resolve('00000000000000000000.log'),
            tempDir.resolve('00000000000000000000.index'),
            0L,
            1024 * 1024L,
            'topic',
            0)

        and:
        def empty = segment.snapshotRollingHash()

        when:
        segment.append(record(0L, 'k0', 'a'))
        segment.append(record(1L, 'k1', 'b'))
        def after = segment.snapshotRollingHash()

        and: "compute the expected hash standalone via RecordHasher"
        int c0 = RecordHasher.recordCrc(0L, 'k0', 'M' as char, 'a')
        int c1 = RecordHasher.recordCrc(1L, 'k1', 'M' as char, 'b')
        def expected = RecordHasher.combine(RecordHasher.combine(RecordHasher.EMPTY_HASH, c0), c1)

        then:
        empty.trustworthy
        empty.recordCount == 0
        empty.maxOffsetExclusive == 0L
        empty.hash == RecordHasher.EMPTY_HASH

        after.trustworthy
        after.recordCount == 2
        after.maxOffsetExclusive == 2L
        after.hash == expected

        cleanup:
        segment?.close()
    }

    def "DELETE record contributes a distinct CRC compared to its MESSAGE counterpart"() {
        given:
        def segment = new Segment(tempDir.resolve('00000000000000000050.log'),
            tempDir.resolve('00000000000000000050.index'),
            50L,
            1024 * 1024L,
            'topic',
            0)

        when:
        segment.append(deleteRecord(50L, 'k0'))
        def snap = segment.snapshotRollingHash()
        int delCrc = RecordHasher.recordCrc(50L, 'k0', 'D' as char, null)
        int msgCrc = RecordHasher.recordCrc(50L, 'k0', 'M' as char, null)

        then:
        snap.hash == RecordHasher.combine(RecordHasher.EMPTY_HASH, delCrc)
        snap.hash != RecordHasher.combine(RecordHasher.EMPTY_HASH, msgCrc)

        cleanup:
        segment?.close()
    }

    def "loaded segment with existing data starts as untrustworthy until installRollingHash"() {
        given: "a segment with two records, then reopened"
        def basePath = tempDir.resolve('00000000000000000000')
        def logPath = Path.of(basePath.toString() + '.log')
        def indexPath = Path.of(basePath.toString() + '.index')
        def seg1 = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'topic', 0)
        seg1.append(record(0L, 'k0', 'a'))
        seg1.append(record(1L, 'k1', 'b'))
        def trustedHash = seg1.snapshotRollingHash().hash
        seg1.close()

        when: "reopen the segment from disk"
        def seg2 = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'topic', 0)
        def reopened = seg2.snapshotRollingHash()

        then: "rolling hash is empty AND flagged untrustworthy"
        !reopened.trustworthy
        reopened.hash == RecordHasher.EMPTY_HASH
        reopened.recordCount == 0

        when: "caller installs the reconstructed hash"
        seg2.installRollingHash(trustedHash, 2L)
        def installed = seg2.snapshotRollingHash()

        then:
        installed.trustworthy
        installed.hash == trustedHash
        installed.recordCount == 2

        cleanup:
        seg2?.close()
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'topic', 0, key, EventType.MESSAGE, data, Instant.now())
    }

    private static MessageRecord deleteRecord(long offset, String key) {
        new MessageRecord(offset, 'topic', 0, key, EventType.DELETE, null, Instant.now())
    }
}
