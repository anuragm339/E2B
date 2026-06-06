package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

class SegmentHasherSpec extends Specification {

    @TempDir
    Path tempDir

    def "reconstructFromScratch produces the same hash as the append-time rolling hash"() {
        given: "fresh segment with N records and its trusted rolling hash"
        def logPath = tempDir.resolve('00000000000000000000.log')
        def indexPath = tempDir.resolve('00000000000000000000.index')
        def seg1 = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'topic', 0)
        ['a', 'b', 'c', null, 'd'].eachWithIndex { val, i ->
            seg1.append(val == null
                    ? new MessageRecord(i.longValue(), 'topic', 0, "k${i}", EventType.DELETE, null, Instant.now())
                    : new MessageRecord(i.longValue(), 'topic', 0, "k${i}", EventType.MESSAGE, val, Instant.now()))
        }
        def trusted = seg1.snapshotRollingHash()
        seg1.close()

        when: "reopen segment (untrustworthy) and run reconstruct"
        def seg2 = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'topic', 0)
        SegmentHasher.reconstructFromScratch(seg2)
        def reconstructed = seg2.snapshotRollingHash()

        then:
        reconstructed.trustworthy
        reconstructed.hash == trusted.hash
        reconstructed.recordCount == trusted.recordCount

        cleanup:
        seg2?.close()
    }
}
