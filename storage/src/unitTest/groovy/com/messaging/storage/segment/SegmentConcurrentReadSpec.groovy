package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class SegmentConcurrentReadSpec extends Specification {

    @TempDir
    Path tempDir

    def "concurrent reads are safe"() {
        given: "a segment with data"
        def logPath = tempDir.resolve("00000000000000001000.log")
        def indexPath = tempDir.resolve("00000000000000001000.index")
        def baseOffset = 1000L
        def maxSize = 10 * 1024 * 1024L
        def topic = "concurrent-topic"
        def partition = 0
        def segment = new Segment(logPath, indexPath, baseOffset, maxSize, topic, partition)

        and: "append records"
        int total = 200
        total.times { i ->
            segment.append(createRecord(baseOffset + i, "key-${i}", "data-${i}"))
        }

        and: "prepare concurrent readers"
        def errors = new ConcurrentLinkedQueue<String>()
        def executor = Executors.newFixedThreadPool(4)
        def seed = 42L

        when: "multiple threads read random offsets"
        4.times { threadIdx ->
            executor.submit {
                def rnd = new Random(seed + threadIdx)
                100.times {
                    long offset = baseOffset + rnd.nextInt(total)
                    def record = segment.read(offset)
                    if (record == null) {
                        errors.add("null record at offset ${offset}")
                    } else if (record.getMsgKey() != "key-${offset - baseOffset}") {
                        errors.add("mismatch at offset ${offset}: ${record.getMsgKey()}")
                    }
                }
            }
        }
        executor.shutdown()
        executor.awaitTermination(10, TimeUnit.SECONDS)

        then: "no errors are observed"
        errors.isEmpty()

        cleanup:
        segment?.close()
    }

    private MessageRecord createRecord(long offset, String key, String data) {
        def record = new MessageRecord()
        record.setOffset(offset)
        record.setMsgKey(key)
        record.setData(data)
        record.setEventType(EventType.MESSAGE)
        record.setCreatedAt(java.time.Instant.now())
        return record
    }
}
