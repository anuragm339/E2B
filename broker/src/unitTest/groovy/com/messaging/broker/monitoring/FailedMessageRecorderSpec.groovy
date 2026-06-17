package com.messaging.broker.monitoring

import spock.lang.Specification

/**
 * The bounded failed-message registry: per-record identity (topic#offset) with attempt counting,
 * a STUCK poison threshold, fixed 20-entry LRU capacity, and newest-first reads.
 */
class FailedMessageRecorderSpec extends Specification {

    FailedMessageRecorder rec = new FailedMessageRecorder()

    def "records a failure as one RETRYING entry with attempts=1"() {
        when:
        rec.record("prices-v1", 1000L, "k1", "g1", "storage write failed")

        then:
        def list = rec.recent()
        list.size() == 1
        list[0].topic == "prices-v1"
        list[0].offset == 1000L
        list[0].key == "k1"
        list[0].group == "g1"
        list[0].reason == "storage write failed"
        list[0].attempts == 1
        list[0].disposition() == "RETRYING"
    }

    def "repeated failures of the SAME record increment attempts and flip to STUCK at the poison threshold"() {
        when: "the same topic#offset fails 12 times"
        12.times { rec.record("prices-v1", 1000L, null, "g1", "consumer ack timeout (redelivering)") }

        then: "one row, not twelve — attempts counts, disposition escalates"
        rec.size() == 1
        def f = rec.recent()[0]
        f.attempts == 12
        f.disposition() == "STUCK"        // >= 10
    }

    def "different offsets are distinct rows"() {
        when:
        rec.record("t", 1L, null, null, "x")
        rec.record("t", 2L, null, null, "x")

        then:
        rec.size() == 2
    }

    def "is bounded to MAX with LRU eviction and reads newest-first"() {
        when: "25 distinct records fail (more than the 20 capacity)"
        (1..25).each { rec.record("t", it as long, null, null, "x") }

        then: "only the most recent 20 are kept; the oldest 5 evicted"
        rec.size() == FailedMessageRecorder.MAX
        def list = rec.recent()
        list.size() == 20
        list[0].offset == 25L     // newest first
        list[-1].offset == 6L     // 1..5 evicted
    }
}
