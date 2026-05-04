package com.messaging.broker.ack

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class RocksDbAckStoreSpec extends Specification {

    @TempDir
    Path tempDir

    RocksDbAckStore store

    def setup() {
        store = new RocksDbAckStore(tempDir.toString(), 8 * 1024 * 1024L)
        store.init()
    }

    def cleanup() {
        store?.close()
    }

    def "put and get round-trip returns stored record"() {
        given:
        def record = new AckRecord(42L, 1000L)

        when:
        store.put("prices-v1", "price-group", 42L, record)
        def result = store.get("prices-v1", "price-group", 42L)

        then:
        result != null
        result.offset == 42L
        result.ackedAtMs == 1000L
    }

    def "get returns null for absent offset"() {
        expect:
        store.get("prices-v1", "price-group", 999L) == null
    }

    def "put overwrites existing entry (latest wins)"() {
        given:
        store.put("prices-v1", "price-group", 10L, new AckRecord(10L, 100L))

        when:
        store.put("prices-v1", "price-group", 10L, new AckRecord(10L, 999L))
        def result = store.get("prices-v1", "price-group", 10L)

        then:
        result.offset == 10L
        result.ackedAtMs == 999L
    }

    def "AckRecord binary serde is exactly 16 bytes"() {
        given:
        def record = new AckRecord(Long.MAX_VALUE, Long.MIN_VALUE)

        when:
        def bytes = record.toBytes()
        def roundTripped = AckRecord.fromBytes(bytes)

        then:
        bytes.length == 16
        roundTripped.offset == Long.MAX_VALUE
        roundTripped.ackedAtMs == Long.MIN_VALUE
    }

    def "putBatch writes multiple records atomically"() {
        given:
        def topics   = ["t1", "t1", "t2"] as String[]
        def groups   = ["g1", "g1", "g1"] as String[]
        def records  = [new AckRecord(1L, 10L), new AckRecord(2L, 20L), new AckRecord(3L, 30L)] as AckRecord[]

        when:
        store.putBatch(topics, groups, records)

        then:
        store.get("t1", "g1", 1L).offset == 1L
        store.get("t1", "g1", 2L).offset == 2L
        store.get("t2", "g1", 3L).offset == 3L
    }

    def "putBatch writes all records — no null-offset filtering"() {
        given:
        def topics   = ["t1", "t1"] as String[]
        def groups   = ["g1", "g1"] as String[]
        def records  = [new AckRecord(0L, 10L), new AckRecord(1L, 20L)] as AckRecord[]

        when:
        store.putBatch(topics, groups, records)

        then:
        noExceptionThrown()
        store.get("t1", "g1", 0L).offset == 0L
        store.get("t1", "g1", 1L).offset == 1L
    }

    def "clearByTopicAndGroup deletes only matching entries and leaves others intact"() {
        given:
        store.put("t1", "g1", 1L, new AckRecord(1L, 10L))
        store.put("t1", "g1", 2L, new AckRecord(2L, 20L))
        store.put("t1", "g2", 3L, new AckRecord(3L, 30L))   // different group
        store.put("t2", "g1", 4L, new AckRecord(4L, 40L))   // different topic

        when:
        store.clearByTopicAndGroup("t1", "g1")

        then:
        store.get("t1", "g1", 1L) == null
        store.get("t1", "g1", 2L) == null
        store.get("t1", "g2", 3L) != null   // preserved — different group
        store.get("t2", "g1", 4L) != null   // preserved — different topic
    }
}
