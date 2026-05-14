package com.messaging.broker.compaction

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class RocksDbCompactionIndexSpec extends Specification {

    @TempDir
    Path tempDir

    SharedRocksDb sharedDb
    RocksDbCompactionIndex index

    def setup() {
        sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        index = new RocksDbCompactionIndex(sharedDb)
    }

    def cleanup() {
        sharedDb?.close()
    }

    // ── updateKey / isSuperseded ───────────────────────────────────────────────

    def "isSuperseded returns false when no entry exists for key"() {
        expect:
        !index.isSuperseded("prices-v1", "product-abc", 0L)
    }

    def "isSuperseded returns false for the first record of a key"() {
        when:
        index.updateKey("prices-v1", "product-abc", 5L, System.currentTimeMillis())

        then:
        !index.isSuperseded("prices-v1", "product-abc", 5L)
    }

    def "isSuperseded returns true when a newer record exists for the same key"() {
        given:
        index.updateKey("prices-v1", "product-abc", 5L, System.currentTimeMillis())
        index.updateKey("prices-v1", "product-abc", 10L, System.currentTimeMillis())

        expect:
        index.isSuperseded("prices-v1", "product-abc", 5L)
    }

    def "isSuperseded returns false for the latest record when superseded one exists"() {
        given:
        index.updateKey("prices-v1", "product-abc", 5L, System.currentTimeMillis())
        index.updateKey("prices-v1", "product-abc", 10L, System.currentTimeMillis())

        expect:
        !index.isSuperseded("prices-v1", "product-abc", 10L)
    }

    def "isSuperseded is scoped to topic — same key on different topics are independent"() {
        given:
        index.updateKey("prices-v1",    "product-abc", 5L,  System.currentTimeMillis())
        index.updateKey("reference-v5", "product-abc", 10L, System.currentTimeMillis())

        expect: "prices-v1 key-5 is NOT superseded — only 1 record on that topic"
        !index.isSuperseded("prices-v1", "product-abc", 5L)

        and: "reference-v5 key-10 is the latest on its topic — not superseded"
        !index.isSuperseded("reference-v5", "product-abc", 10L)
    }

    def "updateKey advances timestamp on each call — latest wins"() {
        given:
        long ts1 = 1000L
        long ts2 = 2000L
        index.updateKey("prices-v1", "product-abc", 5L,  ts1)
        index.updateKey("prices-v1", "product-abc", 10L, ts2)

        when:
        def latest = index.getLatestOffsetAndTimestamp("prices-v1", "product-abc")

        then:
        latest[0] == 10L
        latest[1] == ts2
    }

    def "updateKey with lower offset than existing does not overwrite"() {
        given:
        index.updateKey("prices-v1", "product-abc", 10L, 2000L)

        when: "old record arrives out-of-order"
        index.updateKey("prices-v1", "product-abc", 5L, 1000L)

        then: "latest offset is still 10"
        !index.isSuperseded("prices-v1", "product-abc", 10L)
        index.isSuperseded("prices-v1", "product-abc", 5L)
    }

    def "DELETE record at latest offset is not superseded"() {
        given: "a DELETE tombstone is the latest record for a key"
        index.updateKey("prices-v1", "product-abc", 5L,  1000L)
        index.updateKey("prices-v1", "product-abc", 10L, 2000L)  // this could be a DELETE

        expect: "it is the latest — not superseded regardless of event type"
        !index.isSuperseded("prices-v1", "product-abc", 10L)
    }

    // ── getLatestOffsetsForTopic ───────────────────────────────────────────────

    def "getLatestOffsetsForTopic returns entries only for the given topic"() {
        given:
        index.updateKey("prices-v1",    "key-A", 1L, 1000L)
        index.updateKey("prices-v1",    "key-B", 2L, 2000L)
        index.updateKey("reference-v5", "key-C", 3L, 3000L)   // different topic

        when:
        def result = index.getLatestOffsetsForTopic("prices-v1")

        then:
        result.size() == 2
        result["key-A"][0] == 1L
        result["key-B"][0] == 2L
        !result.containsKey("key-C")
    }

    def "getLatestOffsetsForTopic returns empty map when topic has no entries"() {
        expect:
        index.getLatestOffsetsForTopic("no-such-topic").isEmpty()
    }

    def "getLatestOffsetsForTopic result includes timestamp in second element"() {
        given:
        index.updateKey("prices-v1", "key-X", 7L, 99999L)

        when:
        def result = index.getLatestOffsetsForTopic("prices-v1")

        then:
        result["key-X"][1] == 99999L
    }

    // ── persistence across reopen ─────────────────────────────────────────────

    def "data persists across SharedRocksDb reopen"() {
        given:
        index.updateKey("prices-v1", "product-abc", 42L, 12345L)
        sharedDb.close()

        when:
        sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        index = new RocksDbCompactionIndex(sharedDb)

        then:
        index.isSuperseded("prices-v1", "product-abc", 0L)
        !index.isSuperseded("prices-v1", "product-abc", 42L)
    }
}
