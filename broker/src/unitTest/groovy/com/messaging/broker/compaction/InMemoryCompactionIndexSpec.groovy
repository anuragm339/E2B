package com.messaging.broker.compaction

import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class InMemoryCompactionIndexSpec extends Specification {

    @Subject InMemoryCompactionIndex index = new InMemoryCompactionIndex()

    def "updateKey records the latest offset and timestamp for a fresh key"() {
        when:
        index.updateKey('prices-v1', 'sku-1', 10L, 1_000L)

        then:
        def latest = index.getLatestOffsetAndTimestamp('prices-v1', 'sku-1')
        latest[0] == 10L
        latest[1] == 1_000L
    }

    def "updateKey is a no-op for null msgKey"() {
        when:
        index.updateKey('prices-v1', null, 10L, 1_000L)

        then:
        !index.hasIndexedKeysForTopic('prices-v1')
    }

    def "updateKey does not regress on an out-of-order lower offset"() {
        given:
        index.updateKey('prices-v1', 'sku-1', 100L, 500L)

        when:
        index.updateKey('prices-v1', 'sku-1', 50L, 999L)

        then:
        def latest = index.getLatestOffsetAndTimestamp('prices-v1', 'sku-1')
        latest[0] == 100L
        latest[1] == 500L
    }

    def "isSuperseded returns true when a newer offset exists, false otherwise"() {
        given:
        index.updateKey('prices-v1', 'sku-1', 200L, 1L)

        expect:
        index.isSuperseded('prices-v1', 'sku-1', 100L)
        !index.isSuperseded('prices-v1', 'sku-1', 200L)   // same offset is the latest, not superseded
        !index.isSuperseded('prices-v1', 'sku-1', 300L)
        !index.isSuperseded('prices-v1', 'sku-1', 200L)   // sanity, no off-by-one
    }

    def "isSuperseded always returns false for null msgKey"() {
        given:
        index.updateKey('prices-v1', 'sku-1', 100L, 1L)

        expect:
        !index.isSuperseded('prices-v1', null, 50L)
    }

    def "shouldFilterDelivery flips on once an offset is superseded and flips off after markCompactedThrough"() {
        given: "two updates for the same key — the first becomes stale"
        index.updateKey('prices-v1', 'sku-1', 10L, 1L)
        index.updateKey('prices-v1', 'sku-1', 50L, 2L)

        expect: "any delivery up to and including offset 10 may contain a superseded record"
        index.shouldFilterDelivery('prices-v1', 0L)
        index.shouldFilterDelivery('prices-v1', 10L)
        !index.shouldFilterDelivery('prices-v1', 11L)

        when: "the compaction sweep has covered the stale watermark"
        index.markCompactedThrough('prices-v1', 10L)

        then: "fast-path delivery resumes for the whole topic"
        !index.shouldFilterDelivery('prices-v1', 0L)
        !index.shouldFilterDelivery('prices-v1', 10L)
    }

    def "markCompactedThrough below the watermark is a no-op"() {
        given:
        index.updateKey('prices-v1', 'sku-1', 10L, 1L)
        index.updateKey('prices-v1', 'sku-1', 50L, 2L)

        when:
        index.markCompactedThrough('prices-v1', 5L)   // below the stale-offset watermark of 10

        then:
        index.shouldFilterDelivery('prices-v1', 0L)
    }

    def "getLatestOffsetsForTopic returns one entry per key for that topic only"() {
        given:
        index.updateKey('a', 'k1', 1L, 100L)
        index.updateKey('a', 'k2', 2L, 200L)
        index.updateKey('b', 'k1', 9L, 999L)

        when:
        def snapshot = index.getLatestOffsetsForTopic('a')

        then:
        snapshot.size() == 2
        snapshot['k1'][0] == 1L
        snapshot['k1'][1] == 100L
        snapshot['k2'][0] == 2L
        snapshot['k2'][1] == 200L
        !snapshot.containsKey('b')   // sanity: not poisoned by other topics
    }

    def "hasIndexedKeysForTopic is true only after the first updateKey for that topic"() {
        expect:
        !index.hasIndexedKeysForTopic('topic-a')

        when:
        index.updateKey('topic-a', 'k', 1L, 1L)

        then:
        index.hasIndexedKeysForTopic('topic-a')
        !index.hasIndexedKeysForTopic('topic-b')
    }

    def "concurrent updateKey for the same key preserves the highest offset"() {
        given:
        def writers = 16
        def latch = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(writers)

        when:
        def futures = (1..writers).collect { idx ->
            CompletableFuture.runAsync({
                latch.await()
                index.updateKey('prices', 'key', (long) idx, (long) idx)
            }, pool)
        }
        latch.countDown()
        CompletableFuture.allOf(futures as CompletableFuture[]).get(5, TimeUnit.SECONDS)

        then:
        def latest = index.getLatestOffsetAndTimestamp('prices', 'key')
        latest[0] == writers
        latest[1] == writers

        cleanup:
        pool.shutdownNow()
    }
}
