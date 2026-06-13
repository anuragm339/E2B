package com.messaging.broker.ack

import com.messaging.common.exception.MessagingException
import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class InMemoryAckStoreSpec extends Specification {

    @Subject InMemoryAckStore store = new InMemoryAckStore()

    def "put then get returns the same record"() {
        given:
        def rec = new AckRecord(42L, 1_000L)

        when:
        store.put('prices-v1', 'price-group', 42L, rec)

        then:
        store.get('prices-v1', 'price-group', 42L) == rec
        store.get('prices-v1', 'price-group', 43L) == null   // no entry
    }

    def "get returns null when the topic or group does not match"() {
        given:
        store.put('topic-a', 'group-a', 1L, new AckRecord(1L, 1L))

        expect:
        store.get('topic-b', 'group-a', 1L) == null
        store.get('topic-a', 'group-b', 1L) == null
    }

    def "duplicate offsets across different groups stay separate"() {
        given:
        store.put('prices-v1', 'group-a', 5L, new AckRecord(5L, 100L))
        store.put('prices-v1', 'group-b', 5L, new AckRecord(5L, 200L))

        expect:
        store.get('prices-v1', 'group-a', 5L).ackedAtMs == 100L
        store.get('prices-v1', 'group-b', 5L).ackedAtMs == 200L
    }

    def "putBatch applies every entry"() {
        given:
        String[] topics  = ['t', 't', 't'] as String[]
        String[] groups  = ['g', 'g', 'g'] as String[]
        AckRecord[] recs = [new AckRecord(0L, 1L), new AckRecord(1L, 2L), new AckRecord(2L, 3L)] as AckRecord[]

        when:
        store.putBatch(topics, groups, recs)

        then:
        store.get('t', 'g', 0L).ackedAtMs == 1L
        store.get('t', 'g', 1L).ackedAtMs == 2L
        store.get('t', 'g', 2L).ackedAtMs == 3L
    }

    def "putBatch rejects unequal array lengths"() {
        when:
        store.putBatch(['t'] as String[], ['g', 'g'] as String[], [new AckRecord(0L, 0L)] as AckRecord[])

        then:
        thrown(MessagingException)
    }

    def "put rejects negative offsets"() {
        when:
        store.put('t', 'g', -1L, new AckRecord(0L, 0L))

        then:
        thrown(MessagingException)
    }

    def "clearByTopicAndGroup removes only entries for the matching pair"() {
        given:
        store.put('t1', 'g1', 1L, new AckRecord(1L, 1L))
        store.put('t1', 'g1', 2L, new AckRecord(2L, 2L))
        store.put('t1', 'g2', 1L, new AckRecord(1L, 3L))
        store.put('t2', 'g1', 1L, new AckRecord(1L, 4L))

        when:
        store.clearByTopicAndGroup('t1', 'g1')

        then:
        store.get('t1', 'g1', 1L) == null
        store.get('t1', 'g1', 2L) == null
        store.get('t1', 'g2', 1L) != null
        store.get('t2', 'g1', 1L) != null
    }

    def "concurrent puts on the same offset converge on one of the writes"() {
        given:
        def writers = 16
        def latch = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(writers)
        def lastFailure = new AtomicReference<Throwable>()

        when:
        def futures = (0..<writers).collect { idx ->
            CompletableFuture.runAsync({
                try {
                    latch.await()
                    store.put('topic', 'group', 100L, new AckRecord(100L, idx))
                } catch (Throwable t) {
                    lastFailure.set(t)
                }
            }, pool)
        }
        latch.countDown()
        CompletableFuture.allOf(futures as CompletableFuture[]).get(5, TimeUnit.SECONDS)

        then:
        lastFailure.get() == null
        def winner = store.get('topic', 'group', 100L)
        winner != null
        winner.ackedAtMs >= 0 && winner.ackedAtMs < writers

        cleanup:
        pool.shutdownNow()
    }

    def "getAckedOffsetsInRange returns only matching topic, group, and half-open range"() {
        given:
        [5L, 7L, 9L, 12L].each { store.put('prices-v1', 'g1', it, new AckRecord(it, 100L)) }
        store.put('prices-v1', 'g2', 7L, new AckRecord(7L, 100L))
        store.put('orders-v1', 'g1', 7L, new AckRecord(7L, 100L))

        expect:
        store.getAckedOffsetsInRange('prices-v1', 'g1', 5L, 12L) == [5L, 7L, 9L] as Set
        store.getAckedOffsetsInRange('prices-v1', 'g1', 0L, 5L).isEmpty()
        store.getAckedOffsetsInRange('prices-v1', 'g1', 12L, 12L).isEmpty()
    }
}
