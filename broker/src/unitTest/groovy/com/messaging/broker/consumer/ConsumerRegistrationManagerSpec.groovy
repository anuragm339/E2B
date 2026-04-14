package com.messaging.broker.consumer

import com.messaging.broker.model.ConsumerKey
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.broker.monitoring.ConsumerEventLogger
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch

class ConsumerRegistrationManagerSpec extends Specification {

    ConsumerSessionStore    sessionStore   = new InMemoryConsumerSessionStore()
    ConsumerOffsetTracker   offsetTracker  = Mock()
    StorageEngine           storage        = Mock()
    BrokerMetrics           metrics        = Mock()
    ConsumerEventLogger     consumerLogger = Mock()

    ConsumerRegistrationManager manager = new ConsumerRegistrationManager(
            sessionStore, offsetTracker, storage, metrics, consumerLogger)

    def setup() {
        // Return offset 0 and storage head 0 by default (valid, no clamping needed)
        offsetTracker.getOffset(_) >> 0L
        storage.getCurrentOffset(_, _) >> 0L
    }

    // ── Happy-path ────────────────────────────────────────────────────────────

    def "registerConsumer registers a new consumer with restored offset"() {
        when:
        def result = manager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1")

        then:
        result.isNew()
        result.consumer().clientId == "client-1"
        result.consumer().topic    == "prices-v1"
        sessionStore.contains(ConsumerKey.of("client-1", "prices-v1", "group-a"))
    }

    def "registerConsumer returns duplicate when already registered"() {
        given:
        manager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1")

        when:
        def result = manager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-2")

        then:
        result.isDuplicate()
    }

    def "registerConsumer clamps offset above storage head"() {
        given:
        // Use a fresh mock for storage so it returns 100 for this test without fighting setup()
        def localStorage = Mock(com.messaging.common.api.StorageEngine) {
            getCurrentOffset("prices-v1", 0) >> 100L
        }
        def localTracker = Mock(ConsumerOffsetTracker) {
            getOffset(_) >> 999L
        }
        def localManager = new ConsumerRegistrationManager(
                new InMemoryConsumerSessionStore(), localTracker, localStorage, metrics, consumerLogger)

        when:
        def result = localManager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1")

        then:
        result.isNew()
        result.consumer().currentOffset == 100L
        1 * localTracker.updateOffset(_, 100L)
    }

    def "registerConsumer corrects negative offset to earliest available offset"() {
        // Branch (b): restoredOffset < 0 — e.g. a corrupted properties file written -1.
        // validateAndCorrectOffset() calls storage.getEarliestOffset() and resets to it,
        // preventing the consumer from polling from an invalid position.
        given:
        def localStorage = Mock(StorageEngine) {
            getCurrentOffset("prices-v1", 0) >> 50L
            getEarliestOffset("prices-v1", 0) >> 10L
        }
        def localTracker = Mock(ConsumerOffsetTracker) {
            getOffset(_) >> -1L   // corrupted / uninitialised persisted offset
        }
        def localManager = new ConsumerRegistrationManager(
                new InMemoryConsumerSessionStore(), localTracker, localStorage, metrics, consumerLogger)

        when:
        def result = localManager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1")

        then:
        result.isNew()
        result.consumer().currentOffset == 10L          // corrected to earliest
        1 * localTracker.updateOffset(_, 10L)           // persisted so it survives the next restart
    }

    def "registerConsumer falls back to restored offset when storage validation throws"() {
        // Branch (c): storage.getCurrentOffset() throws (e.g. disk error during startup).
        // The exception is caught; the consumer is registered with whatever offset was
        // previously persisted, rather than blocking registration entirely.
        given:
        def localStorage = Mock(StorageEngine) {
            getCurrentOffset("prices-v1", 0) >> { throw new RuntimeException("disk-error") }
        }
        def localTracker = Mock(ConsumerOffsetTracker) {
            getOffset(_) >> 42L   // last persisted offset before the disk error
        }
        def localManager = new ConsumerRegistrationManager(
                new InMemoryConsumerSessionStore(), localTracker, localStorage, metrics, consumerLogger)

        when:
        def result = localManager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-1")

        then:
        result.isNew()
        result.consumer().currentOffset == 42L    // restored offset returned unchanged
        0 * localTracker.updateOffset(_, _)       // no correction written — we don't know the right value
    }

    def "unregisterConsumer removes all entries for that clientId"() {
        given:
        manager.registerConsumer("client-1", "prices-v1", "group-a", false, "t1")
        manager.registerConsumer("client-1", "ref-data",  "group-a", false, "t2")
        manager.registerConsumer("client-2", "prices-v1", "group-b", false, "t3")

        when:
        def count = manager.unregisterConsumer("client-1")

        then:
        count == 2
        manager.getAllConsumers().size() == 1
        manager.getAllConsumers().every { it.clientId == "client-2" }
    }

    // ── Concurrency: Fix 3 ───────────────────────────────────────────────────

    def "concurrent registerConsumer for the same key — exactly one registration succeeds"() {
        // Before the fix, ConsumerRegistrationManager used contains() then put(), which is
        // not atomic. Two simultaneous SUBSCRIBE messages for the same client/topic/group
        // could both pass the contains() check and both register, creating duplicate state.
        // The fix uses putIfAbsent() so only one thread wins.
        given:
        def startLatch = new CountDownLatch(1)
        def newRegistrations = new CopyOnWriteArrayList()
        def duplicates       = new CopyOnWriteArrayList()

        def threads = (1..20).collect {
            Thread.start {
                startLatch.await()
                def result = manager.registerConsumer("client-1", "prices-v1", "group-a", false, "trace-${it}")
                if (result.isNew())       newRegistrations.add(result)
                else if (result.isDuplicate()) duplicates.add(result)
            }
        }

        when:
        startLatch.countDown()
        threads*.join()

        then:
        newRegistrations.size() == 1        // exactly one thread registered the consumer
        duplicates.size()       == 19       // all other threads saw a duplicate
        sessionStore.size()     == 1        // only one entry in the store
    }
}
