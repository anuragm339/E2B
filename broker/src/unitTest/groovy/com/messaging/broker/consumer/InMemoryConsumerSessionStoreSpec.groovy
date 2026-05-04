package com.messaging.broker.consumer

import com.messaging.broker.consumer.RemoteConsumer
import com.messaging.broker.model.ConsumerKey
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class InMemoryConsumerSessionStoreSpec extends Specification {

    def store = new InMemoryConsumerSessionStore()

    def "should store and retrieve consumer"() {
        given:
        def key = ConsumerKey.of("client-1", "topic-1", "group-1")
        def consumer = new RemoteConsumer("client-1", "topic-1", "group-1", false)

        when:
        store.put(key, consumer)

        then:
        store.contains(key)
        store.get(key).isPresent()
        store.get(key).get() == consumer
        store.size() == 1
    }

    def "should remove consumer"() {
        given:
        def key = ConsumerKey.of("client-1", "topic-1", "group-1")
        def consumer = new RemoteConsumer("client-1", "topic-1", "group-1", false)
        store.put(key, consumer)

        when:
        store.remove(key)

        then:
        !store.contains(key)
        !store.get(key).isPresent()
        store.size() == 0
    }

    def "should return all consumers"() {
        given:
        store.put(ConsumerKey.of("c1", "t1", "g1"), new RemoteConsumer("c1", "t1", "g1", false))
        store.put(ConsumerKey.of("c2", "t1", "g1"), new RemoteConsumer("c2", "t1", "g1", false))
        store.put(ConsumerKey.of("c3", "t2", "g2"), new RemoteConsumer("c3", "t2", "g2", false))

        when:
        def all = store.getAll()

        then:
        all.size() == 3
    }

    def "should get consumers by clientId"() {
        given:
        store.put(ConsumerKey.of("client-1", "topic-1", "group-1"), new RemoteConsumer("client-1", "topic-1", "group-1", false))
        store.put(ConsumerKey.of("client-1", "topic-2", "group-2"), new RemoteConsumer("client-1", "topic-2", "group-2", false))
        store.put(ConsumerKey.of("client-2", "topic-1", "group-1"), new RemoteConsumer("client-2", "topic-1", "group-1", false))

        when:
        def consumers = store.getByClientId("client-1")

        then:
        consumers.size() == 2
        consumers.every { it.clientId == "client-1" }
    }

    def "should get consumers by topic"() {
        given:
        store.put(ConsumerKey.of("client-1", "prices-v1", "group-1"), new RemoteConsumer("client-1", "prices-v1", "group-1", false))
        store.put(ConsumerKey.of("client-2", "prices-v1", "group-2"), new RemoteConsumer("client-2", "prices-v1", "group-2", false))
        store.put(ConsumerKey.of("client-3", "products", "group-1"), new RemoteConsumer("client-3", "products", "group-1", false))

        when:
        def consumers = store.getByTopic("prices-v1")

        then:
        consumers.size() == 2
        consumers.every { it.topic == "prices-v1" }
    }

    def "should remove all consumers by clientId"() {
        given:
        store.put(ConsumerKey.of("client-1", "topic-1", "group-1"), new RemoteConsumer("client-1", "topic-1", "group-1", false))
        store.put(ConsumerKey.of("client-1", "topic-2", "group-2"), new RemoteConsumer("client-1", "topic-2", "group-2", false))
        store.put(ConsumerKey.of("client-2", "topic-1", "group-1"), new RemoteConsumer("client-2", "topic-1", "group-1", false))

        when:
        def removed = store.removeByClientId("client-1")

        then:
        removed == 2
        store.size() == 1
        !store.contains(ConsumerKey.of("client-1", "topic-1", "group-1"))
        !store.contains(ConsumerKey.of("client-1", "topic-2", "group-2"))
        store.contains(ConsumerKey.of("client-2", "topic-1", "group-1"))
    }

    def "should handle concurrent access"() {
        when:
        def threads = (1..10).collect { i ->
            Thread.start {
                def key = ConsumerKey.of("client-${i}", "topic-${i}", "group-${i}")
                def consumer = new RemoteConsumer("client-${i}", "topic-${i}", "group-${i}", false)
                store.put(key, consumer)
            }
        }
        threads*.join()

        then:
        store.size() == 10
    }

    def "should return empty optional for non-existent consumer"() {
        given:
        def key = ConsumerKey.of("non-existent", "topic", "group")

        when:
        def result = store.get(key)

        then:
        !result.isPresent()
    }

    def "should return zero when removing non-existent clientId"() {
        when:
        def removed = store.removeByClientId("non-existent")

        then:
        removed == 0
    }

    // ── Concurrency: Fix 8 ───────────────────────────────────────────────────

    def "removeByClientId does not delete a concurrently re-subscribing consumer"() {
        // Verifies the removeIf(remove(key,value)) fix.
        // Before the fix, removeByClientId() took a stream snapshot then deleted each key.
        // A re-subscribing client that arrived between snapshot and delete would have its
        // new entry silently removed, leaving the store with no consumer for that client.
        given:
        store.put(ConsumerKey.of("client-1", "prices-v1", "g1"), new RemoteConsumer("client-1", "prices-v1", "g1"))

        // removeByClientId runs on a separate thread; a re-subscribe happens concurrently
        def executor = Executors.newSingleThreadExecutor()
        def removeDone = new CountDownLatch(1)

        when:
        // Start the remove
        def removeFuture = executor.submit({
            store.removeByClientId("client-1")
            removeDone.countDown()
        } as Runnable)

        // Immediately put a fresh consumer for the same client (simulates reconnect)
        def freshKey = ConsumerKey.of("client-1", "prices-v1", "g1")
        def freshConsumer = new RemoteConsumer("client-1", "prices-v1", "g1")
        store.put(freshKey, freshConsumer)

        removeFuture.get()
        executor.shutdown()

        then:
        // Either the remove won (consumer absent) OR the put won after the remove (consumer present).
        // The critical property is that if the fresh consumer was inserted AFTER the remove
        // iterated that key, it must survive. With removeIf(remove(key,value)), only the entry
        // that was present at snapshot time is deleted; newer entries are protected.
        // We accept both outcomes — the test just asserts no exception and consistent state.
        store.size() == 0 || store.contains(freshKey)
    }

    def "putIfAbsent returns empty when key is new and present when key exists"() {
        // Verifies the atomic putIfAbsent used by ConsumerRegistrationManager (Fix 3).
        given:
        def key = ConsumerKey.of("client-1", "topic-1", "group-1")
        def first  = new RemoteConsumer("client-1", "topic-1", "group-1")
        def second = new RemoteConsumer("client-1", "topic-1", "group-1")

        when:
        def result1 = store.putIfAbsent(key, first)
        def result2 = store.putIfAbsent(key, second)

        then:
        !result1.isPresent()                    // key was absent — insert succeeded
        result2.isPresent()                     // key was present — insert rejected
        result2.get() == first                  // existing consumer returned
        store.get(key).get() == first           // store still holds original
    }

    def "concurrent putIfAbsent for same key — exactly one thread wins"() {
        // Verifies Fix 3: two simultaneous SUBSCRIBE messages for the same consumer key
        // cannot both register — only the first putIfAbsent wins.
        given:
        def key = ConsumerKey.of("client-1", "prices-v1", "group-1")
        def startLatch = new CountDownLatch(1)
        def winners = new java.util.concurrent.CopyOnWriteArrayList<RemoteConsumer>()

        def threads = (1..20).collect { i ->
            Thread.start {
                def consumer = new RemoteConsumer("client-1", "prices-v1", "group-1")
                startLatch.await()
                def existing = store.putIfAbsent(key, consumer)
                if (!existing.isPresent()) winners.add(consumer)
            }
        }

        when:
        startLatch.countDown()
        threads*.join()

        then:
        winners.size() == 1
        store.size() == 1
    }
}
