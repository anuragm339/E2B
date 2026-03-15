package com.messaging.broker.consumer

import com.messaging.broker.consumer.RemoteConsumer
import com.messaging.broker.model.ConsumerKey
import spock.lang.Specification

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
}
