package com.messaging.broker.model

import spock.lang.Specification

class DeliveryKeySpec extends Specification {

    def "should create DeliveryKey from components"() {
        when:
        def key = DeliveryKey.of("group-1", "topic-1")

        then:
        key.group() == "group-1"
        key.topic() == "topic-1"
    }

    def "should generate correct string representation"() {
        given:
        def key = DeliveryKey.of("group-1", "topic-1")

        when:
        def result = key.toString()

        then:
        result == "group-1:topic-1"
    }

    def "should parse DeliveryKey from string format"() {
        when:
        def key = DeliveryKey.parse("group-1:topic-1")

        then:
        key.group() == "group-1"
        key.topic() == "topic-1"
    }

    def "should handle colons in topic name when parsing"() {
        when:
        def key = DeliveryKey.parse("group-1:topic:with:colons")

        then:
        key.group() == "group-1"
        key.topic() == "topic:with:colons"
    }

    def "should implement equals correctly"() {
        given:
        def key1 = DeliveryKey.of("group-1", "topic-1")
        def key2 = DeliveryKey.of("group-1", "topic-1")
        def key3 = DeliveryKey.of("group-2", "topic-1")

        expect:
        key1 == key2
        key1 != key3
    }

    def "should implement hashCode correctly"() {
        given:
        def key1 = DeliveryKey.of("group-1", "topic-1")
        def key2 = DeliveryKey.of("group-1", "topic-1")

        expect:
        key1.hashCode() == key2.hashCode()
    }

    def "should reject null group"() {
        when:
        DeliveryKey.of(null, "topic-1")

        then:
        thrown(NullPointerException)
    }

    def "should reject null topic"() {
        when:
        DeliveryKey.of("client-1", null)

        then:
        thrown(NullPointerException)
    }

    def "should reject null string in parse"() {
        when:
        DeliveryKey.parse(null)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("cannot be null or empty")
    }

    def "should reject empty string in parse"() {
        when:
        DeliveryKey.parse("")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("cannot be null or empty")
    }

    def "should reject invalid format with no colon"() {
        when:
        DeliveryKey.parse("group-1-topic-1")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("Invalid DeliveryKey format")
        e.message.contains("expected group:topic")
    }

    def "should parse DeliveryKey with special characters"() {
        when:
        def key = DeliveryKey.parse("group-123:topic_v1.0")

        then:
        key.group() == "group-123"
        key.topic() == "topic_v1.0"
    }

    def "should be usable as map key"() {
        given:
        def map = [:]
        def key1 = DeliveryKey.of("group-1", "topic-1")
        def key2 = DeliveryKey.of("group-1", "topic-1")

        when:
        map[key1] = "value1"
        def result = map[key2]

        then:
        result == "value1"
    }
}
