package com.messaging.broker.model

import spock.lang.Specification

class ConsumerKeySpec extends Specification {

    def "should create ConsumerKey from components"() {
        when:
        def key = ConsumerKey.of("client-1", "topic-1", "group-1")

        then:
        key.clientId() == "client-1"
        key.topic() == "topic-1"
        key.group() == "group-1"
    }

    def "should generate correct string representation"() {
        given:
        def key = ConsumerKey.of("client-1", "topic-1", "group-1")

        when:
        def result = key.toString()

        then:
        result == "client-1:topic-1:group-1"
    }

    def "should parse ConsumerKey from string format"() {
        when:
        def key = ConsumerKey.parse("client-1:topic-1:group-1")

        then:
        key.clientId() == "client-1"
        key.topic() == "topic-1"
        key.group() == "group-1"
    }

    def "should handle colons in group name when parsing"() {
        when:
        def key = ConsumerKey.parse("client-1:topic-1:group:with:colons")

        then:
        key.clientId() == "client-1"
        key.topic() == "topic-1"
        key.group() == "group:with:colons"
    }

    def "should implement equals correctly"() {
        given:
        def key1 = ConsumerKey.of("client-1", "topic-1", "group-1")
        def key2 = ConsumerKey.of("client-1", "topic-1", "group-1")
        def key3 = ConsumerKey.of("client-2", "topic-1", "group-1")

        expect:
        key1 == key2
        key1 != key3
    }

    def "should implement hashCode correctly"() {
        given:
        def key1 = ConsumerKey.of("client-1", "topic-1", "group-1")
        def key2 = ConsumerKey.of("client-1", "topic-1", "group-1")

        expect:
        key1.hashCode() == key2.hashCode()
    }

    def "should convert to DeliveryKey"() {
        given:
        def consumerKey = ConsumerKey.of("client-1", "topic-1", "group-1")

        when:
        def deliveryKey = consumerKey.toDeliveryKey()

        then:
        deliveryKey.group() == "group-1"
        deliveryKey.topic() == "topic-1"
    }

    def "should reject null clientId"() {
        when:
        ConsumerKey.of(null, "topic-1", "group-1")

        then:
        thrown(NullPointerException)
    }

    def "should reject null topic"() {
        when:
        ConsumerKey.of("client-1", null, "group-1")

        then:
        thrown(NullPointerException)
    }

    def "should reject null group"() {
        when:
        ConsumerKey.of("client-1", "topic-1", null)

        then:
        thrown(NullPointerException)
    }

    def "should reject null string in parse"() {
        when:
        ConsumerKey.parse(null)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("cannot be null or empty")
    }

    def "should reject empty string in parse"() {
        when:
        ConsumerKey.parse("")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("cannot be null or empty")
    }

    def "should reject invalid format with too few parts"() {
        when:
        ConsumerKey.parse("client-1:topic-1")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("Invalid ConsumerKey format")
        e.message.contains("expected clientId:topic:group")
    }

    def "should reject invalid format with only one part"() {
        when:
        ConsumerKey.parse("client-1")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("Invalid ConsumerKey format")
    }
}
