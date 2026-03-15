package com.messaging.broker.model

import spock.lang.Specification

class SubscribeRequestSpec extends Specification {

    // Modern protocol tests

    def "should create modern subscribe request"() {
        when:
        def request = SubscribeRequest.modern("client-1", "topic-1", "group-1")

        then:
        request.clientId() == "client-1"
        request.isModern()
        !request.isLegacy()
        request instanceof SubscribeRequest.Modern
    }

    def "modern request should have topic and group"() {
        when:
        def request = SubscribeRequest.modern("client-1", "prices-v1", "price-quote-group")

        then:
        with(request as SubscribeRequest.Modern) {
            topic() == "prices-v1"
            group() == "price-quote-group"
        }
    }

    def "modern request should reject null clientId"() {
        when:
        SubscribeRequest.modern(null, "topic-1", "group-1")

        then:
        thrown(NullPointerException)
    }

    def "modern request should reject null topic"() {
        when:
        SubscribeRequest.modern("client-1", null, "group-1")

        then:
        thrown(NullPointerException)
    }

    def "modern request should reject null group"() {
        when:
        SubscribeRequest.modern("client-1", "topic-1", null)

        then:
        thrown(NullPointerException)
    }

    def "modern request should implement equals correctly"() {
        given:
        def request1 = SubscribeRequest.modern("client-1", "topic-1", "group-1")
        def request2 = SubscribeRequest.modern("client-1", "topic-1", "group-1")
        def request3 = SubscribeRequest.modern("client-2", "topic-1", "group-1")

        expect:
        request1 == request2
        request1 != request3
    }

    // Legacy protocol tests

    def "should create legacy subscribe request"() {
        when:
        def request = SubscribeRequest.legacy("client-1", "price-quote", ["prices-v1", "reference-data-v5"])

        then:
        request.clientId() == "client-1"
        !request.isModern()
        request.isLegacy()
        request instanceof SubscribeRequest.Legacy
    }

    def "legacy request should have service name and topics"() {
        when:
        def request = SubscribeRequest.legacy("client-1", "price-quote", ["prices-v1", "reference-data-v5"])

        then:
        with(request as SubscribeRequest.Legacy) {
            serviceName() == "price-quote"
            topics() == ["prices-v1", "reference-data-v5"]
        }
    }

    def "legacy request should handle single topic"() {
        when:
        def request = SubscribeRequest.legacy("client-1", "service-1", ["topic-1"])

        then:
        with(request as SubscribeRequest.Legacy) {
            topics().size() == 1
            topics()[0] == "topic-1"
        }
    }

    def "legacy request should handle multiple topics"() {
        when:
        def topicList = ["t1", "t2", "t3", "t4", "t5", "t6"]
        def request = SubscribeRequest.legacy("client-1", "service-1", topicList)

        then:
        with(request as SubscribeRequest.Legacy) {
            topics().size() == 6
            topics() == ["t1", "t2", "t3", "t4", "t5", "t6"]
        }
    }

    def "legacy request should create immutable copy of topics list"() {
        given:
        def mutableTopics = ["topic-1", "topic-2"]

        when:
        def request = SubscribeRequest.legacy("client-1", "service-1", mutableTopics)
        mutableTopics.add("topic-3")

        then:
        with(request as SubscribeRequest.Legacy) {
            topics().size() == 2
            topics() == ["topic-1", "topic-2"]
        }
    }

    def "legacy request should reject null clientId"() {
        when:
        SubscribeRequest.legacy(null, "service-1", ["topic-1"])

        then:
        thrown(NullPointerException)
    }

    def "legacy request should reject null serviceName"() {
        when:
        SubscribeRequest.legacy("client-1", null, ["topic-1"])

        then:
        thrown(NullPointerException)
    }

    def "legacy request should reject null topics list"() {
        when:
        SubscribeRequest.legacy("client-1", "service-1", null)

        then:
        thrown(NullPointerException)
    }

    def "legacy request should reject empty topics list"() {
        when:
        SubscribeRequest.legacy("client-1", "service-1", [])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("topics list cannot be empty")
    }

    def "legacy request should implement equals correctly"() {
        given:
        def request1 = SubscribeRequest.legacy("client-1", "service-1", ["topic-1"])
        def request2 = SubscribeRequest.legacy("client-1", "service-1", ["topic-1"])
        def request3 = SubscribeRequest.legacy("client-1", "service-2", ["topic-1"])

        expect:
        request1 == request2
        request1 != request3
    }

    // Pattern matching / type discrimination tests

    def "should support pattern matching on request type"() {
        given:
        def modernRequest = SubscribeRequest.modern("client-1", "topic-1", "group-1")
        def legacyRequest = SubscribeRequest.legacy("client-2", "service-1", ["topic-1"])

        expect:
        modernRequest instanceof SubscribeRequest.Modern
        legacyRequest instanceof SubscribeRequest.Legacy
    }

    def "should correctly identify request types"() {
        given:
        def modernRequest = SubscribeRequest.modern("client-1", "topic-1", "group-1")
        def legacyRequest = SubscribeRequest.legacy("client-2", "service-1", ["topic-1"])

        expect:
        modernRequest.isModern() && !modernRequest.isLegacy()
        legacyRequest.isLegacy() && !legacyRequest.isModern()
    }

    // Real-world scenario tests

    def "should create request for real consumer service"() {
        when:
        def request = SubscribeRequest.legacy(
            "client-123",
            "price-quote",
            ["prices-v1", "reference-data-v5", "non-promotable-products", "prices-v4", "minimum-price", "deposit"]
        )

        then:
        with(request as SubscribeRequest.Legacy) {
            clientId() == "client-123"
            serviceName() == "price-quote"
            topics().size() == 6
            topics().contains("prices-v1")
            topics().contains("deposit")
        }
    }
}
