package com.messaging.broker.model

import spock.lang.Specification

class CommitOffsetRequestSpec extends Specification {

    def "should create CommitOffsetRequest from components"() {
        when:
        def request = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 12345L)

        then:
        request.clientId() == "client-1"
        request.topic() == "topic-1"
        request.group() == "group-1"
        request.offset() == 12345L
    }

    def "should create request with zero offset"() {
        when:
        def request = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 0L)

        then:
        request.offset() == 0L
    }

    def "should create request with large offset"() {
        when:
        def request = CommitOffsetRequest.of("client-1", "topic-1", "group-1", Long.MAX_VALUE)

        then:
        request.offset() == Long.MAX_VALUE
    }

    def "should allow negative offset"() {
        when:
        def request = CommitOffsetRequest.of("client-1", "topic-1", "group-1", -1L)

        then:
        request.offset() == -1L
    }

    def "should convert to ConsumerKey"() {
        given:
        def request = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 100L)

        when:
        def consumerKey = request.toConsumerKey()

        then:
        consumerKey.clientId() == "client-1"
        consumerKey.topic() == "topic-1"
        consumerKey.group() == "group-1"
    }

    def "should convert to DeliveryKey"() {
        given:
        def request = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 100L)

        when:
        def deliveryKey = request.toDeliveryKey()

        then:
        deliveryKey.group() == "group-1"
        deliveryKey.topic() == "topic-1"
    }

    def "should implement equals correctly"() {
        given:
        def request1 = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 100L)
        def request2 = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 100L)
        def request3 = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 200L)

        expect:
        request1 == request2
        request1 != request3
    }

    def "should implement hashCode correctly"() {
        given:
        def request1 = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 100L)
        def request2 = CommitOffsetRequest.of("client-1", "topic-1", "group-1", 100L)

        expect:
        request1.hashCode() == request2.hashCode()
    }

    def "should reject null clientId"() {
        when:
        CommitOffsetRequest.of(null, "topic-1", "group-1", 100L)

        then:
        thrown(NullPointerException)
    }

    def "should reject null topic"() {
        when:
        CommitOffsetRequest.of("client-1", null, "group-1", 100L)

        then:
        thrown(NullPointerException)
    }

    def "should reject null group"() {
        when:
        CommitOffsetRequest.of("client-1", "topic-1", null, 100L)

        then:
        thrown(NullPointerException)
    }

    def "should handle real-world consumer commit"() {
        when:
        def request = CommitOffsetRequest.of(
            "client-abc-123",
            "prices-v1",
            "price-quote-group",
            987654321L
        )

        then:
        request.clientId() == "client-abc-123"
        request.topic() == "prices-v1"
        request.group() == "price-quote-group"
        request.offset() == 987654321L
        request.toConsumerKey().toString() == "client-abc-123:prices-v1:price-quote-group"
        request.toDeliveryKey().toString() == "price-quote-group:prices-v1"
    }
}
