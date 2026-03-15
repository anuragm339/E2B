package com.messaging.broker.model

import spock.lang.Specification

class DataMessageRequestSpec extends Specification {

    def "should create DataMessageRequest with default partition"() {
        given:
        def payloadBytes = "test message".bytes

        when:
        def request = DataMessageRequest.of("topic-1", payloadBytes)

        then:
        request.topic() == "topic-1"
        request.payload() == payloadBytes
        request.partition() == 0
    }

    def "should create DataMessageRequest with specific partition"() {
        given:
        def payloadBytes = "test message".bytes

        when:
        def request = DataMessageRequest.of("topic-1", payloadBytes, 5)

        then:
        request.topic() == "topic-1"
        request.payload() == payloadBytes
        request.partition() == 5
    }

    def "should return correct payload size"() {
        given:
        def payloadBytes = "test message with some content".bytes
        def request = DataMessageRequest.of("topic-1", payloadBytes)

        when:
        def size = request.payloadSize()

        then:
        size == payloadBytes.length
    }

    def "should return defensive copy of payload"() {
        given:
        def originalPayload = "original".bytes
        def request = DataMessageRequest.of("topic-1", originalPayload)

        when:
        def retrievedPayload = request.payload()
        retrievedPayload[0] = (byte)88  // Modify retrieved copy ('X' = 88)

        then:
        request.payload()[0] != (byte)88  // Original should be unchanged
        request.payload() == "original".bytes
    }

    def "should create immutable copy of payload in constructor"() {
        given:
        def mutablePayload = "mutable".bytes

        when:
        def request = DataMessageRequest.of("topic-1", mutablePayload)
        mutablePayload[0] = (byte)88  // Modify original array ('X' = 88)

        then:
        request.payload()[0] != (byte)88  // Request payload should be unchanged
        request.payload() == "mutable".bytes
    }

    def "should handle empty payload"() {
        when:
        def request = DataMessageRequest.of("topic-1", new byte[0])

        then:
        request.payloadSize() == 0
        request.payload().length == 0
    }

    def "should handle large payload"() {
        given:
        def largePayload = new byte[1024 * 1024]  // 1MB
        Arrays.fill(largePayload, (byte) 42)

        when:
        def request = DataMessageRequest.of("topic-1", largePayload)

        then:
        request.payloadSize() == 1024 * 1024
        request.payload()[0] == 42 as byte
    }

    def "should implement equals correctly"() {
        given:
        def payload1 = "message".bytes
        def payload2 = "message".bytes
        def payload3 = "different".bytes
        def request1 = DataMessageRequest.of("topic-1", payload1, 0)
        def request2 = DataMessageRequest.of("topic-1", payload2, 0)
        def request3 = DataMessageRequest.of("topic-1", payload3, 0)
        def request4 = DataMessageRequest.of("topic-2", payload1, 0)
        def request5 = DataMessageRequest.of("topic-1", payload1, 1)

        expect:
        request1 == request2  // Same topic, payload, partition
        request1 != request3  // Different payload
        request1 != request4  // Different topic
        request1 != request5  // Different partition
    }

    def "should implement hashCode correctly"() {
        given:
        def payload1 = "message".bytes
        def payload2 = "message".bytes
        def request1 = DataMessageRequest.of("topic-1", payload1)
        def request2 = DataMessageRequest.of("topic-1", payload2)

        expect:
        request1.hashCode() == request2.hashCode()
    }

    def "should reject null topic"() {
        when:
        DataMessageRequest.of(null, "payload".bytes)

        then:
        thrown(NullPointerException)
    }

    def "should reject null payload"() {
        when:
        DataMessageRequest.of("topic-1", null)

        then:
        thrown(NullPointerException)
    }

    def "should handle real-world JSON payload"() {
        given:
        def jsonPayload = '{"event":"price_update","product_id":"12345","price":99.99}'.bytes

        when:
        def request = DataMessageRequest.of("prices-v1", jsonPayload)

        then:
        request.topic() == "prices-v1"
        request.payloadSize() == jsonPayload.length
        new String(request.payload()) == '{"event":"price_update","product_id":"12345","price":99.99}'
    }

    def "toString should not expose payload content"() {
        given:
        def sensitivePayload = "sensitive data".bytes
        def request = DataMessageRequest.of("topic-1", sensitivePayload)

        when:
        def str = request.toString()

        then:
        str.contains("topic-1")
        str.contains("payloadSize=")
        !str.contains("sensitive data")  // Payload content not in toString
    }
}
