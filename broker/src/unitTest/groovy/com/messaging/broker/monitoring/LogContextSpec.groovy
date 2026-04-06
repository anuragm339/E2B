package com.messaging.broker.monitoring

import com.messaging.broker.monitoring.LogContext
import spock.lang.Specification

class LogContextSpec extends Specification {

    def "should build log context with topic and clientId"() {
        when:
        def context = LogContext.builder()
                .topic("test-topic")
                .clientId("client-1")
                .build()

        then:
        context.get("topic") == "test-topic"
        context.get("clientId") == "client-1"
    }

    def "should build log context with multiple fields"() {
        when:
        def context = LogContext.builder()
                .topic("prices-v1")
                .clientId("client-123")
                .consumerGroup("price-quote-group")
                .refreshId("1234567890")
                .offset(100L)
                .build()

        then:
        context.get("topic") == "prices-v1"
        context.get("clientId") == "client-123"
        context.get("consumerGroup") == "price-quote-group"
        context.get("refreshId") == "1234567890"
        context.get("offset") == "100"
    }

    def "should support custom fields"() {
        when:
        def context = LogContext.builder()
                .custom("errorCode", "ERR_001")
                .custom("retryCount", 3)
                .build()

        then:
        context.get("errorCode") == "ERR_001"
        context.get("retryCount") == "3"
    }

    def "should format toString consistently"() {
        when:
        def context = LogContext.builder()
                .topic("test-topic")
                .offset(50L)
                .build()

        then:
        context.toString().contains("topic=test-topic")
        context.toString().contains("offset=50")
    }

    def "should return null for missing keys"() {
        when:
        def context = LogContext.builder()
                .topic("test-topic")
                .build()

        then:
        context.get("nonexistent") == null
    }

    def "should build context with state"() {
        when:
        def context = LogContext.builder()
                .topic("topic-1")
                .state("REPLAYING")
                .refreshId("12345")
                .build()

        then:
        context.get("state") == "REPLAYING"
        context.get("topic") == "topic-1"
        context.get("refreshId") == "12345"
    }
}
