package com.messaging.broker.consumer

import spock.lang.Specification

import java.time.Instant

class RefreshContextSpec extends Specification {

    def "recordResetAck marks consumer replaying and offset 0"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)

        when:
        context.recordResetAck("groupA:topic")

        then:
        context.getReceivedResetAcks().contains("groupA:topic")
        context.getConsumerOffsets().get("groupA:topic") == 0L
        context.isConsumerReplaying("groupA:topic")
        context.getResetAckTimes().get("groupA:topic") != null
    }

    def "recordReadyAck marks consumer not replaying"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.recordResetAck("groupA:topic")

        when:
        context.recordReadyAck("groupA:topic")

        then:
        context.getReceivedReadyAcks().contains("groupA:topic")
        !context.isConsumerReplaying("groupA:topic")
        context.getReadyAckTimes().get("groupA:topic") != null
    }

    def "downtime tracking aggregates periods"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        def shutdown = Instant.parse("2025-01-01T00:00:00Z")
        def startup = Instant.parse("2025-01-01T00:10:00Z")

        when:
        context.recordShutdown(shutdown)
        context.recordStartup(startup)

        then:
        context.getDowntimePeriods().size() == 1
        context.getTotalDowntimeSeconds() == 600
    }

    def "all reset and ready acks checks reflect expected consumers"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic", "groupB:topic"] as Set)

        when:
        context.recordResetAck("groupA:topic")
        context.recordReadyAck("groupA:topic")

        then:
        !context.allResetAcksReceived()
        !context.allReadyAcksReceived()

        when:
        context.recordResetAck("groupB:topic")
        context.recordReadyAck("groupB:topic")

        then:
        context.allResetAcksReceived()
        context.allReadyAcksReceived()
    }
}
