package com.messaging.broker.consumer

import spock.lang.Specification

class RemoteConsumerSpec extends Specification {

    def "backoff grows exponentially and is capped"() {
        given:
        def consumer = new RemoteConsumer('client-1', 'prices-v1', 'group-a')

        expect:
        consumer.backoffDelay == 0L

        when:
        1.upto(8) { consumer.recordFailure() }

        then:
        consumer.consecutiveFailures == 8
        consumer.backoffDelay == 5000L
    }

    def "resetFailures clears counters"() {
        given:
        def consumer = new RemoteConsumer('client-1', 'prices-v1', 'group-a', true)
        consumer.recordFailure()
        consumer.recordFailure()

        when:
        consumer.resetFailures()

        then:
        consumer.consecutiveFailures == 0
        consumer.lastFailureTime == 0L
        consumer.legacy
    }
}
