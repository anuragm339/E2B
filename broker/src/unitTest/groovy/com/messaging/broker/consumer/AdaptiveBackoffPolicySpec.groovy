package com.messaging.broker.consumer

import spock.lang.Specification

class AdaptiveBackoffPolicySpec extends Specification {

    def "uses configured minimum and maximum delays"() {
        given:
        def policy = new AdaptiveBackoffPolicy(75L, 1500L)

        when:
        def successDelay = policy.calculateNextDelay(500L, true)
        def backoffDelay = policy.calculateNextDelay(75L, false)
        def cappedDelay = policy.calculateNextDelay(1000L, false)

        then:
        policy.getInitialDelay() == 75L
        successDelay == 75L
        backoffDelay == 150L
        cappedDelay == 1500L
    }

    def "normalizes invalid minimum delay to at least one millisecond"() {
        given:
        def policy = new AdaptiveBackoffPolicy(0L, 800L)

        expect:
        policy.getInitialDelay() == 1L
        policy.calculateNextDelay(50L, true) == 1L
        policy.calculateNextDelay(500L, false) == 800L
    }
}
