package com.messaging.broker.consumer

import spock.lang.Specification

class GlobalBarrierPolicySpec extends Specification {

    def policy = new GlobalBarrierPolicy()

    def "canGoLive is FALSE while any in-flight refresh has not settled"() {
        given:
        def settled = Mock(RefreshContext) { allReadyAcksReceived() >> true }
        def notSettled = Mock(RefreshContext) { allReadyAcksReceived() >> false }

        expect: "topic x is settled, but y is still catching up → x is held by the barrier"
        !policy.canGoLive("x", ["x": settled, "y": notSettled])
    }

    def "canGoLive is TRUE once every in-flight refresh has settled"() {
        given:
        def a = Mock(RefreshContext) { allReadyAcksReceived() >> true }
        def b = Mock(RefreshContext) { allReadyAcksReceived() >> true }

        expect:
        policy.canGoLive("x", ["x": a, "y": b])
    }

    def "a single-topic refresh trivially passes the barrier"() {
        given:
        def only = Mock(RefreshContext) { allReadyAcksReceived() >> true }

        expect:
        policy.canGoLive("x", ["x": only])
    }

    def "health scope is node-wide"() {
        expect:
        policy.healthScope() == RefreshReadinessPolicy.HealthScope.NODE_WIDE
    }
}
