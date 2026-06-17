package com.messaging.broker.consumer

import spock.lang.Specification

class DeliveryFreshnessTrackerSpec extends Specification {

    DeliveryFreshnessTracker tracker = new DeliveryFreshnessTracker()

    def "starts with no delivery and never reports fresh before first delivery"() {
        expect:
        tracker.getLastSuccessfulDeliveryMs() == 0L
        !tracker.deliveredWithin(60_000L, System.currentTimeMillis())
    }

    def "markDelivered records a recent timestamp that counts as fresh inside the window"() {
        when:
        tracker.markDelivered()

        then:
        tracker.getLastSuccessfulDeliveryMs() > 0L
        tracker.deliveredWithin(60_000L, System.currentTimeMillis())
    }

    def "a delivery older than the window is not fresh"() {
        given: "a delivery timestamp 10 minutes ago, window of 5 minutes"
        tracker.markDelivered()
        long tenMinAgo = tracker.getLastSuccessfulDeliveryMs()
        long now = tenMinAgo + (10L * 60_000L)

        expect:
        !tracker.deliveredWithin(5L * 60_000L, now)
        tracker.deliveredWithin(15L * 60_000L, now)
    }

    def "exactly at the window boundary still counts as fresh"() {
        given:
        tracker.markDelivered()
        long ts = tracker.getLastSuccessfulDeliveryMs()

        expect: "now - ts == windowMs is inclusive"
        tracker.deliveredWithin(1000L, ts + 1000L)
        !tracker.deliveredWithin(1000L, ts + 1001L)
    }
}
