package com.messaging.broker.consumer

import com.messaging.common.api.StorageEngine
import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import spock.lang.Specification

/**
 * Covers the delivery-freshness READY gate added to {@link RefreshReplayService}: a caught-up
 * refresh may only reach READY when a real delivery happened within the window, or the topic is
 * empty (healthy-idle). Uses the full constructor (gate enabled), unlike RefreshReplayServiceSpec
 * which uses the deprecated gate-disabled constructor.
 */
class RefreshReplayGateSpec extends Specification {

    ConsumerRegistry remoteConsumers = Mock()
    StorageEngine storage = Mock()
    DataRefreshMetrics metrics = Mock()
    RefreshEventLogger refreshLogger = Mock()
    DeliveryFreshnessTracker freshness = new DeliveryFreshnessTracker()

    static final long WINDOW_MS = 6L * 60L * 60L * 1000L // 6h

    RefreshReplayService service = new RefreshReplayService(
            remoteConsumers, storage, metrics, refreshLogger, freshness, WINDOW_MS)

    private RefreshContext caughtUpContext() {
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-1")
        context.getReceivedResetAcks().add("group-a:prices-v1")
        remoteConsumers.allConsumersCaughtUp("prices-v1", ["group-a:prices-v1"] as Set) >> true
        return context
    }

    def "HOLDS ready when caught up, topic has data, but no recent delivery"() {
        given: "topic has data (head >= 0) and nothing was ever delivered"
        def context = caughtUpContext()
        storage.getCurrentOffset("prices-v1", 0) >> 42L

        when:
        def result = service.checkReplayProgress("prices-v1", context)

        then: "READY is held and not signalled"
        !result
        0 * refreshLogger.logReplayProgress(_)
    }

    def "ALLOWS ready when caught up, topic has data, and delivery is fresh"() {
        given:
        def context = caughtUpContext()
        storage.getCurrentOffset("prices-v1", 0) >> 42L
        freshness.markDelivered()

        when:
        def result = service.checkReplayProgress("prices-v1", context)

        then:
        result
        1 * refreshLogger.logReplayProgress(_)
    }

    def "ALLOWS ready (healthy-idle) when caught up but topic is empty, even with no delivery"() {
        given: "empty topic -> getCurrentOffset returns -1"
        def context = caughtUpContext()
        storage.getCurrentOffset("prices-v1", 0) >> -1L

        when:
        def result = service.checkReplayProgress("prices-v1", context)

        then:
        result
        1 * refreshLogger.logReplayProgress(_)
    }
}
