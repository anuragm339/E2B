package com.messaging.broker.consumer

import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import spock.lang.Specification

class RefreshReplayServiceSpec extends Specification {

    ConsumerRegistry remoteConsumers = Mock()
    DataRefreshMetrics metrics = Mock()
    RefreshEventLogger refreshLogger = Mock()
    RefreshReplayService service = new RefreshReplayService(remoteConsumers, metrics, refreshLogger)

    def "checkReplayProgress short circuits when state or ack set is not ready"() {
        given:
        def idle = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        idle.setState(RefreshState.IDLE)
        def replaying = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        replaying.setState(RefreshState.REPLAYING)

        when:
        def idleResult = service.checkReplayProgress("prices-v1", idle)
        def replayingResult = service.checkReplayProgress("prices-v1", replaying)

        then:
        !idleResult
        !replayingResult
        0 * remoteConsumers._
    }

    def "checkReplayProgress returns true once all acked consumers are caught up"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-1")
        context.getReceivedResetAcks().add("group-a:prices-v1")
        remoteConsumers.allConsumersCaughtUp("prices-v1", ["group-a:prices-v1"] as Set) >> true

        when:
        def done = service.checkReplayProgress("prices-v1", context)

        then:
        done
        1 * refreshLogger.logReplayProgress(_)
        0 * remoteConsumers.getConsumerGroupTopicPairs(_)
    }

    def "checkReplayProgress triggers replay only for reset acked consumers"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1", "group-b:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-2")
        context.getReceivedResetAcks().add("group-a:prices-v1")
        remoteConsumers.allConsumersCaughtUp("prices-v1", ["group-a:prices-v1"] as Set) >> false
        remoteConsumers.getConsumerGroupTopicPairs("prices-v1") >> [
                new ConsumerRegistry.ConsumerGroupTopicPair("client-a", "group-a:prices-v1"),
                new ConsumerRegistry.ConsumerGroupTopicPair("client-b", "group-b:prices-v1")
        ]

        when:
        def done = service.checkReplayProgress("prices-v1", context)

        then:
        !done
        1 * metrics.recordReplayStarted("prices-v1", "prices-v1", "refresh-2")
    }

    def "startReplayForConsumer swallows replay startup exceptions"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setRefreshId("refresh-3")
        metrics.recordReplayStarted("prices-v1", "prices-v1", "refresh-3") >> { throw new RuntimeException("boom") }

        when:
        service.startReplayForConsumer("client-a", "prices-v1", context)

        then:
        noExceptionThrown()
    }

    def "allConsumersCaughtUp delegates to consumer registry"() {
        given:
        def acked = ["group-a:prices-v1"] as Set
        remoteConsumers.allConsumersCaughtUp("prices-v1", acked) >> true

        expect:
        service.allConsumersCaughtUp("prices-v1", acked)
    }
}
