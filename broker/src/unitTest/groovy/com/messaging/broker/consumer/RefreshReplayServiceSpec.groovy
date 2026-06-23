package com.messaging.broker.consumer

import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import com.messaging.common.api.PipeConnector
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

class RefreshReplayServiceSpec extends Specification {

    ConsumerRegistry remoteConsumers = Mock()
    DataRefreshMetrics metrics = Mock()
    RefreshEventLogger refreshLogger = Mock()
    RefreshReplayService service = new RefreshReplayService(remoteConsumers, metrics, refreshLogger)

    private RefreshContext dynamicReplaying() {
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("dyn")
        context.setDynamicReplayTarget(true)
        context.setReplayTargetOffset(0L)  // hasReplayTargetOffset() == true
        context.getReceivedResetAcks().add("group-a:prices-v1")
        context
    }

    def "dynamic refresh HOLDS ready while the pipe is still streaming the backlog (not drained)"() {
        given:
        def pipe = Mock(PipeConnector)
        def resolver = Mock(RefreshReplayWindowResolver)
        def dynService = new RefreshReplayService(remoteConsumers, Mock(StorageEngine), metrics, refreshLogger, resolver, pipe)
        pipe.isUpstreamDrained() >> false   // load not finished

        when:
        def done = dynService.checkReplayProgress("prices-v1", dynamicReplaying())

        then: "not ready — does not even compute the settled target yet"
        !done
        0 * resolver.settledTarget(_)
    }

    def "dynamic refresh reaches READY once the pipe is drained and consumers caught up to the settled target"() {
        given:
        def pipe = Mock(PipeConnector)
        def resolver = Mock(RefreshReplayWindowResolver)
        def dynService = new RefreshReplayService(remoteConsumers, Mock(StorageEngine), metrics, refreshLogger, resolver, pipe)
        pipe.isUpstreamDrained() >> true                 // load finished
        resolver.settledTarget("prices-v1") >> 9L
        remoteConsumers.getCommittedOffset("group-a:prices-v1") >> 9L
        remoteConsumers.isLegacyGroupTopic("prices-v1", "group-a:prices-v1") >> true  // required == target

        when:
        def done = dynService.checkReplayProgress("prices-v1", dynamicReplaying())

        then:
        done
    }

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

    def "checkReplayProgress uses captured replay target with legacy offset convention"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-window")
        context.setReplayTargetOffset(10L)
        context.getReceivedResetAcks().add("group-a:prices-v1")
        remoteConsumers.getCommittedOffset("group-a:prices-v1") >> 10L
        remoteConsumers.isLegacyGroupTopic("prices-v1", "group-a:prices-v1") >> true

        when:
        def done = service.checkReplayProgress("prices-v1", context)

        then:
        done
        0 * remoteConsumers.allConsumersCaughtUp(_, _)
        1 * refreshLogger.logReplayProgress(_)
    }

    def "checkReplayProgress waits for target plus one with modern offset convention"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-window")
        context.setReplayTargetOffset(10L)
        context.getReceivedResetAcks().add("group-a:prices-v1")
        remoteConsumers.isLegacyGroupTopic("prices-v1", "group-a:prices-v1") >> false
        remoteConsumers.getCommittedOffset("group-a:prices-v1") >>> [10L, 10L, 11L, 11L]
        remoteConsumers.getConsumerGroupTopicPairs("prices-v1") >> []

        when:
        def beforeTargetBoundary = service.checkReplayProgress("prices-v1", context)

        then:
        !beforeTargetBoundary
        0 * refreshLogger.logReplayProgress(_)

        when:
        def done = service.checkReplayProgress("prices-v1", context)

        then:
        done
        0 * remoteConsumers.allConsumersCaughtUp(_, _)
        1 * refreshLogger.logReplayProgress(_)
    }

    def "checkReplayProgress holds READY until captured replay target is reached"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-window")
        context.setReplayTargetOffset(10L)
        context.getReceivedResetAcks().add("group-a:prices-v1")
        remoteConsumers.getCommittedOffset("group-a:prices-v1") >> 9L
        remoteConsumers.isLegacyGroupTopic("prices-v1", "group-a:prices-v1") >> true
        remoteConsumers.getConsumerGroupTopicPairs("prices-v1") >> []

        when:
        def done = service.checkReplayProgress("prices-v1", context)

        then:
        !done
        0 * remoteConsumers.allConsumersCaughtUp(_, _)
        0 * refreshLogger.logReplayProgress(_)
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
        1 * metrics.recordReplayStarted("prices-v1", "group-a:prices-v1", "refresh-2")
    }

    def "startReplayForConsumer swallows replay startup exceptions"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setRefreshId("refresh-3")
        metrics.recordReplayStarted("prices-v1", "group-a:prices-v1", "refresh-3") >> { throw new RuntimeException("boom") }

        when:
        service.startReplayForConsumer("client-a", "prices-v1", "group-a:prices-v1", context)

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
