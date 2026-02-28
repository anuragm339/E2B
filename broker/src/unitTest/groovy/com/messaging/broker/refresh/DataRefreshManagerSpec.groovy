package com.messaging.broker.refresh

import com.messaging.broker.consumer.AdaptiveBatchDeliveryManager
import com.messaging.broker.consumer.RemoteConsumerRegistry
import com.messaging.broker.metrics.DataRefreshMetrics
import com.messaging.common.api.PipeConnector
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class DataRefreshManagerSpec extends Specification {

    def "startRefresh with no consumers returns completed result"() {
        given:
        def remoteConsumers = Stub(RemoteConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> ([] as Set)
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics)
        def stateStore = Mock(DataRefreshStateStore)
        def adaptiveManager = Mock(AdaptiveBatchDeliveryManager)
        def manager = new DataRefreshManager(remoteConsumers, pipeConnector, new DataRefreshConfiguration(),
            stateStore, metrics, adaptiveManager)

        when:
        def result = manager.startRefresh("topic").get(2, TimeUnit.SECONDS)

        then:
        result.isSuccess()
        result.getTopic() == "topic"
        result.getState() == DataRefreshState.COMPLETED
        result.getConsumerCount() == 0
        0 * pipeConnector._
        0 * metrics._

        cleanup:
        manager.shutdown()
    }

    def "startRefresh with consumers pauses pipe and broadcasts reset"() {
        given:
        def remoteConsumers = Stub(RemoteConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> (["groupA:topic", "groupB:topic"] as Set)
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics)
        def stateStore = Mock(DataRefreshStateStore)
        def adaptiveManager = Mock(AdaptiveBatchDeliveryManager)
        def manager = new DataRefreshManager(remoteConsumers, pipeConnector, new DataRefreshConfiguration(),
            stateStore, metrics, adaptiveManager)

        when:
        def result = manager.startRefresh("topic").get(2, TimeUnit.SECONDS)

        then:
        result.isSuccess()
        result.getState() == DataRefreshState.RESET_SENT
        1 * metrics.resetMetricsForNewRefresh()
        1 * metrics.recordRefreshStarted("topic", "LOCAL", _ as String)
        2 * metrics.recordResetSent("topic", _ as String, _ as String)
        1 * pipeConnector.pausePipeCalls()
        1 * stateStore.saveState(_ as DataRefreshContext)

        cleanup:
        manager.shutdown()
    }

    def "handleResetAck transitions to replaying and resets offset"() {
        given:
        def remoteConsumers = Mock(RemoteConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> (["groupA:topic"] as Set)
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics)
        def stateStore = Mock(DataRefreshStateStore)
        def adaptiveManager = Mock(AdaptiveBatchDeliveryManager)
        def manager = new DataRefreshManager(remoteConsumers, pipeConnector, new DataRefreshConfiguration(),
            stateStore, metrics, adaptiveManager)
        manager.startRefresh("topic").get(2, TimeUnit.SECONDS)

        when:
        manager.handleResetAck("groupA:topic", "client-1", "topic")

        then:
        1 * remoteConsumers.resetConsumerOffset("client-1", "topic", "groupA", 0L)
        1 * metrics.recordResetAckReceived("topic", "groupA:topic", _ as String)
        (2.._) * stateStore.saveState(_ as DataRefreshContext)
        manager.getRefreshStatus("topic").getState() == DataRefreshState.REPLAYING

        cleanup:
        manager.shutdown()
    }

    def "handleReadyAck completes refresh and resumes pipe"() {
        given:
        def remoteConsumers = Mock(RemoteConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> (["groupA:topic"] as Set)
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics)
        def stateStore = Mock(DataRefreshStateStore)
        def adaptiveManager = Mock(AdaptiveBatchDeliveryManager)
        def manager = new DataRefreshManager(remoteConsumers, pipeConnector, new DataRefreshConfiguration(),
            stateStore, metrics, adaptiveManager)
        manager.startRefresh("topic").get(2, TimeUnit.SECONDS)
        def context = manager.getRefreshStatus("topic")
        context.setState(DataRefreshState.READY_SENT)

        when:
        manager.handleReadyAck("groupA:topic", "topic")

        then:
        1 * metrics.recordReadyAckReceived("topic", "groupA:topic", _ as String)
        1 * metrics.recordRefreshCompleted("topic", "LOCAL", "SUCCESS", _ as String, _ as DataRefreshContext)
        1 * pipeConnector.resumePipeCalls()
        (2.._) * stateStore.saveState(_ as DataRefreshContext)
        1 * stateStore.clearState("topic")
        manager.getRefreshStatus("topic").getState() == DataRefreshState.COMPLETED

        cleanup:
        manager.shutdown()
    }
}
