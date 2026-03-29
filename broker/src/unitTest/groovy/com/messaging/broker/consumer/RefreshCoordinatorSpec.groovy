package com.messaging.broker.consumer

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.monitoring.RefreshEventLogger
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.common.api.PipeConnector
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class RefreshCoordinatorSpec extends Specification {

    def "startRefresh with no consumers returns completed result"() {
        given:
        def remoteConsumers = Stub(ConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> ([] as Set)
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics)
        def stateStore = Mock(RefreshStateStore) {
            loadAllRefreshes() >> ([:] as Map)
        }
        def refreshLogger = Mock(RefreshEventLogger)

        def stateMachine = new RefreshStateMachine()
        def initiationService = new RefreshInitiator(remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger)
        def ackStoreMock = Mock(RocksDbAckStore)
        def resetService = new RefreshResetService(remoteConsumers, metrics, stateStore, refreshLogger, ackStoreMock)
        def replayService = new RefreshReplayService(remoteConsumers, metrics, refreshLogger)
        def readyService = new RefreshReadyService(remoteConsumers, pipeConnector, metrics, stateStore, refreshLogger)
        def recoveryService = new RefreshRecoveryService(remoteConsumers, pipeConnector, metrics, stateStore, resetService, refreshLogger)

        def gatePolicy = Mock(com.messaging.broker.consumer.RefreshGatePolicy)
        def batchDeliveryService = Mock(com.messaging.broker.consumer.BatchDeliveryService)
        def coordinator = new RefreshCoordinator(initiationService, resetService, replayService, readyService, recoveryService, stateMachine, gatePolicy, batchDeliveryService, remoteConsumers)

        when:
        def result = coordinator.startRefresh("topic").get(2, TimeUnit.SECONDS)

        then:
        result.isSuccess()
        result.getTopic() == "topic"
        result.getState() == RefreshState.COMPLETED
        result.getConsumerCount() == 0
        0 * pipeConnector._

        cleanup:
        coordinator.shutdown()
    }

    def "startRefresh with consumers pauses pipe and broadcasts reset"() {
        given:
        def remoteConsumers = Stub(ConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> (["groupA:topic", "groupB:topic"] as Set)
            broadcastResetToTopic(_ as String) >> { }
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics)
        def stateStore = Mock(RefreshStateStore) {
            loadAllRefreshes() >> ([:] as Map)
        }
        def refreshLogger = Mock(RefreshEventLogger)

        def stateMachine = new RefreshStateMachine()
        def initiationService = new RefreshInitiator(remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger)
        def ackStoreMock = Mock(RocksDbAckStore)
        def resetService = new RefreshResetService(remoteConsumers, metrics, stateStore, refreshLogger, ackStoreMock)
        def replayService = new RefreshReplayService(remoteConsumers, metrics, refreshLogger)
        def readyService = new RefreshReadyService(remoteConsumers, pipeConnector, metrics, stateStore, refreshLogger)
        def recoveryService = new RefreshRecoveryService(remoteConsumers, pipeConnector, metrics, stateStore, resetService, refreshLogger)

        def gatePolicy = Mock(com.messaging.broker.consumer.RefreshGatePolicy)
        def batchDeliveryService = Mock(com.messaging.broker.consumer.BatchDeliveryService)
        def coordinator = new RefreshCoordinator(initiationService, resetService, replayService, readyService, recoveryService, stateMachine, gatePolicy, batchDeliveryService, remoteConsumers)

        when:
        def result = coordinator.startRefresh("topic").get(2, TimeUnit.SECONDS)

        then:
        result.isSuccess()
        result.getState() == RefreshState.RESET_SENT
        1 * metrics.resetMetricsForNewRefresh()
        1 * metrics.recordRefreshStarted("topic", "LOCAL", _ as String)
        2 * metrics.recordResetSent("topic", _ as String, _ as String)
        1 * pipeConnector.pausePipeCalls()
        1 * stateStore.saveState(_ as RefreshContext)

        cleanup:
        coordinator.shutdown()
    }

    def "handleResetAck transitions to replaying and resets offset"() {
        given:
        def remoteConsumers = Mock(ConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> (["groupA:topic"] as Set)
            broadcastResetToTopic(_ as String) >> { }
            resetConsumerOffset(_ as String, _ as String, _ as String, _ as Long) >> { }
        }
        def pipeConnector = Mock(PipeConnector)
        def metrics = Mock(DataRefreshMetrics) {
            resetMetricsForNewRefresh() >> { }
            recordRefreshStarted(_ as String, _ as String, _ as String) >> { }
            recordResetSent(_ as String, _ as String, _ as String) >> { }
            recordResetAckReceived(_ as String, _ as String, _ as String) >> { }
        }
        def stateStore = Mock(RefreshStateStore) {
            loadAllRefreshes() >> ([:] as Map)
            saveState(_ as RefreshContext) >> { }
        }
        def refreshLogger = Mock(RefreshEventLogger)

        def stateMachine = new RefreshStateMachine()
        def initiationService = new RefreshInitiator(remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger)
        def ackStoreMock = Mock(RocksDbAckStore)
        def resetService = new RefreshResetService(remoteConsumers, metrics, stateStore, refreshLogger, ackStoreMock)
        def replayService = new RefreshReplayService(remoteConsumers, metrics, refreshLogger)
        def readyService = new RefreshReadyService(remoteConsumers, pipeConnector, metrics, stateStore, refreshLogger)
        def recoveryService = new RefreshRecoveryService(remoteConsumers, pipeConnector, metrics, stateStore, resetService, refreshLogger)

        def gatePolicy = Mock(com.messaging.broker.consumer.RefreshGatePolicy)
        def batchDeliveryService = Mock(com.messaging.broker.consumer.BatchDeliveryService)
        def coordinator = new RefreshCoordinator(initiationService, resetService, replayService, readyService, recoveryService, stateMachine, gatePolicy, batchDeliveryService, remoteConsumers)
        coordinator.startRefresh("topic").get(2, TimeUnit.SECONDS)

        when:
        coordinator.handleResetAck("groupA:topic", "client-1", "topic", "trace-1")

        then:
        coordinator.getRefreshStatus("topic").getState() == RefreshState.REPLAYING

        cleanup:
        coordinator.shutdown()
    }

    def "handleReadyAck completes refresh and resumes pipe"() {
        given:
        def remoteConsumers = Mock(ConsumerRegistry) {
            getGroupTopicIdentifiers(_ as String) >> (["groupA:topic"] as Set)
            broadcastResetToTopic(_ as String) >> { }
            sendReadyToAckedConsumers(_ as String, _ as Set) >> { }
            resetConsumerOffset(_ as String, _ as String, _ as String, _ as Long) >> { }
        }
        def pipeConnector = Mock(PipeConnector) {
            pausePipeCalls() >> { }
            resumePipeCalls() >> { }
        }
        def metrics = Mock(DataRefreshMetrics) {
            resetMetricsForNewRefresh() >> { }
            recordRefreshStarted(_ as String, _ as String, _ as String) >> { }
            recordResetSent(_ as String, _ as String, _ as String) >> { }
            recordReadyAckReceived(_ as String, _ as String, _ as String) >> { }
            recordRefreshCompleted(_ as String, _ as String, _ as String, _ as String, _ as RefreshContext) >> { }
            recordReadySent(_ as String, _ as String, _ as String) >> { }
        }
        def stateStore = Mock(RefreshStateStore) {
            loadAllRefreshes() >> ([:] as Map)
            saveState(_ as RefreshContext) >> { }
            clearState(_ as String) >> { }
        }
        def refreshLogger = Mock(RefreshEventLogger)

        def stateMachine = new RefreshStateMachine()
        def initiationService = new RefreshInitiator(remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger)
        def ackStoreMock = Mock(RocksDbAckStore)
        def resetService = new RefreshResetService(remoteConsumers, metrics, stateStore, refreshLogger, ackStoreMock)
        def replayService = new RefreshReplayService(remoteConsumers, metrics, refreshLogger)
        def readyService = new RefreshReadyService(remoteConsumers, pipeConnector, metrics, stateStore, refreshLogger)
        def recoveryService = new RefreshRecoveryService(remoteConsumers, pipeConnector, metrics, stateStore, resetService, refreshLogger)

        def gatePolicy = Mock(com.messaging.broker.consumer.RefreshGatePolicy)
        def batchDeliveryService = Mock(com.messaging.broker.consumer.BatchDeliveryService)
        def coordinator = new RefreshCoordinator(initiationService, resetService, replayService, readyService, recoveryService, stateMachine, gatePolicy, batchDeliveryService, remoteConsumers)
        coordinator.startRefresh("topic").get(2, TimeUnit.SECONDS)
        def context = coordinator.getRefreshStatus("topic")
        context.setState(RefreshState.READY_SENT)

        when:
        coordinator.handleReadyAck("groupA:topic", "topic", "trace-1")

        then:
        coordinator.getRefreshStatus("topic").getState() == RefreshState.COMPLETED

        cleanup:
        coordinator.shutdown()
    }
}
