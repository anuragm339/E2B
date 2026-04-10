package com.messaging.broker.consumer

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.monitoring.RefreshEventLogger
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.common.api.PipeConnector
import spock.lang.Specification

import java.lang.reflect.Method
import java.util.concurrent.TimeUnit

class RefreshCoordinatorSpec extends Specification {

    def "init wires collaborators and triggers recovery"() {
        given:
        def gatePolicy = Mock(RefreshGatePolicy)
        def batchDeliveryService = Mock(BatchDeliveryService)
        def remoteConsumers = Mock(ConsumerRegistry)
        def recoveryService = Mock(RefreshRecovery)
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                Mock(ResetPhase),
                Mock(ReplayPhase),
                Mock(ReadyPhase),
                recoveryService,
                new RefreshStateMachine(),
                gatePolicy,
                batchDeliveryService,
                remoteConsumers
        )

        when:
        coordinator.init()

        then:
        1 * gatePolicy.setDataRefreshCoordinator(coordinator)
        1 * batchDeliveryService.setDataRefreshCoordinator(coordinator)
        1 * remoteConsumers.setRefreshCoordinator(coordinator)
        1 * recoveryService.recoverAndResumeRefreshes()

        cleanup:
        coordinator.shutdown()
    }

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

    def "handleResetAck does not transition when reset phase rejects ack"() {
        given:
        def resetService = Mock(ResetPhase)
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                resetService,
                Mock(ReplayPhase),
                Mock(ReadyPhase),
                Mock(RefreshRecovery),
                new RefreshStateMachine(),
                Mock(RefreshGatePolicy),
                Mock(BatchDeliveryService),
                Mock(ConsumerRegistry)
        )
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.setState(RefreshState.RESET_SENT)
        coordinator.@activeRefreshes.put("topic", context)
        resetService.handleResetAck("groupA:topic", "client-1", "topic", context, "trace-1") >> false

        when:
        coordinator.handleResetAck("groupA:topic", "client-1", "topic", "trace-1")

        then:
        context.state == RefreshState.RESET_SENT

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

    def "handleReadyAck leaves refresh active when more ready acks are pending"() {
        given:
        def readyService = Mock(ReadyPhase)
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                Mock(ResetPhase),
                Mock(ReplayPhase),
                readyService,
                Mock(RefreshRecovery),
                new RefreshStateMachine(),
                Mock(RefreshGatePolicy),
                Mock(BatchDeliveryService),
                Mock(ConsumerRegistry)
        )
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.setState(RefreshState.READY_SENT)
        coordinator.@activeRefreshes.put("topic", context)
        readyService.handleReadyAck("groupA:topic", "topic", context, "trace-1") >> false

        when:
        coordinator.handleReadyAck("groupA:topic", "topic", "trace-1")

        then:
        context.state == RefreshState.READY_SENT

        cleanup:
        coordinator.shutdown()
    }

    def "handleResetAck and handleReadyAck ignore unknown topics"() {
        given:
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                Mock(ResetPhase),
                Mock(ReplayPhase),
                Mock(ReadyPhase),
                Mock(RefreshRecovery),
                new RefreshStateMachine(),
                Mock(RefreshGatePolicy),
                Mock(BatchDeliveryService),
                Mock(ConsumerRegistry)
        )

        when:
        coordinator.handleResetAck("groupA:topic", "client-1", "missing", "trace-1")
        coordinator.handleReadyAck("groupA:topic", "missing", "trace-2")

        then:
        noExceptionThrown()

        cleanup:
        coordinator.shutdown()
    }

    def "late joining consumer during reset sent transitions to replaying when all reset acks are present"() {
        given:
        def initiationService = Mock(RefreshStarter)
        def resetService = Mock(ResetPhase)
        def replayService = Mock(ReplayPhase)
        def readyService = Mock(ReadyPhase)
        def recoveryService = Mock(RefreshRecovery)
        def gatePolicy = Mock(RefreshGatePolicy)
        def batchDeliveryService = Mock(BatchDeliveryService)
        def remoteConsumers = Mock(ConsumerRegistry)
        def coordinator = new RefreshCoordinator(
                initiationService, resetService, replayService, readyService, recoveryService,
                new RefreshStateMachine(), gatePolicy, batchDeliveryService, remoteConsumers)

        def context = new RefreshContext("topic", ["groupA:topic", "groupB:topic"] as Set)
        context.setState(RefreshState.RESET_SENT)
        coordinator.@activeRefreshes.put("topic", context)
        def scheduled = Mock(java.util.concurrent.ScheduledFuture)
        coordinator.@resetRetryTasks.put("topic", scheduled)

        when:
        coordinator.registerLateJoiningConsumer("topic", "groupA:topic")
        coordinator.registerLateJoiningConsumer("topic", "groupB:topic")

        then:
        context.state == RefreshState.REPLAYING
        context.receivedResetAcks == ["groupA:topic", "groupB:topic"] as Set
        1 * scheduled.cancel(false)

        cleanup:
        coordinator.shutdown()
    }

    def "late joining consumer in replaying state is recorded and ready sent state is ignored"() {
        given:
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                Mock(ResetPhase),
                Mock(ReplayPhase),
                Mock(ReadyPhase),
                Mock(RefreshRecovery),
                new RefreshStateMachine(),
                Mock(RefreshGatePolicy),
                Mock(BatchDeliveryService),
                Mock(ConsumerRegistry)
        )
        def replaying = new RefreshContext("topic", ["groupA:topic"] as Set)
        replaying.setState(RefreshState.REPLAYING)
        coordinator.@activeRefreshes.put("topic", replaying)
        def readySent = new RefreshContext("other", ["groupA:other"] as Set)
        readySent.setState(RefreshState.READY_SENT)
        coordinator.@activeRefreshes.put("other", readySent)

        when:
        coordinator.registerLateJoiningConsumer("topic", "groupA:topic")
        coordinator.registerLateJoiningConsumer("other", "groupA:other")

        then:
        replaying.receivedResetAcks.contains("groupA:topic")
        !readySent.receivedResetAcks.contains("groupA:other")

        cleanup:
        coordinator.shutdown()
    }

    def "private replay and ready timeout checks drive ready send and retry"() {
        given:
        def replayService = Mock(ReplayPhase)
        def readyService = Mock(ReadyPhase)
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                Mock(ResetPhase),
                replayService,
                readyService,
                Mock(RefreshRecovery),
                new RefreshStateMachine(),
                Mock(RefreshGatePolicy),
                Mock(BatchDeliveryService),
                Mock(ConsumerRegistry)
        )
        def replaying = new RefreshContext("topic", ["groupA:topic"] as Set)
        replaying.setState(RefreshState.REPLAYING)
        coordinator.@activeRefreshes.put("topic", replaying)
        def replayTask = Mock(java.util.concurrent.ScheduledFuture)
        coordinator.@replayCheckTasks.put("topic", replayTask)
        replayService.checkReplayProgress("topic", replaying) >> true

        when:
        invokePrivate(coordinator, "checkReplayProgress", "topic")

        then:
        1 * readyService.sendReady("topic", replaying)
        1 * replayTask.cancel(false)

        when:
        def readySent = new RefreshContext("ready-topic", ["groupA:ready-topic", "groupB:ready-topic"] as Set)
        readySent.setState(RefreshState.READY_SENT)
        readySent.recordReadyAck("groupA:ready-topic")
        coordinator.@activeRefreshes.put("ready-topic", readySent)
        invokePrivate(coordinator, "checkReadyAckTimeout", "ready-topic")

        then:
        coordinator.getRefreshStatus("ready-topic") == readySent
        1 * readyService.checkReadyAckTimeout("ready-topic", _ as RefreshContext)

        cleanup:
        coordinator.shutdown()
    }

    def "status and abort helpers reflect active refresh state"() {
        given:
        def coordinator = new RefreshCoordinator(
                Mock(RefreshStarter),
                Mock(ResetPhase),
                Mock(ReplayPhase),
                Mock(ReadyPhase),
                Mock(RefreshRecovery),
                new RefreshStateMachine(),
                Mock(RefreshGatePolicy),
                Mock(BatchDeliveryService),
                Mock(ConsumerRegistry)
        )
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-1")
        coordinator.@activeRefreshes.put("topic", context)
        def replayTask = Mock(java.util.concurrent.ScheduledFuture)
        coordinator.@replayCheckTasks.put("topic", replayTask)

        expect:
        coordinator.isRefreshInProgress()
        coordinator.isRefreshActive("topic")
        coordinator.getRefreshIdForTopic("topic") == "refresh-1"
        coordinator.getRefreshTypeForTopic("topic") == "LOCAL"
        coordinator.getCurrentRefreshTopic() == "topic"
        coordinator.getCurrentRefreshContext() == context

        when:
        invokePrivate(coordinator, "abortRefreshIfStuck", "topic")

        then:
        !coordinator.isRefreshActive("topic")
        1 * replayTask.cancel(false)

        cleanup:
        coordinator.shutdown()
    }

    private static Object invokePrivate(Object target, String methodName, Object... args) {
        Method method = target.class.getDeclaredMethod(methodName, args.collect { it.class } as Class[])
        method.accessible = true
        method.invoke(target, args)
    }
}
