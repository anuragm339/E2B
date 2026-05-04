package com.messaging.broker.consumer

import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import com.messaging.common.api.PipeConnector
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.ScheduledFuture

class RefreshRecoveryServiceSpec extends Specification {

    ConsumerRegistry remoteConsumers = Mock()
    PipeConnector pipeConnector = Mock()
    DataRefreshMetrics metrics = Mock()
    RefreshStateStore stateStore = Mock()
    ResetPhase resetPhase = Mock()
    RefreshEventLogger refreshLogger = Mock()

    Map<String, RefreshContext> activeRefreshes = [:]
    Map<String, ScheduledFuture<?>> resetRetryTasks = [:]
    Map<String, ScheduledFuture<?>> replayCheckTasks = [:]
    List<String> resetRetryTopics = []
    List<String> replayTopics = []
    List<String> readyTimeoutTopics = []

    RefreshRecoveryService service = new RefreshRecoveryService(
            remoteConsumers, pipeConnector, metrics, stateStore, resetPhase, refreshLogger)

    def setup() {
        service.setSharedState(activeRefreshes, resetRetryTasks, replayCheckTasks)
        service.setSchedulingCallbacks(
                { topic -> resetRetryTopics << topic },
                { topic -> replayTopics << topic },
                { topic -> readyTimeoutTopics << topic }
        )
    }

    def "recoverAndResumeRefreshes returns empty when no saved state exists"() {
        given:
        stateStore.loadAllRefreshes() >> [:]

        when:
        def result = service.recoverAndResumeRefreshes()

        then:
        result.isEmpty()
        activeRefreshes.isEmpty()
        0 * pipeConnector.pausePipeCalls()
        0 * refreshLogger._
    }

    def "recoverAndResumeRefreshes pauses pipe assigns missing refresh id and resumes saved topics"() {
        given:
        def resetContext = refreshContext("prices-v1", RefreshState.RESET_SENT, ["group-a:prices-v1"] as Set)
        resetContext.recordShutdown(Instant.now().minusSeconds(5))
        resetContext.setRefreshId(null)

        def completedContext = refreshContext("orders-v1", RefreshState.COMPLETED, ["group-b:orders-v1"] as Set)
        completedContext.recordShutdown(Instant.now().minusSeconds(4))
        completedContext.setRefreshId("batch-2")

        stateStore.loadAllRefreshes() >> [
                "prices-v1": resetContext,
                "orders-v1": completedContext
        ]
        resetPhase.getMissingResetAcks(resetContext) >> (["group-a:prices-v1"] as Set)

        when:
        def result = service.recoverAndResumeRefreshes()

        then:
        result.size() == 2
        resetContext.refreshId
        resetContext.downtimePeriods.size() == 1
        completedContext.downtimePeriods.size() == 1
        activeRefreshes["prices-v1"].is(resetContext)
        !activeRefreshes.containsKey("orders-v1")
        resetRetryTopics == ["prices-v1"]
        replayTopics.isEmpty()
        1 * pipeConnector.pausePipeCalls()
        1 * remoteConsumers.broadcastResetToTopic("prices-v1")
        2 * stateStore.saveState(_ as RefreshContext)
        1 * stateStore.clearState("orders-v1")
        2 * refreshLogger.logStateTransition(_)
        1 * refreshLogger.logPipePaused(_)
        1 * refreshLogger.logResetSent(_)
    }

    def "resumeRefresh advances reset sent context directly to replaying when all reset acks are present"() {
        given:
        def context = refreshContext("prices-v1", RefreshState.RESET_SENT, ["group-a:prices-v1"] as Set)
        context.setRefreshId("refresh-1")
        context.setResetSentTime(Instant.now().minusSeconds(2))
        context.getReceivedResetAcks().add("group-a:prices-v1")
        context.getResetAckTimes().put("group-a:prices-v1", Instant.now().minusSeconds(1))
        activeRefreshes["prices-v1"] = context
        resetPhase.getMissingResetAcks(context) >> ([] as Set)

        when:
        service.resumeRefresh(context)

        then:
        context.state == RefreshState.REPLAYING
        replayTopics == ["prices-v1"]
        1 * stateStore.saveState(context)
        (1..2) * metrics.recordResetAckDuration("prices-v1", "group-a:prices-v1", "refresh-1", _)
        0 * remoteConsumers.broadcastResetToTopic(_)
        0 * refreshLogger.logResetSent(_)
    }

    def "resumeRefresh resends reset and schedules retry when reset acks are missing"() {
        given:
        def context = refreshContext("prices-v1", RefreshState.RESET_SENT, ["group-a:prices-v1", "group-b:prices-v1"] as Set)
        context.setRefreshId("refresh-2")
        context.setResetSentTime(Instant.now().minusSeconds(1))
        context.getReceivedResetAcks().add("group-a:prices-v1")
        context.getResetAckTimes().put("group-a:prices-v1", Instant.now())
        resetPhase.getMissingResetAcks(context) >> (["group-b:prices-v1"] as Set)

        when:
        service.resumeRefresh(context)

        then:
        resetRetryTopics == ["prices-v1"]
        1 * metrics.recordResetAckDuration("prices-v1", "group-a:prices-v1", "refresh-2", _)
        1 * metrics.recordResetSentAt("prices-v1", "group-b:prices-v1", "refresh-2", _)
        1 * remoteConsumers.broadcastResetToTopic("prices-v1")
        1 * refreshLogger.logResetSent(_)
        0 * stateStore.saveState(_)
    }

    def "resumeRefresh handles replaying ready sent completed and aborted states"() {
        given:
        def replaying = refreshContext("prices-v1", RefreshState.REPLAYING, ["group-a:prices-v1"] as Set)
        replaying.setRefreshId("refresh-3")
        replaying.setResetSentTime(Instant.now().minusSeconds(2))
        replaying.getReceivedResetAcks().add("group-a:prices-v1")
        replaying.getResetAckTimes().put("group-a:prices-v1", Instant.now().minusSeconds(1))

        def readySent = refreshContext("orders-v1", RefreshState.READY_SENT, ["group-b:orders-v1"] as Set)
        readySent.setRefreshId("refresh-4")
        readySent.setReadySentTime(Instant.now().minusSeconds(2))
        readySent.getReceivedReadyAcks().add("group-b:orders-v1")
        readySent.getReadyAckTimes().put("group-b:orders-v1", Instant.now().minusSeconds(1))

        def completed = refreshContext("users-v1", RefreshState.COMPLETED, ["group-c:users-v1"] as Set)
        completed.setRefreshId("refresh-5")
        activeRefreshes["users-v1"] = completed

        def aborted = refreshContext("inventory-v1", RefreshState.ABORTED, ["group-d:inventory-v1"] as Set)
        aborted.setRefreshId("refresh-6")

        when:
        service.resumeRefresh(replaying)
        service.resumeRefresh(readySent)
        service.resumeRefresh(completed)
        service.resumeRefresh(aborted)

        then:
        replayTopics == ["prices-v1"]
        readyTimeoutTopics == ["orders-v1"]
        !activeRefreshes.containsKey("users-v1")
        1 * stateStore.clearState("users-v1")
        1 * metrics.recordResetAckDuration("prices-v1", "group-a:prices-v1", "refresh-3", _)
        1 * metrics.recordReadyAckDuration("orders-v1", "group-b:orders-v1", "refresh-4", _)
        4 * refreshLogger.logStateTransition(_)
    }

    private static RefreshContext refreshContext(String topic, RefreshState state, Set<String> consumers) {
        def context = new RefreshContext(topic, consumers)
        context.setState(state)
        context
    }
}
