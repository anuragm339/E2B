package com.messaging.broker.consumer

import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import com.messaging.common.api.PipeConnector
import spock.lang.Specification

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ScheduledFuture

class RefreshInitiatorSpec extends Specification {

    ConsumerRegistry    remoteConsumers = Mock()
    PipeConnector       pipeConnector   = Mock()
    DataRefreshMetrics  metrics         = Mock()
    RefreshStateStore   stateStore      = Mock()
    RefreshEventLogger  refreshLogger   = Mock()
    RefreshWorkflow     stateMachine    = new RefreshStateMachine()

    RefreshInitiator initiator

    // Shared state maps wired into the initiator (as the coordinator would do)
    Map<String, RefreshContext>       activeRefreshes  = new ConcurrentHashMap<>()
    Map<String, ScheduledFuture<?>>   resetRetryTasks  = new ConcurrentHashMap<>()
    Map<String, ScheduledFuture<?>>   replayCheckTasks = new ConcurrentHashMap<>()

    def setup() {
        initiator = new RefreshInitiator(
                remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger)
        initiator.setSharedState(activeRefreshes, resetRetryTasks, replayCheckTasks)

        // Default stubs
        remoteConsumers.getGroupTopicIdentifiers("prices-v1")  >> (["group-a:prices-v1"] as Set)
        remoteConsumers.getGroupTopicIdentifiers("ref-data-v5") >> (["group-b:ref-data-v5"] as Set)
        stateStore.saveState(_) >> {}
    }

    // ── Happy-path ────────────────────────────────────────────────────────────

    def "startRefresh creates context in RESET_SENT state for known topic"() {
        when:
        initiator.startRefresh("prices-v1").get()

        then:
        activeRefreshes.containsKey("prices-v1")
        activeRefreshes["prices-v1"].state == RefreshState.RESET_SENT
        1 * pipeConnector.pausePipeCalls()
    }

    def "startRefresh skips when no consumers are registered"() {
        given:
        remoteConsumers.getGroupTopicIdentifiers("empty-topic") >> ([] as Set)

        when:
        def result = initiator.startRefresh("empty-topic").get()

        then:
        result.success
        !activeRefreshes.containsKey("empty-topic")
        0 * pipeConnector.pausePipeCalls()
    }

    def "cancelExistingRefresh removes old context and cancels scheduled tasks"() {
        given:
        initiator.startRefresh("prices-v1").get()
        def mockReset  = Mock(ScheduledFuture) { isDone() >> false }
        def mockReplay = Mock(ScheduledFuture) { isDone() >> false }
        resetRetryTasks["prices-v1"]  = mockReset
        replayCheckTasks["prices-v1"] = mockReplay

        when:
        initiator.cancelExistingRefresh("prices-v1", "test")

        then:
        !activeRefreshes.containsKey("prices-v1")
        1 * mockReset.cancel(false)
        1 * mockReplay.cancel(false)
    }

    // ── Concurrency: Fix 7 ───────────────────────────────────────────────────

    def "concurrent startRefresh for different topics share the same refreshId"() {
        // Before the fix, two concurrent startRefresh() calls could both observe
        // activeRefreshes.isEmpty() == true (before either put their context), so each
        // generated its own refreshId. The second write to currentRefreshId overwrote the
        // first, but the first topic's context already had the old refreshId — mismatched.
        // The fix moves both setRefreshId() and activeRefreshes.put() inside the
        // synchronized block, making the assignment and insertion visible as a unit.
        given:
        def startLatch = new CountDownLatch(1)
        def refreshIds = new CopyOnWriteArrayList<String>()

        def threadA = Thread.start {
            startLatch.await()
            initiator.startRefresh("prices-v1").get()
            refreshIds.add(activeRefreshes["prices-v1"]?.refreshId)
        }
        def threadB = Thread.start {
            startLatch.await()
            initiator.startRefresh("ref-data-v5").get()
            refreshIds.add(activeRefreshes["ref-data-v5"]?.refreshId)
        }

        when:
        startLatch.countDown()
        threadA.join()
        threadB.join()

        then:
        activeRefreshes.size() == 2
        // Both topics must share the same refreshId — they are part of the same refresh batch
        refreshIds.size() == 2
        refreshIds[0] != null
        refreshIds[1] != null
        refreshIds[0] == refreshIds[1]
    }

    def "isRefreshActive reflects live map state"() {
        when:
        initiator.startRefresh("prices-v1").get()

        then:
        initiator.isRefreshActive("prices-v1")
        !initiator.isRefreshActive("other-topic")
    }
}
