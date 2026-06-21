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
    RefreshReplayWindowResolver replayWindowResolver = Mock()
    com.messaging.broker.legacy.LegacyClientConfig legacyClientConfig = Mock()  // getServiceTopics() defaults to null

    RefreshInitiator initiator

    // Shared state maps wired into the initiator (as the coordinator would do)
    Map<String, RefreshContext>       activeRefreshes   = new ConcurrentHashMap<>()
    Map<String, ScheduledFuture<?>>   resetRetryTasks   = new ConcurrentHashMap<>()
    Map<String, ScheduledFuture<?>>   replayCheckTasks  = new ConcurrentHashMap<>()
    Map<String, ScheduledFuture<?>>   abortWatchdogTasks = new ConcurrentHashMap<>()
    Map<String, ScheduledFuture<?>>   readyTimeoutTasks  = new ConcurrentHashMap<>()

    def setup() {
        initiator = new RefreshInitiator(
                remoteConsumers, pipeConnector, metrics, stateMachine, stateStore, refreshLogger,
                replayWindowResolver, legacyClientConfig)
        initiator.setSharedState(activeRefreshes, resetRetryTasks, replayCheckTasks, abortWatchdogTasks, readyTimeoutTasks)

        // Default stubs
        remoteConsumers.getGroupTopicIdentifiers("prices-v1")  >> (["group-a:prices-v1"] as Set)
        remoteConsumers.getGroupTopicIdentifiers("ref-data-v5") >> (["group-b:ref-data-v5"] as Set)
        replayWindowResolver.resolve(_ as String, _) >> new RefreshReplayWindowResolver.RefreshReplayWindow(0L, Long.MIN_VALUE, null)
        stateStore.saveState(_) >> {}
    }

    def "getExpectedConsumers includes CONFIGURED groups so a cold-boot refresh waits instead of skipping"() {
        given: "no consumer registered at runtime yet, but config maps the service to this topic"
        remoteConsumers.getGroupTopicIdentifiers("minimum-price") >> ([] as Set)
        legacyClientConfig.getServiceTopics() >> ["price-quote": ["minimum-price", "prices-v1"]]

        expect: "the configured group:topic is expected, so the refresh is not skipped"
        initiator.getExpectedConsumers("minimum-price") == (["price-quote:minimum-price"] as Set)
    }

    def "getExpectedConsumers prefers runtime consumers and ignores config when any are connected"() {
        given: "a consumer is connected with a different group than the configured one"
        remoteConsumers.getGroupTopicIdentifiers("minimum-price") >> (["dynamic-grp:minimum-price"] as Set)
        legacyClientConfig.getServiceTopics() >> ["price-quote": ["minimum-price"]]

        expect: "only the connected consumer is expected — we don't wait on a configured-but-absent group"
        initiator.getExpectedConsumers("minimum-price") == (["dynamic-grp:minimum-price"] as Set)
    }

    // ── Happy-path ────────────────────────────────────────────────────────────

    def "startRefresh creates context in RESET_SENT state without pausing pipe"() {
        when:
        initiator.startRefresh("prices-v1").get()

        then:
        activeRefreshes.containsKey("prices-v1")
        activeRefreshes["prices-v1"].state == RefreshState.RESET_SENT
        0 * pipeConnector.pausePipeCalls()
    }

    def "startRefresh captures replay window offsets on context"() {
        given:
        def cutoff = java.time.Instant.parse("2026-06-19T00:00:00Z")
        when:
        initiator.startRefresh("prices-v1").get()

        then:
        1 * replayWindowResolver.resolve("prices-v1", _) >> new RefreshReplayWindowResolver.RefreshReplayWindow(200L, 350L, cutoff)
        activeRefreshes["prices-v1"].replayStartOffset == 200L
        activeRefreshes["prices-v1"].replayTargetOffset == 350L
        activeRefreshes["prices-v1"].replayCutoffTime == cutoff
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

    def "cancelExistingRefresh removes old context and cancels all scheduled tasks"() {
        given:
        initiator.startRefresh("prices-v1").get()
        def mockReset    = Mock(ScheduledFuture) { isDone() >> false }
        def mockReplay   = Mock(ScheduledFuture) { isDone() >> false }
        def mockWatchdog = Mock(ScheduledFuture) { isDone() >> false }
        def mockReady    = Mock(ScheduledFuture) { isDone() >> false }
        resetRetryTasks["prices-v1"]    = mockReset
        replayCheckTasks["prices-v1"]   = mockReplay
        abortWatchdogTasks["prices-v1"] = mockWatchdog
        readyTimeoutTasks["prices-v1"]  = mockReady

        when:
        initiator.cancelExistingRefresh("prices-v1", "test")

        then:
        !activeRefreshes.containsKey("prices-v1")
        1 * mockReset.cancel(false)
        1 * mockReplay.cancel(false)
        1 * mockWatchdog.cancel(false)
        1 * mockReady.cancel(false)
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
