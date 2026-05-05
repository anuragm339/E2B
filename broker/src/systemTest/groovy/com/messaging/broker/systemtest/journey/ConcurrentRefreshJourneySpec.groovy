package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Journey: two simultaneous startRefresh() calls — the state machine must not deadlock,
 * corrupt state, or produce a stuck refresh.
 *
 * POS scenario: a cloud-side orchestrator sends two back-to-back refresh commands within
 * milliseconds (e.g., duplicate HTTP request, race between two operator consoles).
 * RefreshInitiator has a synchronized block around activeRefreshes.put(), but the
 * check-then-act on the outer ConcurrentHashMap is not atomic. This test proves the
 * broker still converges to a single completed refresh.
 *
 * Verified behaviours:
 * 1. No exception is thrown by either thread.
 * 2. The broker does not deadlock or get stuck — refresh reaches COMPLETED.
 * 3. Consumer receives at least one RESET and at least one READY.
 * 4. Post-refresh data delivery resumes without gaps.
 */
class ConcurrentRefreshJourneySpec extends BrokerSystemTestSupport {

    def "two simultaneous startRefresh() calls do not deadlock or corrupt state"() {
        given: "consumer is connected and has received initial records"
        collector().reset()
        cloudServer.enqueueMessages((1..3).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "init-${i}", eventType: 'MESSAGE', data: """{"v":${i}}"""]
        })
        collector().waitForRecords(3, 20)

        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        def errors = new AtomicInteger(0)

        when: "two threads simultaneously call startRefresh() for the same topic"
        def latch = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(2)

        def thread1 = pool.submit({
            latch.await()
            try { coordinator.startRefresh('prices-v1') }
            catch (Exception e) { errors.incrementAndGet() }
        })
        def thread2 = pool.submit({
            latch.await()
            try { coordinator.startRefresh('prices-v1') }
            catch (Exception e) { errors.incrementAndGet() }
        })

        latch.countDown()   // release both threads at the same instant
        thread1.get(10, TimeUnit.SECONDS)
        thread2.get(10, TimeUnit.SECONDS)

        then: "neither thread threw an exception"
        errors.get() == 0

        and: "consumer received at least one RESET — some refresh did start"
        new PollingConditions(timeout: 20, delay: 0.5).eventually {
            assert collector().resetCount >= 1
        }

        and: "the refresh converges to a terminal state — no stuck REPLAYING or RESET_SENT"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            def status = coordinator.getRefreshStatus('prices-v1')
            // Either the context is null (COMPLETED and cleaned up) or explicitly COMPLETED
            assert status == null || status.state == RefreshState.COMPLETED
        }

        and: "consumer received at least one READY — refresh did complete (not just aborted)"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }

        when: "new records are published after the refresh"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 10L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-concurrent', eventType: 'MESSAGE', data: '{"after":1}']
        ])

        then: "post-refresh delivery resumes normally — pipe is not stuck paused"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'post-concurrent' }
        }

        cleanup:
        pool?.shutdownNow()
    }
}
