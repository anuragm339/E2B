package com.messaging.broker.consumer

import com.messaging.broker.ack.AckReconciliationScheduler
import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import spock.lang.Specification

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch

class RefreshResetServiceSpec extends Specification {

    ConsumerRegistry           remoteConsumers          = Mock()
    DataRefreshMetrics         metrics                  = Mock()
    RefreshStateStore          stateStore               = Mock()
    RefreshEventLogger         refreshLogger            = Mock()
    RocksDbAckStore            ackStore                 = Mock()
    AckReconciliationScheduler reconciliationScheduler  = Mock()

    RefreshResetService service = new RefreshResetService(
            remoteConsumers, metrics, stateStore, refreshLogger, ackStore, reconciliationScheduler)

    // ── sendReset ─────────────────────────────────────────────────────────────

    def "sendReset pauses reconciliation before clearing RocksDB — prevents false-positive missing counts"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1", "group-b:prices-v1"] as Set)
        context.setState(RefreshState.RESET_SENT)
        context.setRefreshId("refresh-1")

        when:
        service.sendReset("prices-v1", context)

        then: "reconciliation paused once for the topic (before RocksDB is wiped)"
        1 * reconciliationScheduler.pauseForTopic("prices-v1")

        then: "RocksDB cleared for each group being refreshed"
        1 * ackStore.clearByTopicAndGroup("prices-v1", "group-a")
        1 * ackStore.clearByTopicAndGroup("prices-v1", "group-b")

        then: "RESET broadcast goes out after the wipe"
        1 * remoteConsumers.broadcastResetToTopic("prices-v1")
    }

    def "sendReset only clears groups subscribed to the refreshed topic — other groups untouched"() {
        given: "only group-a is subscribed to prices-v1"
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setState(RefreshState.RESET_SENT)
        context.setRefreshId("refresh-2")

        when:
        service.sendReset("prices-v1", context)

        then: "exactly one wipe — for group-a on prices-v1 only; no other groups or topics cleared"
        1 * ackStore.clearByTopicAndGroup("prices-v1", "group-a")
        0 * ackStore.clearByTopicAndGroup("prices-v1", { it != "group-a" })
        0 * ackStore.clearByTopicAndGroup({ it != "prices-v1" }, _)
    }

    // ── handleResetAck ────────────────────────────────────────────────────────

    def "handleResetAck returns false for unexpected consumer"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.setState(RefreshState.RESET_SENT)

        when:
        def result = service.handleResetAck("unknown:topic", "client-x", "topic", context, "trace-1")

        then:
        !result
    }

    def "handleResetAck returns false for duplicate ack from same consumer"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.setState(RefreshState.RESET_SENT)
        service.handleResetAck("groupA:topic", "client-1", "topic", context, "trace-1")

        when:
        def result = service.handleResetAck("groupA:topic", "client-1", "topic", context, "trace-2")

        then:
        !result
    }

    def "handleResetAck returns true on first valid ack (single-consumer topic)"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.setState(RefreshState.RESET_SENT)

        when:
        def result = service.handleResetAck("groupA:topic", "client-1", "topic", context, "trace-1")

        then:
        result
        context.getReceivedResetAcks().contains("groupA:topic")
    }

    // ── Concurrency: Fix 5 ───────────────────────────────────────────────────

    def "simultaneous RESET_ACKs from two different consumers — exactly one drives REPLAYING transition"() {
        // Before the fix, RefreshResetService checked receivedResetAcks.size() == 1 after add().
        // With two concurrent ACKs: both threads call add() (making size == 2), then both check
        // size() == 1 — neither branch triggers, the refresh gets stuck in RESET_SENT forever.
        // The fix uses markFirstResetAck() — a CAS on an AtomicBoolean — so exactly one thread wins.
        given:
        def context = new RefreshContext("topic", ["groupA:topic", "groupB:topic"] as Set)
        context.setState(RefreshState.RESET_SENT)
        context.setRefreshId("refresh-1")

        def startLatch = new CountDownLatch(1)
        def transitionClaims = new CopyOnWriteArrayList<Boolean>()

        def threadA = Thread.start {
            startLatch.await()
            def won = service.handleResetAck("groupA:topic", "client-a", "topic", context, "trace-a")
            if (won) transitionClaims.add(true)
        }

        def threadB = Thread.start {
            startLatch.await()
            def won = service.handleResetAck("groupB:topic", "client-b", "topic", context, "trace-b")
            if (won) transitionClaims.add(true)
        }

        when:
        startLatch.countDown()
        threadA.join()
        threadB.join()

        then:
        transitionClaims.size() == 1                                    // exactly one thread drives transition
        context.getReceivedResetAcks().size() == 2                      // both ACKs were recorded
        context.getReceivedResetAcks().containsAll(["groupA:topic", "groupB:topic"])
    }

    def "getMissingResetAcks returns consumers that have not yet acked"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic", "groupB:topic", "groupC:topic"] as Set)
        context.recordResetAck("groupA:topic")

        when:
        def missing = service.getMissingResetAcks(context)

        then:
        missing == ["groupB:topic", "groupC:topic"] as Set
    }
}
