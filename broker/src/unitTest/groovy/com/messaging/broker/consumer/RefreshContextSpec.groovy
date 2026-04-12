package com.messaging.broker.consumer

import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch

class RefreshContextSpec extends Specification {

    def "recordResetAck marks consumer replaying and offset 0"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)

        when:
        context.recordResetAck("groupA:topic")

        then:
        context.getReceivedResetAcks().contains("groupA:topic")
        context.getConsumerOffsets().get("groupA:topic") == 0L
        context.isConsumerReplaying("groupA:topic")
        context.getResetAckTimes().get("groupA:topic") != null
    }

    def "recordReadyAck marks consumer not replaying"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        context.recordResetAck("groupA:topic")

        when:
        context.recordReadyAck("groupA:topic")

        then:
        context.getReceivedReadyAcks().contains("groupA:topic")
        !context.isConsumerReplaying("groupA:topic")
        context.getReadyAckTimes().get("groupA:topic") != null
    }

    def "downtime tracking aggregates periods"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        def shutdown = Instant.parse("2025-01-01T00:00:00Z")
        def startup = Instant.parse("2025-01-01T00:10:00Z")

        when:
        context.recordShutdown(shutdown)
        context.recordStartup(startup)

        then:
        context.getDowntimePeriods().size() == 1
        context.getTotalDowntimeSeconds() == 600
    }

    // ── Concurrency: Fix 4 ───────────────────────────────────────────────────

    def "concurrent recordShutdown and recordStartup do not throw ConcurrentModificationException"() {
        // Verifies downtimePeriods uses CopyOnWriteArrayList, not ArrayList.
        // Before the fix, a monitoring thread iterating getDowntimePeriods() while a recovery
        // thread called recordStartup() could throw ConcurrentModificationException.
        given:
        def context = new RefreshContext("topic", ["groupA:topic"] as Set)
        def errors = new CopyOnWriteArrayList<Throwable>()
        def startLatch = new CountDownLatch(1)

        // Writer thread: repeatedly records shutdown → startup cycles
        def writer = Thread.start {
            startLatch.await()
            50.times {
                context.recordShutdown(Instant.now())
                context.recordStartup(Instant.now())
            }
        }

        // Reader threads: iterate the downtime list while writer is active
        def readers = (1..5).collect {
            Thread.start {
                startLatch.await()
                try {
                    50.times { context.getDowntimePeriods().size() }
                } catch (Throwable t) {
                    errors.add(t)
                }
            }
        }

        when:
        startLatch.countDown()
        writer.join()
        readers*.join()

        then:
        errors.isEmpty()
        context.getDowntimePeriods().size() == 50
    }

    // ── Concurrency: Fix 5 ───────────────────────────────────────────────────

    def "markFirstResetAck returns true exactly once under concurrent calls"() {
        // Verifies the AtomicBoolean CAS: when many threads race to claim the
        // RESET_SENT → REPLAYING transition, exactly one succeeds.
        // Before the fix, ConcurrentHashSet.add() + size()==1 was not atomic,
        // so two simultaneous ACKs could both miss the transition.
        given:
        def context = new RefreshContext("topic", ["a:t", "b:t", "c:t", "d:t", "e:t"] as Set)
        def startLatch = new CountDownLatch(1)
        def winners = new CopyOnWriteArrayList<Boolean>()

        def threads = (1..10).collect {
            Thread.start {
                startLatch.await()
                def won = context.markFirstResetAck()
                if (won) winners.add(true)
            }
        }

        when:
        startLatch.countDown()
        threads*.join()

        then:
        winners.size() == 1
    }

    def "all reset and ready acks checks reflect expected consumers"() {
        given:
        def context = new RefreshContext("topic", ["groupA:topic", "groupB:topic"] as Set)

        when:
        context.recordResetAck("groupA:topic")
        context.recordReadyAck("groupA:topic")

        then:
        !context.allResetAcksReceived()
        !context.allReadyAcksReceived()

        when:
        context.recordResetAck("groupB:topic")
        context.recordReadyAck("groupB:topic")

        then:
        context.allResetAcksReceived()
        context.allReadyAcksReceived()
    }
}
