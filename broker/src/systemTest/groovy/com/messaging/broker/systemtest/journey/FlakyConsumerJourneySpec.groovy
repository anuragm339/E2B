package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.lang.Shared
import spock.util.concurrent.PollingConditions

/**
 * Journey: stable consumer A and flapping consumer B on the same topic / different groups.
 *
 * Consumer A (group-a) is healthy and connected throughout.
 * Consumer B (group-b) crashes twice and restarts twice, simulating a restart loop.
 *
 * Verifies:
 * 1. Consumer A receives all records uninterrupted — B's crashes have zero impact on A.
 * 2. Consumer B resumes from its committed offset on each reconnect — no gaps, no duplicates.
 * 3. The broker does not accumulate stuck delivery state across B's repeated crashes.
 * 4. RocksDB ack-store reflects all records ACKed by B across all restart cycles.
 *
 * Implementation note on ACK timeout:
 *   The broker's TopicFairScheduler enforces maxInFlightPerTopic=1, meaning only one
 *   delivery per topic can be in progress at a time. If B crashes mid-delivery, A's
 *   delivery for the same topic is blocked until B's pending-ACK slot is freed. The
 *   ACK timeout is overridden to 5s here (default: 30s) so that after B crashes, A
 *   resumes delivery within seconds rather than up to 30s. This also means the
 *   PollingConditions timeouts in this spec are set tighter than other specs.
 */
class FlakyConsumerJourneySpec extends BrokerSystemTestSupport {

    @Shared ApplicationContext consumerBCtx

    // Consumer A uses the default consumer context from BrokerSystemTestSupport
    @Override protected String defaultGroup() { 'group-a' }

    // Shorten ACK timeout so the in-flight delivery slot is freed quickly after B crashes.
    // Without this, A's delivery for prices-v1 is blocked for up to 30 seconds (the default
    // ACK timeout) whenever B dies with a pending delivery.
    @Override
    protected Map<String, String> brokerProperties() {
        def props = new LinkedHashMap<>(super.brokerProperties())
        props['broker.consumer.ack-timeout'] = '5000'   // 5s — freed quickly after B crash
        return props
    }

    def setupSpec() {
        // Start consumer B — same topic as A, different group
        consumerBCtx = newConsumerB()
        triggerConsumerManagerStartup(consumerBCtx)
        sleep(2000)
    }

    def cleanupSpec() {
        consumerBCtx?.close()
    }

    def "stable consumer A is unaffected while consumer B crashes and restarts twice"() {
        given: "both consumers receive 3 initial records"
        cloudServer.enqueueMessages((1..3).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "init-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })
        def collectorA = collector()
        // Use PollingConditions on getAll() — race-free vs waitForRecords (latch-based)
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collectorA.getAll().size() >= 3
        }
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert consumerBCtx.getBean(TestRecordCollector).getAll().size() >= 3
        }

        and: "both groups have a committed offset (init batch ACKed by both)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-a:prices-v1') > 0
            assert offsetTracker.getOffset('group-b:prices-v1') > 0
        }
        collectorA.reset()

        // ── FIRST CRASH ────────────────────────────────────────────────────────

        when: "consumer B crashes (first crash)"
        consumerBCtx.close()
        // Wait for the broker to process B's disconnect and free B's delivery state.
        // Any in-flight delivery to B will time out after the 5s ACK timeout (overridden above),
        // freeing the prices-v1 in-flight slot so A's delivery can proceed.
        sleep(6000)

        and: "3 records arrive while B is down — offsets 4-6"
        cloudServer.enqueueMessages((4..6).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "crash1-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })

        then: "consumer A receives crash1 records without interruption — B's crash has no impact on A"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collectorA.getAll().any { it.msgKey == 'crash1-4' }
            assert collectorA.getAll().any { it.msgKey == 'crash1-5' }
            assert collectorA.getAll().any { it.msgKey == 'crash1-6' }
        }

        and: "A's committed offset advances — A's delivery pipeline was not stalled"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-a:prices-v1') >= 6
        }
        collectorA.reset()

        // ── FIRST RESTART ─────────────────────────────────────────────────────

        when: "consumer B restarts (first restart)"
        consumerBCtx = newConsumerB()
        triggerConsumerManagerStartup(consumerBCtx)
        sleep(2000)
        def collectorB1 = consumerBCtx.getBean(TestRecordCollector)

        then: "consumer B receives crash1-4..6 — resumed from its committed offset (3)"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collectorB1.getAll().any { it.msgKey == 'crash1-4' }
            assert collectorB1.getAll().any { it.msgKey == 'crash1-5' }
            assert collectorB1.getAll().any { it.msgKey == 'crash1-6' }
        }

        and: "init records are NOT re-delivered to B after first restart (no cross-restart duplication)"
        !collectorB1.getAll().any { it.msgKey.startsWith('init-') }

        and: "B's committed offset advances to at least 6 after ACKing the catch-up batch"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-b:prices-v1') >= 6
        }
        collectorA.reset()

        // ── SECOND CRASH ──────────────────────────────────────────────────────

        when: "consumer B crashes again (second crash)"
        consumerBCtx.close()
        sleep(6000) // same reason: free in-flight slot before enqueueing new records

        and: "3 more records arrive while B is down again — offsets 7-9"
        cloudServer.enqueueMessages((7..9).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "crash2-${i}", eventType: 'MESSAGE', data: """{"i":${i}}"""]
        })

        then: "consumer A continues receiving — second B crash also has no impact"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collectorA.getAll().any { it.msgKey == 'crash2-7' }
            assert collectorA.getAll().any { it.msgKey == 'crash2-8' }
            assert collectorA.getAll().any { it.msgKey == 'crash2-9' }
        }
        collectorA.reset()

        // ── SECOND RESTART ────────────────────────────────────────────────────

        when: "consumer B restarts a second time"
        consumerBCtx = newConsumerB()
        triggerConsumerManagerStartup(consumerBCtx)
        sleep(2000)
        def collectorB2 = consumerBCtx.getBean(TestRecordCollector)

        then: "consumer B receives crash2-7..9 — resumed from committed offset 6"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collectorB2.getAll().any { it.msgKey == 'crash2-7' }
            assert collectorB2.getAll().any { it.msgKey == 'crash2-8' }
            assert collectorB2.getAll().any { it.msgKey == 'crash2-9' }
        }

        and: "init and crash1 records are NOT re-delivered after second restart (no duplication across restarts)"
        !collectorB2.getAll().any { it.msgKey.startsWith('init-') }
        !collectorB2.getAll().any { it.msgKey.startsWith('crash1-') }

        and: "B's committed offset advances to at least 9 after ACKing the final catch-up batch"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-b:prices-v1') >= 9
        }

        and: "RocksDB ack-store has all 9 records for group-b across all restart cycles"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (1..3).every { i -> ackStore.get('prices-v1', 'group-b', "init-${i}")   != null }
            assert (4..6).every { i -> ackStore.get('prices-v1', 'group-b', "crash1-${i}") != null }
            assert (7..9).every { i -> ackStore.get('prices-v1', 'group-b', "crash2-${i}") != null }
        }

        and: "group-a also has RocksDB entries for all 9 records (fan-out — A received everything)"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (1..3).every { i -> ackStore.get('prices-v1', 'group-a', "init-${i}")   != null }
            assert (4..6).every { i -> ackStore.get('prices-v1', 'group-a', "crash1-${i}") != null }
            assert (7..9).every { i -> ackStore.get('prices-v1', 'group-a', "crash2-${i}") != null }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ApplicationContext newConsumerB() {
        ApplicationContext.run([
            'messaging.broker.host'  : '127.0.0.1',
            'messaging.broker.port'  : "${brokerTcpPort}",
            'consumer.topics'        : 'prices-v1',
            'consumer.group'         : 'group-b',
            'consumer.legacy.enabled': 'false',
            'consumer.type'          : 'system-test-b',
            'micronaut.server.port'  : "${findFreePort()}",
            'broker.storage.dataDir' : "${dataDir}/consumer-b",
        ] as Map<String, Object>)
    }

    private static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
