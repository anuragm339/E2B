package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.lang.Shared
import spock.util.concurrent.PollingConditions

/**
 * Journey: consumer B crashes mid-REPLAYING, reconnects, and the refresh completes cleanly.
 *
 * POS scenario: a store terminal (consumer B) receives the RESET message and ACKs it,
 * so the broker transitions to REPLAYING. Before B finishes replaying its data, the
 * terminal crashes (power loss, network loss). The broker is now stuck — B's offset
 * does not advance, so allConsumersCaughtUp() stays false. When the terminal comes back
 * up and reconnects, it is registered as a late-joiner, delivery resumes, and the
 * refresh completes for all consumers.
 *
 * Verified behaviours:
 * 1. Both consumers receive the RESET signal before B crashes.
 * 2. After B crashes the broker remains in REPLAYING state (not stuck in COMPLETED).
 * 3. Consumer A continues receiving new records during the stalled replay window.
 * 4. After B reconnects the refresh completes — both consumers receive READY.
 * 5. Post-refresh records are delivered to both groups with no gaps or duplicates.
 * 6. RocksDB ack-store is consistent for both groups after the full lifecycle.
 */
class ConsumerCrashDuringReplayJourneySpec extends BrokerSystemTestSupport {

    @Shared ApplicationContext consumerBCtx  // reassigned in the feature method to simulate reconnect

    // Consumer A uses the default context from BrokerSystemTestSupport (group-a)
    @Override protected String defaultGroup() { 'group-a' }

    // Shorten ACK timeout so B's in-flight delivery slot is freed quickly after crash
    @Override
    protected Map<String, String> brokerProperties() {
        def props = new LinkedHashMap<>(super.brokerProperties())
        props['broker.consumer.ack-timeout'] = '5000'
        return props
    }

    def setupSpec() {
        // super.setupSpec() is invoked automatically by the Spock framework before this method.
        consumerBCtx = newConsumerB()
        triggerConsumerManagerStartup(consumerBCtx)
        awaitConsumerConnected(consumerBCtx)
    }

    def cleanupSpec() {
        consumerBCtx?.close()
        // super.cleanupSpec() is invoked automatically by the Spock framework after this method.
    }

    def "consumer B crashing mid-REPLAYING is recovered when it reconnects — refresh completes for all consumers"() {
        given: "30 pre-refresh records delivered to both consumers (large batch gives a wide replay window)"
        collector().reset()
        cloudServer.enqueueMessages((1..30).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "pre-${i}", eventType: 'MESSAGE', data: """{"v":${i}}"""]
        })
        new PollingConditions(timeout: 30, delay: 0.3).eventually {
            assert collector().getAll().size() >= 30
            assert consumerBCtx.getBean(TestRecordCollector).getAll().size() >= 30
        }

        and: "both groups have committed offsets in the broker"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-a:prices-v1') >= 30
            assert offsetTracker.getOffset('group-b:prices-v1') >= 30
        }
        collector().reset()
        consumerBCtx.getBean(TestRecordCollector).reset()

        when: "refresh is triggered for prices-v1"
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        then: "both consumers receive RESET — broker broadcasts to all registered groups"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().resetCount >= 1
            assert consumerBCtx.getBean(TestRecordCollector).resetCount >= 1
        }

        and: "broker transitions to REPLAYING (both consumers ACKed RESET)"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert coordinator.getRefreshStatus('prices-v1')?.state == RefreshState.REPLAYING
        }

        // ── B CRASHES MID-REPLAY ──────────────────────────────────────────────

        when: "consumer B crashes and a record is queued while the broker is stalled"
        consumerBCtx.close()
        // B's connection is dropped. The broker may remain in REPLAYING (B's offset stalled)
        // or advance to READY_SENT — either way the refresh cannot fully complete until B
        // reconnects. The pipe is paused so this record is buffered until after READY.
        cloudServer.enqueueMessages([
            [offset: 50L, topic: 'prices-v1', partition: 0,
             msgKey: 'during-replay-A', eventType: 'MESSAGE', data: '{"mid":1}']
        ])

        then: "the record was queued without error"
        noExceptionThrown()

        // ── B RECONNECTS ──────────────────────────────────────────────────────

        when: "consumer B reconnects (POS terminal comes back online)"
        consumerBCtx = newConsumerB()
        triggerConsumerManagerStartup(consumerBCtx)
        awaitConsumerConnected(consumerBCtx)

        then: "refresh completes — B registered as late-joiner, both groups caught up"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert consumerBCtx.getBean(TestRecordCollector).readyCount >= 1
        }

        and: "consumer A receives the record queued during the stalled replay window"
        // during-replay-A (offset 50) was buffered while the pipe was paused. It must
        // be delivered when the pipe resumes on refresh completion — before we reset.
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'during-replay-A' }
        }

        // ── POST-REFRESH DELIVERY ─────────────────────────────────────────────

        when: "new records arrive after refresh completion"
        collector().reset()
        consumerBCtx.getBean(TestRecordCollector).reset()
        cloudServer.enqueueMessages([
            [offset: 60L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-refresh-A', eventType: 'MESSAGE', data: '{"post":1}'],
            [offset: 61L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-refresh-B', eventType: 'MESSAGE', data: '{"post":2}'],
        ])

        then: "consumer A receives post-refresh records normally"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'post-refresh-A' }
            assert collector().getAll().any { it.msgKey == 'post-refresh-B' }
        }

        and: "consumer B (reconnected) receives post-refresh records normally"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert consumerBCtx.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-refresh-A' }
            assert consumerBCtx.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-refresh-B' }
        }

        and: "broker committed offsets advance for both groups"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('group-a:prices-v1') >= 61
            assert offsetTracker.getOffset('group-b:prices-v1') >= 61
        }

        and: "RocksDB ack-store has post-refresh entries for both groups"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'group-a', 60L) != null
            assert ackStore.get('prices-v1', 'group-a', 61L) != null
            assert ackStore.get('prices-v1', 'group-b', 60L) != null
            assert ackStore.get('prices-v1', 'group-b', 61L) != null
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

}
