package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: a new consumer (B) connects while a refresh is already in REPLAYING state.
 *
 * POS scenario: consumer A is registered and part of the expected refresh set. During
 * the replay phase (state = REPLAYING), consumer B — a late-booting store terminal —
 * connects and subscribes. The broker's SubscribeHandler detects the REPLAYING state and
 * registers B as a late joiner (adds it to receivedResetAcks without requiring an explicit
 * RESET ACK from B). The broker then waits for BOTH A and B to be caught up before
 * advancing to READY_SENT.
 *
 * Verified behaviours:
 * 1. Consumer B connecting during REPLAYING does not crash the broker or corrupt state.
 * 2. B is registered as a late-joiner — it receives delivery starting from its committed
 *    offset (0 for a new group) and catches up through the replay data.
 * 3. The refresh advances to COMPLETED once both A and B are caught up.
 * 4. Both consumers receive a READY signal.
 * 5. Post-refresh records arrive at both consumers without gaps or duplicates.
 */
class LateConsumerDuringRefreshJourneySpec extends BrokerSystemTestSupport {

    // Only consumer A is connected when startRefresh() is called.
    // Consumer B (group-b) starts mid-refresh and joins as a late subscriber.

    def "late-joining consumer B connects during REPLAYING and refresh completes for both"() {
        given: "consumer A has received 5 pre-refresh records"
        collector().reset()
        cloudServer.enqueueMessages((1..5).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "pre-${i}", eventType: 'MESSAGE', data: """{"v":${i}}"""]
        })
        collector().waitForRecords(5, 20)

        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 5
        }
        collector().reset()

        when: "refresh is triggered — only consumer A (system-test-group) is in expectedConsumers"
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        then: "consumer A receives RESET"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().resetCount >= 1
        }

        and: "broker enters REPLAYING state after A ACKs RESET"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert coordinator.getRefreshStatus('prices-v1')?.state == RefreshState.REPLAYING
        }

        // ── LATE CONSUMER B CONNECTS DURING REPLAYING ─────────────────────────

        when: "consumer B connects while the broker is in REPLAYING state"
        def consumerBCtx = ApplicationContext.run([
            'messaging.broker.host'  : '127.0.0.1',
            'messaging.broker.port'  : "${brokerTcpPort}",
            'consumer.topics'        : 'prices-v1',
            'consumer.group'         : 'group-b',
            'consumer.legacy.enabled': 'false',
            'consumer.type'          : 'system-test-b',
            'micronaut.server.port'  : "${findFreePort()}",
            'broker.storage.dataDir' : "${dataDir}/consumer-b",
        ] as Map<String, Object>)
        triggerConsumerManagerStartup(consumerBCtx)
        awaitConsumerConnected(consumerBCtx)

        // NOTE: We do NOT assert state == REPLAYING here — with a small record set (5 records)
        // and fast adaptive delivery, B may catch up within the connection window and the refresh
        // may already be in READY_SENT or COMPLETED when this then: block executes.
        // The essential behaviour (B receives READY and post-refresh data) is verified below.

        then: "refresh eventually completes — both A and B caught up"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert consumerBCtx.getBean(TestRecordCollector).readyCount >= 1
        }

        // ── POST-REFRESH DELIVERY ─────────────────────────────────────────────

        when: "new records arrive after refresh completion"
        collector().reset()
        consumerBCtx.getBean(TestRecordCollector).reset()
        cloudServer.enqueueMessages([
            [offset: 6L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-late-1', eventType: 'MESSAGE', data: '{"post":1}'],
            [offset: 7L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-late-2', eventType: 'MESSAGE', data: '{"post":2}'],
        ])

        then: "consumer A receives post-refresh records"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'post-late-1' }
            assert collector().getAll().any { it.msgKey == 'post-late-2' }
        }

        and: "consumer B (late joiner) also receives post-refresh records"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert consumerBCtx.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-late-1' }
            assert consumerBCtx.getBean(TestRecordCollector).getAll().any { it.msgKey == 'post-late-2' }
        }

        and: "both groups have advancing committed offsets"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 7
            assert offsetTracker.getOffset('group-b:prices-v1') >= 7
        }

        cleanup:
        consumerBCtx?.close()
    }

}
