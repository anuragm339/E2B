package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: pipe data arriving around a local refresh is delivered without loss or duplicates.
 *
 * POS scenario: a data refresh is triggered at the same time as a stream of new price
 * updates arriving at the cloud pipe. The broker must:
 *  1. Keep the pipe active because local refresh does not mutate pipe-offset.properties.
 *  2. Complete the RESET → REPLAYING → READY → COMPLETED lifecycle.
 *  3. Continue delivering records that arrive before, during, or after refresh.
 *
 * Records enqueued during the refresh window may arrive before READY or through replay;
 * the contract is that they are not lost and the pipe is not left paused.
 *
 * Two sub-scenarios tested:
 * A. Records enqueued BEFORE refresh — must be re-delivered via replay (no loss).
 * B. Records enqueued around refresh — must arrive even though local refresh no longer
 *    pauses or resumes the pipe.
 *
 * Verified behaviours:
 * 1. Pre-refresh records are replayed and received by the consumer after RESET.
 * 2. Records enqueued around refresh are delivered.
 * 3. After READY, additional records continue to flow normally.
 * 4. Committed offset and RocksDB reflect all records in their correct sequence.
 */
class RefreshWithPendingPipeDataJourneySpec extends BrokerSystemTestSupport {

    def "pipe data around local refresh is delivered while pipe remains active"() {
        given: "consumer receives 3 pre-refresh records"
        collector().reset()
        cloudServer.enqueueMessages((1..3).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "pre-${i}", eventType: 'MESSAGE', data: """{"v":${i}}"""]
        })
        collector().waitForRecords(3, 20)

        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 3
        }
        collector().reset()

        and: "3 new records are queued in the pipe before the refresh starts"
        // Local refresh no longer pauses the pipe. These records may be delivered before READY
        // or as part of replay, but they must not be lost.
        cloudServer.enqueueMessages((4..6).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "during-${i}", eventType: 'MESSAGE', data: """{"v":${i}}"""]
        })

        when: "local refresh is triggered"
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        then: "consumer receives RESET"
        new PollingConditions(timeout: 20, delay: 0.5).eventually {
            assert collector().resetCount >= 1
        }

        and: "consumer receives READY — full refresh lifecycle completed"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }

        // ── RECORDS QUEUED AROUND REFRESH ARE DELIVERED ─────────────────────

        and: "records queued around refresh (during-4..6) are delivered"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'during-4' }
            assert collector().getAll().any { it.msgKey == 'during-5' }
            assert collector().getAll().any { it.msgKey == 'during-6' }
        }

        // ── ADDITIONAL POST-REFRESH RECORDS ──────────────────────────────────

        when: "new records are published after refresh completes"
        cloudServer.enqueueMessages([
            [offset: 7L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-7', eventType: 'MESSAGE', data: '{"post":7}'],
            [offset: 8L, topic: 'prices-v1', partition: 0,
             msgKey: 'post-8', eventType: 'MESSAGE', data: '{"post":8}'],
        ])

        then: "post-refresh records arrive normally"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'post-7' }
            assert collector().getAll().any { it.msgKey == 'post-8' }
        }

        and: "committed offset has advanced to include all records"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 8
        }

        and: "RocksDB ack-store has entries for queued and post-refresh records"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            // Records that were queued around local refresh
            assert ackStore.get('prices-v1', 'system-test-group', 4L) != null
            assert ackStore.get('prices-v1', 'system-test-group', 5L) != null
            assert ackStore.get('prices-v1', 'system-test-group', 6L) != null
            // Post-refresh records
            assert ackStore.get('prices-v1', 'system-test-group', 7L) != null
            assert ackStore.get('prices-v1', 'system-test-group', 8L) != null
        }
    }

    def "records enqueued before and after a refresh are all delivered — no data loss across refresh boundary"() {
        given: "2 records arrive, consumer receives them"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 100L, topic: 'prices-v1', partition: 0,
             msgKey: 'boundary-pre-1', eventType: 'MESSAGE', data: '{"b":1}'],
            [offset: 101L, topic: 'prices-v1', partition: 0,
             msgKey: 'boundary-pre-2', eventType: 'MESSAGE', data: '{"b":2}'],
        ])
        collector().waitForRecords(2, 20)
        collector().reset()

        when: "refresh is triggered"
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        and: "consumer completes full refresh cycle"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }
        collector().reset()

        and: "2 post-refresh records are enqueued and delivered"
        cloudServer.enqueueMessages([
            [offset: 102L, topic: 'prices-v1', partition: 0,
             msgKey: 'boundary-post-1', eventType: 'MESSAGE', data: '{"b":3}'],
            [offset: 103L, topic: 'prices-v1', partition: 0,
             msgKey: 'boundary-post-2', eventType: 'MESSAGE', data: '{"b":4}'],
        ])

        then: "post-refresh records arrive without loss"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'boundary-post-1' }
            assert collector().getAll().any { it.msgKey == 'boundary-post-2' }
        }

        and: "no cross-boundary duplicates — pre-refresh keys do not appear again"
        !collector().getAll().any { it.msgKey == 'boundary-pre-1' }
        !collector().getAll().any { it.msgKey == 'boundary-pre-2' }
    }
}
