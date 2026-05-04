package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.common.api.StorageEngine
import spock.util.concurrent.PollingConditions

/**
 * Journey: RESET/READY data refresh workflow.
 *
 * 1. Consumer receives initial records.
 * 2. Refresh triggered by calling RefreshCoordinator directly.
 * 3. Consumer's onReset() is called (RESET message received).
 * 4. Consumer sends READY after replay.
 *
 * Also verifies that the trigger record was correctly written to StorageEngine
 * before the refresh starts, confirming the pipe→storage path for this test's data.
 */
class DataRefreshJourneySpec extends BrokerSystemTestSupport {

    def "data refresh RESET reaches the consumer"() {
        given: "consumer has received at least one record so it is connected"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0, msgKey: 'init-1', eventType: 'MESSAGE', data: '{"init":1}']
        ])
        collector().waitForRecords(1, 20)

        and: "that record is persisted in StorageEngine with correct content (fromOffset=1 — enqueued offset)"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def stored = storage.read('prices-v1', 0, 1L, 100)
            def r = stored.find { it.msgKey == 'init-1' }
            assert r != null
            assert r.data == '{"init":1}'
            assert r.eventType.name() == 'MESSAGE'
        }

        and: "committed offset is recorded in ConsumerOffsetTracker (ACK processed for init-1)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') > 0
        }

        and: "init-1 msgKey is written to RocksDB ack-store"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'system-test-group', 'init-1') != null
        }
        collector().reset()

        when: "refresh is triggered for prices-v1"
        brokerCtx.getBean(RefreshCoordinator).startRefresh('prices-v1')

        then: "consumer receives a RESET"
        new PollingConditions(timeout: 15, delay: 0.5).eventually {
            assert collector().resetCount >= 1
        }
    }

    def "after refresh, consumer continues to receive new records"() {
        given: "initial record delivered"
        cloudServer.enqueueMessages([
            [offset: 50L, topic: 'prices-v1', partition: 0, msgKey: 'pre-refresh', eventType: 'MESSAGE', data: '{"x":1}']
        ])
        collector().waitForRecords(1, 20)

        and: "pre-refresh record is in StorageEngine (fromOffset=50 — enqueued offset)"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def stored = storage.read('prices-v1', 0, 50L, 100)
            assert stored.any { it.msgKey == 'pre-refresh' && it.data == '{"x":1}' }
        }

        and: "committed offset is recorded in ConsumerOffsetTracker (ACK processed for pre-refresh)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') > 0
        }

        and: "pre-refresh msgKey is written to RocksDB ack-store"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'system-test-group', 'pre-refresh') != null
        }
        collector().reset()

        and: "trigger refresh"
        brokerCtx.getBean(RefreshCoordinator).startRefresh('prices-v1')

        and: "wait for reset to propagate"
        new PollingConditions(timeout: 15, delay: 0.5).eventually {
            assert collector().resetCount >= 1
        }
        collector().reset()

        when: "new records arrive after refresh"
        cloudServer.enqueueMessages([
            [offset: 51L, topic: 'prices-v1', partition: 0, msgKey: 'post-refresh', eventType: 'MESSAGE', data: '{"x":2}']
        ])

        then: "post-refresh record is eventually delivered (replay records may also arrive — search by key)"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'post-refresh' }
        }

        and: "post-refresh record data payload intact"
        collector().getAll().find { it.msgKey == 'post-refresh' }?.data == '{"x":2}'

        and: "post-refresh record written to StorageEngine (fromOffset=51 — enqueued offset)"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def stored = storage.read('prices-v1', 0, 51L, 100)
            assert stored.any { it.msgKey == 'post-refresh' && it.data == '{"x":2}' }
        }

        and: "post-refresh msgKey is written to RocksDB ack-store after ACK"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'system-test-group', 'post-refresh') != null
        }
    }
}
