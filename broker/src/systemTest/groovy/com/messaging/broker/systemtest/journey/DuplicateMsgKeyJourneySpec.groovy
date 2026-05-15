package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.AckReconciliationScheduler
import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: duplicate business keys at different offsets are tracked and ACKed independently.
 *
 * The system design allows the same msgKey to reappear as business data changes. Offset is the
 * unique event identity. With log compaction active, only the latest record per key is delivered
 * to consumers (the older version is suppressed). However:
 *  1. the consumer offset advances past ALL offsets in the batch (including the suppressed one)
 *  2. the ACK store records a separate entry for EACH offset in the batch
 *  3. reconciliation does not collapse them into one logical ACK
 */
class DuplicateMsgKeyJourneySpec extends BrokerSystemTestSupport {

    def "same msgKey at two different offsets is ACKed independently by offset"() {
        given:
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 100L, topic: 'prices-v1', partition: 0,
             msgKey: 'mango', eventType: 'MESSAGE', data: '{"version":1}'],
            [offset: 101L, topic: 'prices-v1', partition: 0,
             msgKey: 'mango', eventType: 'MESSAGE', data: '{"version":2}'],
        ])

        when: "consumer receives the batch; with compaction only the latest version is delivered"
        def received = collector().waitForRecords(1, 20)

        then: "only the latest record is delivered (compaction suppresses the earlier version)"
        received.size() == 1
        received[0].msgKey == 'mango'
        received[0].data == '{"version":2}'

        and: "committed offset advances past BOTH offsets — offset, not msgKey, is the unique event id"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 102L
        }

        and: "RocksDB stores separate ACK entries for both offsets (by offset identity, not key)"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'system-test-group', 100L) != null
            assert ackStore.get('prices-v1', 'system-test-group', 101L) != null
        }

        and: "manual reconciliation runs cleanly with the duplicate msgKey history"
        brokerCtx.getBean(AckReconciliationScheduler).reconcile()
        noExceptionThrown()
    }
}
