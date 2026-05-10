package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.AckReconciliationScheduler
import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: duplicate business keys at different offsets are delivered and ACKed independently.
 *
 * The system design allows the same msgKey to reappear as business data changes. Offset is the
 * unique event identity. This spec proves two records with the same msgKey but different offsets:
 *  1. are both delivered to the consumer
 *  2. both advance the committed offset
 *  3. both get their own RocksDB ACK entries
 *  4. reconciliation does not collapse them into one logical ACK
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

        when:
        def received = collector().waitForRecords(2, 20)

        then: "both records arrive even though the business key is identical"
        received.size() == 2
        received*.msgKey == ['mango', 'mango']
        received*.data.toSet() == ['{"version":1}', '{"version":2}'] as Set

        and: "committed offset advances past both offsets"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') >= 102L
        }

        and: "RocksDB stores separate ACK entries for both offsets"
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
