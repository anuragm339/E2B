package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.common.api.StorageEngine
import spock.util.concurrent.PollingConditions

/**
 * Journey: pipe records with sparse offsets are written and delivered correctly
 * across segment boundaries.
 *
 * Uses a tiny segment size (256 bytes) so that rollovers happen after every 2-3
 * records.  Offsets are non-sequential (pipe-style sparse: 1000, 100000, 3000000,
 * …) to verify that:
 *
 *  1. The pre-check in SegmentManager uses hasSpaceFor(record) so the roll happens
 *     before the write attempt, not via exception catch.
 *  2. The new segment's baseOffset equals the failing record's offset (not a
 *     speculative nextOffset+1), keeping every record findable by floorKey lookup.
 *  3. nextOffset is advanced only after a successful write, preventing the
 *     "offset < baseOffset" corruption on retry.
 *  4. All records survive the full pipe → storage → TCP → consumer delivery path.
 */
class SegmentRolloverWithPipeDataJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        // 256 bytes fits ~2 records per segment (each record is ~90-120 bytes on disk),
        // so 6 records will cross multiple segment boundaries.
        base['broker.storage.segment-size'] = '256'
        return base
    }

    def "pipe records with sparse offsets are delivered correctly across segment boundaries"() {
        given: "collector is clean"
        collector().reset()

        when: "6 records arrive with large sparse offsets spanning multiple segment rollovers"
        cloudServer.enqueueMessages([
            [offset: 1000L,    topic: 'prices-v1', partition: 0,
             msgKey: 'roll-1000',    eventType: 'MESSAGE', data: '{"i":1000}'],
            [offset: 100000L,  topic: 'prices-v1', partition: 0,
             msgKey: 'roll-100000',  eventType: 'MESSAGE', data: '{"i":100000}'],
            [offset: 3000000L, topic: 'prices-v1', partition: 0,
             msgKey: 'roll-3m',      eventType: 'MESSAGE', data: '{"i":3000000}'],
            [offset: 3000001L, topic: 'prices-v1', partition: 0,
             msgKey: 'roll-3m1',     eventType: 'MESSAGE', data: '{"i":3000001}'],
            [offset: 5000000L, topic: 'prices-v1', partition: 0,
             msgKey: 'roll-5m',      eventType: 'MESSAGE', data: '{"i":5000000}'],
            [offset: 5000001L, topic: 'prices-v1', partition: 0,
             msgKey: 'roll-5m1',     eventType: 'MESSAGE', data: '{"i":5000001}'],
        ])

        then: "all 6 records are delivered to the consumer"
        def received = collector().waitForRecords(6, 30)
        received.size() == 6
        received*.msgKey.toSet() == [
            'roll-1000', 'roll-100000', 'roll-3m', 'roll-3m1', 'roll-5m', 'roll-5m1'
        ].toSet()

        and: "data payloads are preserved through the full pipe → storage → consumer path"
        def byKey = received.collectEntries { [it.msgKey, it] }
        byKey['roll-1000'].data    == '{"i":1000}'
        byKey['roll-100000'].data  == '{"i":100000}'
        byKey['roll-3m'].data      == '{"i":3000000}'
        byKey['roll-3m1'].data     == '{"i":3000001}'
        byKey['roll-5m'].data      == '{"i":5000000}'
        byKey['roll-5m1'].data     == '{"i":5000001}'

        and: "all 6 records are readable from StorageEngine at their exact sparse offsets"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert storage.read('prices-v1', 0, 1000L,    1).any { it.msgKey == 'roll-1000'   }
            assert storage.read('prices-v1', 0, 100000L,  1).any { it.msgKey == 'roll-100000' }
            assert storage.read('prices-v1', 0, 3000000L, 1).any { it.msgKey == 'roll-3m'     }
            assert storage.read('prices-v1', 0, 3000001L, 1).any { it.msgKey == 'roll-3m1'    }
            assert storage.read('prices-v1', 0, 5000000L, 1).any { it.msgKey == 'roll-5m'     }
            assert storage.read('prices-v1', 0, 5000001L, 1).any { it.msgKey == 'roll-5m1'    }
        }

        and: "consumer committed offset advances to last offset + 1 (= 5000002)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') == 5000002L
        }
    }
}
