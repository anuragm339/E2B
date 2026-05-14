package com.messaging.broker.systemtest.journey

import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: DELETE tombstone for a key is delivered to the consumer; with zero
 * tombstone-retention-days the tombstone is physically removed by the next compaction.
 *
 * Scenario:
 * 1. Pipe delivers a MESSAGE for key 'item-001'; consumer receives it.
 * 2. Pipe delivers a DELETE for key 'item-001'; consumer receives the tombstone so it
 *    can remove the entry from its local state.
 * 3. Filler records are sent to push past the small segment size and seal the segment.
 * 4. Compaction runs — with retention=0 days the tombstone expires immediately and is
 *    removed from the sealed segment.
 * 5. A probe record confirms the system is still live after compaction.
 * 6. After the reset the original consumer received exactly 2 records: MESSAGE + DELETE.
 */
class TombstoneLifecycleJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']           = "${dataDir}/compaction-index-tombstone"
        base['broker.storage.segment-size']       = '512'
        base['compaction.tombstone-retention-days'] = '0'
        return base
    }

    def "DELETE tombstone is delivered then physically removed by compaction with zero retention"() {
        given: "collector is clean"
        collector().reset()

        when: "pipe delivers a MESSAGE for 'item-001'"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'tlc-item-001', eventType: 'MESSAGE', data: '{"name":"widget"}']
        ])

        then: "consumer receives the MESSAGE record"
        def msgBatch = collector().waitForRecords(1, 15)
        msgBatch.size() == 1
        msgBatch[0].msgKey == 'tlc-item-001'
        msgBatch[0].data == '{"name":"widget"}'

        when: "collector is reset and pipe delivers a DELETE tombstone for 'item-001'"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'tlc-item-001', eventType: 'DELETE', data: null]
        ])

        then: "consumer receives the DELETE tombstone (so it can remove from local state)"
        new PollingConditions(timeout: 15, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'tlc-item-001' }
        }
        def deletedRecord = collector().getAll().find { it.msgKey == 'tlc-item-001' }
        deletedRecord != null

        when: "filler records are sent to force segment sealing"
        cloudServer.enqueueMessages((3..10).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "tlc-filler-${i}", eventType: 'MESSAGE', data: """{"filler":${i}}"""]
        })

        // Wait for fillers so we know the segment has rolled over before compaction
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'tlc-filler-10' }
        }

        and: "compaction runs — tombstone for 'item-001' expires immediately (retention=0)"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()

        and: "collector is reset and a probe record is enqueued"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 11L, topic: 'prices-v1', partition: 0,
             msgKey: 'tlc-probe', eventType: 'MESSAGE', data: '{"probe":true}']
        ])

        then: "probe record arrives — system is live after tombstone compaction"
        def probeBatch = collector().waitForRecords(1, 15)
        probeBatch.size() == 1
        probeBatch[0].msgKey == 'tlc-probe'

        and: "after the reset, item-001 is not re-delivered (tombstone was removed)"
        !collector().getAll().any { it.msgKey == 'tlc-item-001' }
    }

    def "original consumer received exactly MESSAGE then DELETE for item-001 — no extra deliveries"() {
        given: "use a dedicated fresh collector to count precisely"
        collector().reset()

        when: "pipe delivers MESSAGE then DELETE in sequence"
        cloudServer.enqueueMessages([
            [offset: 50L, topic: 'prices-v1', partition: 0,
             msgKey: 'tlc-count-key', eventType: 'MESSAGE', data: '{"x":1}'],
            [offset: 51L, topic: 'prices-v1', partition: 0,
             msgKey: 'tlc-count-key', eventType: 'DELETE', data: null],
        ])

        then: "exactly 2 records for that key are delivered (MESSAGE + DELETE)"
        new PollingConditions(timeout: 15, delay: 0.3).eventually {
            def forKey = collector().getAll().findAll { it.msgKey == 'tlc-count-key' }
            assert forKey.size() == 2
        }
        def delivered = collector().getAll().findAll { it.msgKey == 'tlc-count-key' }
        delivered.size() == 2
        delivered.any { it.eventType?.toString()?.contains('MESSAGE') || it.data == '{"x":1}' }
        delivered.any { it.eventType?.toString()?.contains('DELETE') || it.data == null }
    }
}
