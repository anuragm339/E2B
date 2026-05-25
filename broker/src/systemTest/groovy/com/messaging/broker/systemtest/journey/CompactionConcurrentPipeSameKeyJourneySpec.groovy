package com.messaging.broker.systemtest.journey

import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.common.api.StorageEngine
import spock.util.concurrent.PollingConditions

/**
 * Journey: pipe sends v1 then v2 for the same key; compaction runs; consumer should
 * only ever see v2 (the latest) and v1 must be physically removed from the log.
 *
 * Scenario:
 * 1. Pipe delivers v1 for key 'price-001'; consumer receives it.
 * 2. Pipe delivers v2 for the same key; consumer receives it.
 * 3. Compaction runs — v1 is superseded and removed from sealed segments.
 * 4. A probe record (unique key) is enqueued to confirm pipe + consumer are still live.
 * 5. After the reset, v1 is never re-delivered; the compaction index marks offset 0
 *    (v1's stored offset) as superseded.
 */
class CompactionConcurrentPipeSameKeyJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']      = "${dataDir}/compaction-index"
        base['broker.storage.segment-size']  = '512'
        return base
    }

    def "consumer receives only latest record when pipe sends two versions of same key and compaction runs"() {
        given: "collector is clean"
        collector().reset()

        when: "pipe delivers v1 for key 'price-001'"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'price-001', eventType: 'MESSAGE', data: '{"price":100}']
        ])

        then: "consumer receives v1"
        def v1Batch = collector().waitForRecords(1, 15)
        v1Batch.size() == 1
        v1Batch[0].data == '{"price":100}'
        v1Batch[0].msgKey == 'price-001'

        when: "pipe delivers v2 for the same key"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'price-001', eventType: 'MESSAGE', data: '{"price":200}']
        ])

        then: "consumer receives v2"
        def v2Batch = collector().waitForRecords(1, 15)
        v2Batch.size() == 1
        v2Batch[0].data == '{"price":200}'
        v2Batch[0].msgKey == 'price-001'

        when: "compaction runs"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()

        and: "collector is reset and a probe record with a unique key is enqueued"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 3L, topic: 'prices-v1', partition: 0,
             msgKey: 'probe-key-cps', eventType: 'MESSAGE', data: '{"probe":true}']
        ])

        then: "probe record arrives — pipe and consumer are still live after compaction"
        def probeBatch = collector().waitForRecords(1, 15)
        probeBatch.size() == 1
        probeBatch[0].msgKey == 'probe-key-cps'
        probeBatch[0].data == '{"probe":true}'

        and: "v1 (data='{\"price\":100}') is not re-delivered after the collector reset"
        !collector().getAll().any { it.data == '{"price":100}' }

        and: "the compaction index marks the earlier record for 'price-001' as superseded"
        def storage = brokerCtx.getBean(StorageEngine)
        def compactionIndex = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.RocksDbCompactionIndex'))
        // The first stored offset for prices-v1 / price-001 must be superseded
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert compactionIndex.isSuperseded('prices-v1', 'price-001', 0L)
        }
    }
}
