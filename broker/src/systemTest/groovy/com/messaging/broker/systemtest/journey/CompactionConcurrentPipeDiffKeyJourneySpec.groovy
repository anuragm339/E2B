package com.messaging.broker.systemtest.journey

import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: compaction runs on sealed segments for a key with multiple versions while
 * the pipe concurrently delivers records for brand-new keys.
 *
 * Scenario:
 * 1. Pipe delivers 4 versions of 'key-A' (v1..v4).  The delivery filter means only v4
 *    (the latest) is pushed to the consumer.
 * 2. Compaction runs — v1/v2/v3 are removed from the sealed segment for key-A.
 * 3. Pipe then delivers records for entirely new keys 'key-X' and 'key-Y'.
 * 4. key-X and key-Y must arrive in full; key-A was delivered exactly once (v4).
 */
class CompactionConcurrentPipeDiffKeyJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-index-diff"
        base['broker.storage.segment-size'] = '512'
        return base
    }

    def "compaction on sealed segment for key-A does not affect delivery of new keys X and Y"() {
        given: "collector is clean"
        collector().reset()

        when: "pipe delivers 4 versions of 'key-A' to force segment sealing"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'cpd-key-A', eventType: 'MESSAGE', data: '{"v":1}'],
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'cpd-key-A', eventType: 'MESSAGE', data: '{"v":2}'],
            [offset: 3L, topic: 'prices-v1', partition: 0,
             msgKey: 'cpd-key-A', eventType: 'MESSAGE', data: '{"v":3}'],
            [offset: 4L, topic: 'prices-v1', partition: 0,
             msgKey: 'cpd-key-A', eventType: 'MESSAGE', data: '{"v":4}'],
        ])

        then: "due to the delivery filter, only the latest version (v4) is pushed to the consumer"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            // At most one record for key-A — the broker's dedup filter keeps only the latest
            assert collector().getAll().count { it.msgKey == 'cpd-key-A' } == 1
        }
        collector().getAll().find { it.msgKey == 'cpd-key-A' }?.data == '{"v":4}'

        // Ensure old versions were never delivered
        !collector().getAll().any { it.msgKey == 'cpd-key-A' && it.data == '{"v":1}' }
        !collector().getAll().any { it.msgKey == 'cpd-key-A' && it.data == '{"v":2}' }
        !collector().getAll().any { it.msgKey == 'cpd-key-A' && it.data == '{"v":3}' }

        when: "compaction runs — removes superseded v1/v2/v3 from the sealed segment"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()

        and: "pipe delivers records for new keys X and Y"
        cloudServer.enqueueMessages([
            [offset: 5L, topic: 'prices-v1', partition: 0,
             msgKey: 'cpd-key-X', eventType: 'MESSAGE', data: '{"x":1}'],
            [offset: 6L, topic: 'prices-v1', partition: 0,
             msgKey: 'cpd-key-Y', eventType: 'MESSAGE', data: '{"y":1}'],
        ])

        then: "key-X arrives intact after compaction"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cpd-key-X' }
        }
        collector().getAll().find { it.msgKey == 'cpd-key-X' }.data == '{"x":1}'

        and: "key-Y arrives intact after compaction"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cpd-key-Y' }
        }
        collector().getAll().find { it.msgKey == 'cpd-key-Y' }.data == '{"y":1}'

        and: "key-A was delivered exactly once overall (not re-delivered after compaction)"
        collector().getAll().count { it.msgKey == 'cpd-key-A' } == 1

        and: "old key-A versions (v1/v2/v3) were never delivered at any point"
        !collector().getAll().any { it.msgKey == 'cpd-key-A' && it.data == '{"v":1}' }
        !collector().getAll().any { it.msgKey == 'cpd-key-A' && it.data == '{"v":2}' }
        !collector().getAll().any { it.msgKey == 'cpd-key-A' && it.data == '{"v":3}' }
    }
}
