package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: refresh replay reads from BOTH a compacted segment (.compacted.log) AND
 * the active segment (.log) in the same topic-partition.
 *
 * Scenario:
 * 1. Write records with duplicate msgKeys → segment seals → compaction runs
 *    → produces 00000000000000000000.compacted.log (only latest per key retained)
 * 2. Write NEW records to the active segment (plain .log, created after compaction)
 * 3. Trigger data refresh (RESET/READY)
 * 4. During replay the broker must traverse:
 *       compacted segment → active segment (cross-file hop)
 * 5. Assert the consumer receives records from BOTH files:
 *    - latest version of each compacted key (superseded versions are gone)
 *    - new records from the active segment
 * 6. Assert the full refresh lifecycle completes (READY received)
 */
class RefreshWithCompactedAndActiveSegmentsJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-index-refresh-active"
        // Tiny segment so filler records trigger a seal without needing hundreds of messages
        base['broker.storage.segment-size'] = '512'
        base['data-refresh.enabled']        = 'true'
        return base
    }

    def "refresh replays records from both compacted segment and active segment"() {
        given: "collector is clean"
        collector().reset()

        // ── Phase 1: write records with duplicate keys so compaction has something to compact ──
        and: "pipe delivers 3 versions of product-X and unique records for product-Y / product-Z"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-product-X', eventType: 'MESSAGE', data: '{"version":1}'],
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-product-X', eventType: 'MESSAGE', data: '{"version":2}'],
            [offset: 3L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-product-X', eventType: 'MESSAGE', data: '{"version":3}'],
            [offset: 4L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-product-Y', eventType: 'MESSAGE', data: '{"y":1}'],
            [offset: 5L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-product-Z', eventType: 'MESSAGE', data: '{"z":1}'],
        ])

        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            def all = collector().getAll()
            assert all.any { it.msgKey == 'rca-product-X' }
            assert all.any { it.msgKey == 'rca-product-Y' }
            assert all.any { it.msgKey == 'rca-product-Z' }
        }

        and: "filler records force the segment to seal (segment-size=512 means ~5 records fill it)"
        cloudServer.enqueueMessages((6..12).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "rca-filler-${i}", eventType: 'MESSAGE', data: """{"filler":${i}}"""]
        })
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'rca-filler-12' }
        }

        // ── Phase 2: compact → .compacted.log created, v1/v2 of product-X removed ──
        when: "compaction runs — creates .compacted.log with only latest per key"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        // Force-seal the active segment so compact() finds a candidate.
        // All records (~29–44 B each) fit in the 512 B active segment without a natural rollover.
        def segmentAccess = brokerCtx.getBean(
            Class.forName('com.messaging.storage.segment.SegmentAccess'))
        segmentAccess.getSegmentManager('prices-v1', 0)?.forceRollActiveSegment()
        scheduler.compact()
        sleep(500)  // allow compaction to complete and SegmentManager to install new segments

        // ── Phase 3: write NEW records to the active segment (after compaction) ──
        and: "new records arrive on the active segment (plain .log, not .compacted.log)"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 13L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-new-A', eventType: 'MESSAGE', data: '{"new":"A"}'],
            [offset: 14L, topic: 'prices-v1', partition: 0,
             msgKey: 'rca-new-B', eventType: 'MESSAGE', data: '{"new":"B"}'],
        ])
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'rca-new-A' }
            assert collector().getAll().any { it.msgKey == 'rca-new-B' }
        }

        // ── Phase 4: trigger refresh — replay must hop from .compacted.log → active .log ──
        and: "collector is reset and refresh is triggered"
        collector().reset()
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        then: "consumer receives RESET"
        new PollingConditions(timeout: 20, delay: 0.5).eventually {
            assert collector().resetCount >= 1
        }

        // ── Phase 5: assert replay delivers records from BOTH files ──
        and: "consumer receives READY — full replay across compacted + active segment completed"
        new PollingConditions(timeout: 40, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }

        and: "replay includes latest version of compacted key product-X (from .compacted.log)"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'rca-product-X' && it.data == '{"version":3}' }
        }

        and: "replay includes product-Y and product-Z (compacted segment)"
        collector().getAll().any { it.msgKey == 'rca-product-Y' }
        collector().getAll().any { it.msgKey == 'rca-product-Z' }

        and: "replay includes new-A and new-B from the active .log segment"
        collector().getAll().any { it.msgKey == 'rca-new-A' }
        collector().getAll().any { it.msgKey == 'rca-new-B' }

        // ── Phase 6: verify compaction was effective — superseded versions absent ──
        and: "superseded v1 and v2 of product-X are NOT replayed"
        !collector().getAll().any { it.msgKey == 'rca-product-X' && it.data == '{"version":1}' }
        !collector().getAll().any { it.msgKey == 'rca-product-X' && it.data == '{"version":2}' }

        and: "only one record for product-X is delivered (exactly the latest)"
        collector().getAll().count { it.msgKey == 'rca-product-X' } == 1
    }
}
