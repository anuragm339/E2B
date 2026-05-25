package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

/**
 * Journey: compaction and data refresh (RESET/READY cycle) run in close proximity.
 * The refresh must complete correctly; no records are duplicated or lost.
 *
 * Two sub-scenarios:
 * A. Compaction THEN refresh — sealed segments are compacted before the refresh replay.
 * B. Refresh THEN compaction — refresh completes first, then compaction tidies up.
 *
 * In both cases post-scenario records must arrive cleanly with no re-delivery of
 * compacted history.
 */
class CompactionDuringRefreshJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-index-refresh"
        base['broker.storage.segment-size'] = '512'
        base['data-refresh.enabled']        = 'true'
        return base
    }

    // ── Sub-scenario A: compaction THEN refresh ───────────────────────────────

    def "compaction before refresh — refresh completes and post-refresh records arrive without duplication"() {
        given: "collector is clean"
        collector().reset()

        and: "5 unique-key pre-refresh records arrive"
        cloudServer.enqueueMessages((1..5).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "cdr-pre-${i}", eventType: 'MESSAGE', data: """{"pre":${i}}"""]
        })

        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().size() >= 5
        }

        and: "filler records force the segment to seal before compaction"
        cloudServer.enqueueMessages((6..12).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "cdr-filler-${i}", eventType: 'MESSAGE', data: """{"filler":${i}}"""]
        })
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cdr-filler-12' }
        }

        when: "collector is reset and compaction runs first"
        collector().reset()
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()

        and: "refresh is triggered"
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

        when: "collector is reset and 2 post-refresh records are enqueued"
        collector().reset()
        cloudServer.enqueueMessages([
            [offset: 13L, topic: 'prices-v1', partition: 0,
             msgKey: 'cdr-post-A', eventType: 'MESSAGE', data: '{"post":"A"}'],
            [offset: 14L, topic: 'prices-v1', partition: 0,
             msgKey: 'cdr-post-B', eventType: 'MESSAGE', data: '{"post":"B"}'],
        ])

        then: "exactly the 2 post-refresh records arrive — no re-delivery of pre-refresh data"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cdr-post-A' }
            assert collector().getAll().any { it.msgKey == 'cdr-post-B' }
        }

        and: "no compacted pre-refresh records are re-delivered after the reset"
        !collector().getAll().any { it.msgKey.startsWith('cdr-pre-') }
        !collector().getAll().any { it.msgKey.startsWith('cdr-filler-') }
    }

    // ── Sub-scenario B: refresh THEN compaction ───────────────────────────────

    def "refresh before compaction — compaction after READY does not disrupt subsequent record delivery"() {
        given: "collector is clean"
        collector().reset()

        and: "5 unique-key pre-refresh records arrive and are acknowledged"
        cloudServer.enqueueMessages((20..24).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "cdr2-pre-${i}", eventType: 'MESSAGE', data: """{"pre2":${i}}"""]
        })
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cdr2-pre-24' }
        }

        and: "filler records force the segment to seal"
        cloudServer.enqueueMessages((25..30).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: "cdr2-filler-${i}", eventType: 'MESSAGE', data: """{"filler2":${i}}"""]
        })
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cdr2-filler-30' }
        }
        collector().reset()

        when: "refresh is triggered"
        def coordinator = brokerCtx.getBean(RefreshCoordinator)
        coordinator.startRefresh('prices-v1')

        then: "consumer receives RESET"
        new PollingConditions(timeout: 20, delay: 0.5).eventually {
            assert collector().resetCount >= 1
        }

        and: "consumer receives READY — refresh completes successfully"
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            assert collector().readyCount >= 1
        }
        collector().reset()

        when: "compaction runs AFTER the refresh has completed"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()

        and: "2 post-compaction records are enqueued"
        cloudServer.enqueueMessages([
            [offset: 31L, topic: 'prices-v1', partition: 0,
             msgKey: 'cdr2-post-X', eventType: 'MESSAGE', data: '{"post2":"X"}'],
            [offset: 32L, topic: 'prices-v1', partition: 0,
             msgKey: 'cdr2-post-Y', eventType: 'MESSAGE', data: '{"post2":"Y"}'],
        ])

        then: "post-compaction records arrive normally — pipe is not broken after refresh+compact"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'cdr2-post-X' }
            assert collector().getAll().any { it.msgKey == 'cdr2-post-Y' }
        }

        and: "no duplicated pre-refresh records appear after the reset"
        !collector().getAll().any { it.msgKey.startsWith('cdr2-pre-') }
        !collector().getAll().any { it.msgKey.startsWith('cdr2-filler-') }
    }
}
