package com.messaging.broker.systemtest.journey

import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: a second consumer subscribes AFTER compaction has run and should see only
 * the compacted view — latest per key — not historical records.
 *
 * Scenario:
 * 1. Pipe delivers v1/v2/v3 for 'product-A' and one record each for 'product-B' / 'product-C'.
 * 2. First consumer (built-in collector) sees: v3 for A, B, C  → 3 records total.
 * 3. Compaction runs.
 * 4. A second consumer context is created and subscribes to the broker.
 * 5. The second consumer's replay must deliver: v3 for A, B, C → 3 records total.
 * 6. The second consumer must NOT receive v1 or v2 for product-A.
 */
class LateConsumerAfterCompactionJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-index-late"
        base['broker.storage.segment-size'] = '512'
        return base
    }

    def "late consumer subscribing after compaction only sees latest-per-key, not historical records"() {
        given: "collector is clean"
        collector().reset()

        and: "pipe delivers 3 versions of product-A plus unique records for B and C"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'lca-product-A', eventType: 'MESSAGE', data: '{"version":1}'],
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'lca-product-A', eventType: 'MESSAGE', data: '{"version":2}'],
            [offset: 3L, topic: 'prices-v1', partition: 0,
             msgKey: 'lca-product-A', eventType: 'MESSAGE', data: '{"version":3}'],
            [offset: 4L, topic: 'prices-v1', partition: 0,
             msgKey: 'lca-product-B', eventType: 'MESSAGE', data: '{"b":1}'],
            [offset: 5L, topic: 'prices-v1', partition: 0,
             msgKey: 'lca-product-C', eventType: 'MESSAGE', data: '{"c":1}'],
        ])

        and: "first consumer (delivery filter active) sees A-v3, B, C"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            def all = collector().getAll()
            assert all.any { it.msgKey == 'lca-product-A' && it.data == '{"version":3}' }
            assert all.any { it.msgKey == 'lca-product-B' }
            assert all.any { it.msgKey == 'lca-product-C' }
        }

        when: "compaction runs — removes v1 and v2 for product-A from sealed segments"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()
        sleep(500)

        and: "a second consumer context is created and connects to the broker"
        def secondCtx = ApplicationContext.run(
            secondConsumerProperties(brokerTcpPort) as Map<String, Object>)
        triggerConsumerManagerStartup(secondCtx)
        sleep(2000)

        then: "second consumer receives A-v3, B, C"
        def secondCollector = secondCtx.getBean(TestRecordCollector)
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            def all = secondCollector.getAll()
            assert all.any { it.msgKey == 'lca-product-A' && it.data == '{"version":3}' }
            assert all.any { it.msgKey == 'lca-product-B' }
            assert all.any { it.msgKey == 'lca-product-C' }
        }

        and: "second consumer did NOT receive v1 or v2 for product-A"
        !secondCollector.getAll().any { it.msgKey == 'lca-product-A' && it.data == '{"version":1}' }
        !secondCollector.getAll().any { it.msgKey == 'lca-product-A' && it.data == '{"version":2}' }

        and: "only one record for product-A was delivered"
        secondCollector.getAll().count { it.msgKey == 'lca-product-A' } == 1

        cleanup:
        secondCtx?.close()
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Map<String, String> secondConsumerProperties(int port) {
        def props = new LinkedHashMap<>(consumerProperties())
        props['messaging.broker.port'] = "${port}"
        props['micronaut.server.port'] = "${findFreePort()}"
        props['consumer.group']        = 'late-consumer-group'
        return props
    }
}
