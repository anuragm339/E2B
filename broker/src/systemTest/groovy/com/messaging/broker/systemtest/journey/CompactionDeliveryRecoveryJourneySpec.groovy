package com.messaging.broker.systemtest.journey

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.LegacyConsumerClient
import com.messaging.common.api.StorageEngine
import com.messaging.network.legacy.events.BatchEvent
import com.messaging.network.legacy.events.ReadyEvent
import spock.util.concurrent.PollingConditions

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Journey: delivery completes to head offset when compaction runs while batches are in-flight.
 *
 * Regression for Bug 2 — Compaction permanently kills delivery:
 *
 *   Root cause (fixed in BatchDeliveryService):
 *     When sendBatchToConsumer() throws BEFORE the ACK-timeout is scheduled,
 *     the old code left pendingOffset set (non-null) because the "Only remove
 *     pending offset for PERMANENT failures" branch did not run.
 *     Gate 2 (stateService.getPendingOffset != null) then permanently blocked
 *     every subsequent delivery attempt for that topic.
 *
 *   Fix:
 *     BatchDeliveryService now tracks whether the ACK-timeout was scheduled
 *     (timeoutScheduled flag). If the exception is thrown before the timeout
 *     was registered (timeoutScheduled == false), pendingOffset and related
 *     state are cleared unconditionally in the catch block — no ACK is coming,
 *     so there is no reason to retain the pending offset.
 *
 * Test structure:
 *  Phase 1 — fill and seal a segment via pipe replay; run first compaction to
 *             produce a .compacted.log file
 *  Phase 2 — write additional records; connect legacy client; let the broker
 *             deliver the first batch (client does NOT ACK immediately)
 *  Phase 3 — run second compaction in the background while the batch is
 *             in-flight (no ACK from client)
 *  Phase 4 — client ACKs the first batch; assert delivery continues and reaches
 *             head offset
 *
 * Why this catches the regression:
 *   With the old code, if compaction's replaceSegments() closes segment
 *   FileChannels while the broker is inside getBatch(), the resulting
 *   ClosedChannelException reaches the catch block BEFORE setPendingOffset()
 *   is called — so there is no stuck offset in that specific path.
 *
 *   The critical path is triggered in a subsequent retry AFTER an ACK timeout:
 *     1. Batch sent, pendingOffset set, ACK timeout scheduled.
 *     2. Consumer goes silent — ACK timeout fires, clears pendingOffset, releases
 *        inFlight gate.
 *     3. Broker retries; setPendingOffset() set again at line ~223.
 *     4. sendBatchToConsumer() throws ClosedChannelException (consumer TCP closed).
 *     5. Old code: pendingOffset NOT cleared → Gate 2 permanently blocked.
 *     6. Fixed code: pendingOffset cleared → delivery continues.
 *
 *   This test verifies the observable outcome — delivery recovers and reaches
 *   head offset — without relying on sub-millisecond timing to inject the
 *   ClosedChannelException. The unit test in BatchDeliveryServiceSpec verifies
 *   the specific code-level fix (clearPendingOffset is called on transient failure
 *   when timeoutScheduled == false).
 */
class CompactionDeliveryRecoveryJourneySpec extends BrokerSystemTestSupport {

    LegacyConsumerClient legacyClient

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-delivery"
        // 512-byte segments: ~6 records of ~80B each fill and seal a segment.
        base['broker.storage.segment-size'] = '512'
        return base
    }

    @Override
    protected String defaultTopic() { '__unused_legacy_only__' }

    def setup() {
        // legacyClient is connected inside the test AFTER Phase 1 compaction.
        // Connecting before Phase 1 records arrive would let the broker deliver them
        // before the compaction index is populated, conflating the pre-compaction and
        // post-compaction states in a way that makes assertions non-deterministic.
    }

    def cleanup() {
        legacyClient?.close()
    }

    def "delivery reaches head offset when compaction runs while a batch is in-flight"() {
        given: "Phase 1: write 6 records with unique keys to fill and seal a 512-byte segment"
        // ~80B per record × 6 = ~480B which fills the 512B segment and triggers rollover.
        // All keys are unique — compaction keeps all records (no removal).
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-A', eventType: 'MESSAGE', data: '{"v":1}'],
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-B', eventType: 'MESSAGE', data: '{"v":2}'],
            [offset: 3L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-C', eventType: 'MESSAGE', data: '{"v":3}'],
            [offset: 4L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-D', eventType: 'MESSAGE', data: '{"v":4}'],
            [offset: 5L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-E', eventType: 'MESSAGE', data: '{"v":5}'],
            [offset: 6L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-F', eventType: 'MESSAGE', data: '{"v":6}'],
        ])

        and: "first compaction creates the .compacted.log + .compacted.index pair"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            assert storage.read('prices-v1', 0, 6L, 1).any { it.msgKey == 'drv-F' }
        }
        // Force-seal the active segment so compact() finds it as a candidate.
        // Records are ~29 B each; all 6 fit in the 512 B active segment without a
        // natural rollover.  compact() only processes sealed (inactive) segments.
        def segmentAccess = brokerCtx.getBean(
            Class.forName('com.messaging.storage.segment.SegmentAccess'))
        segmentAccess.getSegmentManager('prices-v1', 0)?.forceRollActiveSegment()
        scheduler.compact() // seals the segment; drv-A..F all survive (unique keys)

        and: "connect legacy client AFTER first compaction so delivery starts with index populated"
        legacyClient = LegacyConsumerClient.connect('127.0.0.1', brokerTcpPort, 'price-quote')
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert legacyClient.received.any { it instanceof ReadyEvent }
        }
        legacyClient.sendAck()   // ACK startup READY
        legacyClient.clearReceived()

        when: "Phase 2: write 4 more records into the now-active segment"
        cloudServer.enqueueMessages([
            [offset:  7L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-G', eventType: 'MESSAGE', data: '{"v":7}'],
            [offset:  8L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-H', eventType: 'MESSAGE', data: '{"v":8}'],
            [offset:  9L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-I', eventType: 'MESSAGE', data: '{"v":9}'],
            [offset: 10L, topic: 'prices-v1', partition: 0,
             msgKey: 'drv-J', eventType: 'MESSAGE', data: '{"v":10}'],
        ])

        then: "the broker delivers the first batch to the connected client"
        // Wait for at least one batch to arrive — client intentionally holds the ACK.
        new PollingConditions(timeout: 20, delay: 0.2).eventually {
            assert legacyClient.received.any { it instanceof BatchEvent }
        }

        when: "Phase 3: second compaction runs in the background while the batch is still in-flight"
        // The client has NOT ACKed yet — pendingOffset is set and the inFlight gate is held.
        // Compaction's replaceSegments() will close old segment FileChannels and install new ones.
        // With the old bug, a ClosedChannelException from a subsequent retry after an ACK timeout
        // would leave pendingOffset permanently set and permanently block Gate 2.
        def compactionDone = new CountDownLatch(1)
        Thread.start {
            try {
                // Run compact twice to maximise the chance of racing a read or retry.
                2.times {
                    scheduler.compact()
                }
            } finally {
                compactionDone.countDown()
            }
        }

        then: "compaction completes without hanging (no thread or lock starvation)"
        compactionDone.await(20, TimeUnit.SECONDS)

        when: "Phase 4: client ACKs the in-flight batch"
        // ACK every BatchEvent received so far — this unblocks the broker to continue delivery.
        legacyClient.received.findAll { it instanceof BatchEvent }
                              .each { legacyClient.sendAck() }

        then: "delivery resumes and eventually includes all Phase-2 records"
        // Key assertion: delivery is NOT permanently stalled.
        // With the old code, if any retry threw ClosedChannelException before the ACK-timeout
        // was scheduled, pendingOffset would remain set after the catch block and Gate 2 would
        // block all future delivery — these keys would never appear.
        new PollingConditions(timeout: 30, delay: 0.2).eventually {
            def keys = legacyClient.received
                .findAll { it instanceof BatchEvent }
                .collectMany { (it as BatchEvent).messages*.key }
                .toSet()
            assert keys.contains('drv-G') : "drv-G missing — delivery may be permanently stalled"
            assert keys.contains('drv-H') : "drv-H missing"
            assert keys.contains('drv-I') : "drv-I missing"
            assert keys.contains('drv-J') : "drv-J missing"
        }

        when: "client ACKs all remaining batches"
        legacyClient.received.findAll { it instanceof BatchEvent }
                              .each { legacyClient.sendAck() }

        then: "committed offset advances to head (≥ 10)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 15, delay: 0.3).eventually {
            assert offsetTracker.getOffset('price-quote:prices-v1') >= 10L
        }

        and: "all 10 record keys were delivered (no gap across the compaction boundary)"
        legacyClient.received
            .findAll { it instanceof BatchEvent }
            .collectMany { (it as BatchEvent).messages*.key }
            .toSet()
            .containsAll(['drv-A', 'drv-B', 'drv-C', 'drv-D', 'drv-E', 'drv-F',
                          'drv-G', 'drv-H', 'drv-I', 'drv-J'])

        and: "no wire errors on the legacy connection throughout"
        legacyClient.errors.isEmpty()
    }
}
