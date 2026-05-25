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
 * Journey: legacy consumer delivery is NOT stalled when compaction atomically rewrites
 * an existing .compacted.* segment pair.
 *
 * The race-window bug (fixed in CompactionRewriter):
 *
 *   Old code (lines now removed):
 *     deleteIfExists(finalLogPath);    // ← deletes live .compacted.log
 *     deleteIfExists(finalIndexPath);  // ← deletes live .compacted.index
 *     <write staging .compacting.* files>
 *     <rename .compacting.* → .compacted.*>
 *
 *   During the window between delete and rename, LegacyConsumerDeliveryManager.findIndexPath()
 *   scans the partition directory for "*.index" files.  The only index left is the active
 *   segment's (e.g. 00000000000000000007.index, baseOffset=7).  If the consumer's committed
 *   offset is ≤ 6 (inside the deleted compacted range), that index is skipped because
 *   baseOffset(7) > startOffset(≤6).  findIndexPath() returns null → topic is silently
 *   dropped from the merged batch → offset never advances.
 *
 *   Fixed code:
 *     Only staging (.compacting.*) files are pre-deleted.
 *     finalise() uses Files.move(…, ATOMIC_MOVE) which maps to rename(2) — atomically
 *     replaces the destination without ever having a missing-file window.
 *
 * Test structure:
 *  Phase 1 — write duplicate records, seal segment, first compact  →  .compacted.* created
 *  Phase 2 — write Phase-2 records to the active segment
 *  Phase 3 — connect legacy client; run second compact IN THE BACKGROUND so that delivery
 *             attempts race against the file-swap; write Phase-3 records simultaneously
 *  Assertions — all Phase-2 and Phase-3 records received; committed offset advances past
 *               the compacted segment boundary
 *
 * Why this catches the regression:
 *   With the old code, every delivery attempt whose startOffset falls inside the
 *   (temporarily-deleted) compacted range returns a null cursor.  Running compaction 5 times
 *   in quick succession creates multiple stall windows.  Records whose msgKeys are only in the
 *   compacted range cannot be delivered until the next compaction completes — causing
 *   PollingConditions to timeout if the stall windows collectively exceed the assertion budget.
 */
class CompactionRaceWindowLegacyDeliveryJourneySpec extends BrokerSystemTestSupport {

    LegacyConsumerClient legacyClient

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-index-race"
        // 512-byte segment limit.  Records are ~29 B each, so all Phase-1 records fit in the
        // active segment without a natural rollover.  forceRollActiveSegment() is called before
        // compact() to explicitly seal the segment; the resulting compacted segment has baseOffset=0
        // and the new active segment starts at offset 7.
        base['broker.storage.segment-size'] = '512'
        return base
    }

    def setup() {
        // legacyClient is created inside the test AFTER Phase-1 compaction runs.
        // Connecting before compaction would cause Phase-1 records to be delivered
        // before the index is populated, defeating the delivery-filter dedup assertion.
    }

    def cleanup() {
        legacyClient?.close()
    }

    // ── Main regression scenario ──────────────────────────────────────────────

    def "legacy consumer offset advances through compacted range even when compaction rewrites it concurrently"() {
        given: "Phase-1: write records with duplicate keys to fill and seal a segment"
        // crw-A appears 3 times; compaction will keep only v3 (the latest).
        // crw-B / crw-C / crw-D are unique survivors.  All 4 keys land inside the
        // compacted segment (offsets 1..6, baseOffset 0).
        cloudServer.enqueueMessages([
            [offset:  1L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-A', eventType: 'MESSAGE', data: '{"v":1}'],
            [offset:  2L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-B', eventType: 'MESSAGE', data: '{"b":1}'],
            [offset:  3L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-A', eventType: 'MESSAGE', data: '{"v":2}'],
            [offset:  4L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-C', eventType: 'MESSAGE', data: '{"c":1}'],
            [offset:  5L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-A', eventType: 'MESSAGE', data: '{"v":3}'],
            [offset:  6L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-D', eventType: 'MESSAGE', data: '{"d":1}'],
        ])

        and: "first compaction creates the .compacted.log + .compacted.index pair"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            assert storage.read('prices-v1', 0, 6L, 1).any { it.msgKey == 'crw-D' }
        }
        // Force-seal the active segment before compacting.  Records are ~29 B each, so all 6
        // fit comfortably in a 512 B segment without triggering a natural rollover.
        // compact() only processes SEALED (inactive) segments; without an explicit seal
        // getInactiveSegments() returns empty and compact() is a no-op.
        def segmentAccess = brokerCtx.getBean(
            Class.forName('com.messaging.storage.segment.SegmentAccess'))
        segmentAccess.getSegmentManager('prices-v1', 0)?.forceRollActiveSegment()
        scheduler.compact() // seals & compacts; crw-A v1/v2 removed, v3 + B/C/D survive

        and: "connect legacy client AFTER compaction so the delivery-filter index is populated"
        // Connecting BEFORE Phase-1 compaction would let the broker deliver all 3 crw-A
        // versions before the index is built — hasIndexedKeysForTopic() returns false and
        // the filter fast-path returns the unfiltered batch, causing the final assertion
        // ('crw-A delivered at most once') to fail.
        legacyClient = LegacyConsumerClient.connect('127.0.0.1', brokerTcpPort, 'price-quote-service')
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert legacyClient.received.any { it instanceof ReadyEvent }
        }
        legacyClient.sendAck()   // ACK startup READY
        legacyClient.clearReceived()

        when: "Phase-2: new records land in the active segment (after first compaction)"
        // These offsets are ABOVE the compacted segment range (> 6), so they go to the
        // active segment.  Their delivery requires the legacy cursor to traverse BOTH
        // the compacted segment (crw-B/C/D offsets 2/4/6) and then the active segment.
        cloudServer.enqueueMessages([
            [offset:  7L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-E', eventType: 'MESSAGE', data: '{"e":1}'],
            [offset:  8L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-F', eventType: 'MESSAGE', data: '{"f":1}'],
        ])

        and: "Phase-3: second compaction runs in the background (simulates the scheduled compactor)"
        // Running it concurrently with delivery maximises the chance that a delivery attempt
        // races against the file-swap window.  With the old code this would delete
        // .compacted.index before the rename, causing findIndexPath() to return null for
        // any startOffset <= 6 and silently drop prices-v1 from the merged batch.
        def compactionDone = new CountDownLatch(1)
        Thread.start {
            try {
                // Run compact 5 times in quick succession to create many stall-window
                // opportunities.  Each run may create a new .compacted.* pair.
                5.times {
                    scheduler.compact()
                }
            } finally {
                compactionDone.countDown()
            }
        }

        and: "Phase-3 records arrive while compaction is rewriting the .compacted.* files"
        cloudServer.enqueueMessages([
            [offset:  9L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-G', eventType: 'MESSAGE', data: '{"g":1}'],
            [offset: 10L, topic: 'prices-v1', partition: 0,
             msgKey: 'crw-H', eventType: 'MESSAGE', data: '{"h":1}'],
        ])

        then: "first batch contains compacted segment records (proves atomic-move fix — no stall)"
        // Core regression: with the old delete-before-rename code, .compacted.index was absent
        // during the file-swap window, causing findIndexPath() to return null and silently drop
        // crw-B/C/D from the merged batch.  ATOMIC_MOVE guarantees the index is never absent,
        // so these keys must always be present in the first delivery from the compacted segment.
        // Client connects AFTER first compaction so the index is populated and crw-A v1/v2 are
        // filtered (only v3 at offset 5 survives).  crw-E/F are in the active segment and come
        // in a separate batch after this one is ACKed.
        new PollingConditions(timeout: 20, delay: 0.2).eventually {
            def keys = legacyClient.received
                .findAll { it instanceof BatchEvent }
                .collectMany { (it as BatchEvent).messages*.key }
                .toSet()
            assert keys.contains('crw-B') : "crw-B (compacted range) missing — possible race stall"
            assert keys.contains('crw-C') : "crw-C (compacted range) missing"
            assert keys.contains('crw-D') : "crw-D (compacted range) missing"
        }

        and: "compaction rounds all complete (no hung compactor)"
        compactionDone.await(10, TimeUnit.SECONDS)

        when: "client ACKs the compacted-segment batch to unblock active-segment delivery"
        // crw-E/F are in the active segment; crw-G/H arrived during concurrent compaction.
        // The broker holds these until the first ACK is received (maxInFlightPerTopic=1).
        legacyClient.received.findAll { it instanceof BatchEvent }
                              .each { legacyClient.sendAck() }

        then: "active-segment records (crw-E/F) arrive after the first ACK"
        // These keys are in the active segment and come in a separate batch from the
        // compacted-segment records because the index snapshot taken at cursor creation
        // covers only the records available at that moment.
        new PollingConditions(timeout: 15, delay: 0.2).eventually {
            def keys = legacyClient.received
                .findAll { it instanceof BatchEvent }
                .collectMany { (it as BatchEvent).messages*.key }
                .toSet()
            assert keys.contains('crw-E') : "crw-E (active segment) missing"
            assert keys.contains('crw-F') : "crw-F (active segment) missing"
        }

        when: "ACK active-segment batch to unblock Phase-3 records"
        legacyClient.received.findAll { it instanceof BatchEvent }
                              .each { legacyClient.sendAck() }

        then: "Phase-3 records written during concurrent compaction are delivered"
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            def keys = legacyClient.received
                .findAll { it instanceof BatchEvent }
                .collectMany { (it as BatchEvent).messages*.key }
                .toSet()
            assert keys.contains('crw-G') : "crw-G (during compaction) missing"
            assert keys.contains('crw-H') : "crw-H (during compaction) missing"
        }

        when: "ACK Phase-3 batch so the broker commits the final offset"
        legacyClient.received.findAll { it instanceof BatchEvent }
                              .each { legacyClient.sendAck() }

        then: "committed offset advances past the compacted segment range (≥ offset 10)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('price-quote-service:prices-v1') >= 10L
        }

        and: "crw-A was delivered at most once across all batches (delivery filter deduped superseded versions)"
        // Only crw-A v3 (offset 5) should reach the client; v1 (offset 1) and v2 (offset 3)
        // are superseded in the compaction index and must be filtered by applyCompactionFilter().
        legacyClient.received
            .findAll { it instanceof BatchEvent }
            .collectMany { (it as BatchEvent).messages*.key }
            .count { it == 'crw-A' } <= 1

        and: "no wire errors on the legacy connection throughout"
        legacyClient.errors.isEmpty()
    }
}
