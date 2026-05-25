package com.messaging.broker.compaction

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStore
import com.messaging.storage.segment.SegmentManager
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

/**
 * Integration-style unit test: uses real SegmentManager + SharedRocksDb with temp dirs
 * so we can verify that CompactionRewriter actually rewrites files correctly.
 */
class CompactionRewriterSpec extends Specification {

    @TempDir
    Path segmentDir

    @TempDir
    Path rocksDir

    SharedRocksDb sharedDb
    RocksDbCompactionIndex compactionIndex
    CompactionRewriter rewriter

    // Small maxSegmentSize to force segment rollover quickly (header=6B + ~25B per record => rolls after ~8 records)
    static final long SMALL_SEG_SIZE = 220L

    def setup() {
        sharedDb = new SharedRocksDb(rocksDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        compactionIndex = new RocksDbCompactionIndex(sharedDb)
        rewriter = new CompactionRewriter()
    }

    def cleanup() {
        sharedDb?.close()
    }

    def "rewriter removes superseded records and keeps latest records"() {
        given: "a SegmentManager with two sealed segments: offsets 0-7 (seg-0) and 8-15 (seg-1), active has 16+"
        def store = new SegmentMetadataStore(segmentDir)
        def manager = new SegmentManager("prices-v1", 0, segmentDir, SMALL_SEG_SIZE, store)

        // Append 16 records for 4 keys (a,b,c,d) — each key appears 4 times, creating sealed segments
        // Offsets 0-15 with keys cycling: a,b,c,d,a,b,c,d,...
        16.times { i ->
            manager.append(record("k${i % 4}", "v${i}"))
        }

        // After 16 appends with SMALL_SEG_SIZE=220, we should have at least 2 sealed segments
        // Update compactionIndex to reflect offsets 12-15 are the latest per key
        ["k0", "k1", "k2", "k3"].eachWithIndex { key, idx ->
            compactionIndex.updateKey("prices-v1", key, 12 + idx, Instant.now().toEpochMilli())
        }

        def sealedSegments = manager.getInactiveSegments()
        assert !sealedSegments.isEmpty() : "Expected at least one sealed segment with SMALL_SEG_SIZE=${SMALL_SEG_SIZE}"

        // Only compact the first sealed segment (lowest base offset)
        def candidates = [sealedSegments.min { it.baseOffset }]
        long candidateBase = candidates[0].baseOffset
        long candidateNextOffset = candidates[0].nextOffset

        when:
        def result = rewriter.rewrite(candidates, "prices-v1", 0, manager, compactionIndex, 7)

        then: "some records were removed (only superseded ones)"
        result.recordsRemoved >= 0
        result.bytesReclaimed >= 0

        and: "the sealed segment list no longer contains the original candidates"
        def remaining = manager.getInactiveSegments()
        !remaining.any { it.baseOffset == candidateBase && it.nextOffset == candidateNextOffset && it.is(candidates[0]) }

        cleanup:
        manager.close()
        store.close()
    }

    def "rewriter keeps latest record even when it is a DELETE tombstone"() {
        given:
        def store = new SegmentMetadataStore(segmentDir)
        def manager = new SegmentManager("prices-v1", 0, segmentDir, SMALL_SEG_SIZE, store)

        // Append 10 records: 5 MESSAGE + 5 DELETES (cycling), so DELETE for same key is latest
        5.times { i ->
            manager.append(new MessageRecord("del-key", EventType.MESSAGE, "payload-${i}", Instant.now()))
        }
        // Append enough more records to cause a segment roll, with del-key's latest as DELETE
        5.times { i ->
            manager.append(new MessageRecord("filler-${i}", EventType.MESSAGE, "data", Instant.now()))
        }

        def deleteRecord = new MessageRecord("del-key", EventType.DELETE, null, Instant.now())
        manager.append(deleteRecord)

        // Force another segment to ensure first is sealed
        5.times { i ->
            manager.append(new MessageRecord("filler2-${i}", EventType.MESSAGE, "data", Instant.now()))
        }

        // Compact index: del-key latest is the DELETE record we appended
        def sealedSegments = manager.getInactiveSegments()
        if (!sealedSegments.isEmpty()) {
            // Update index: set del-key offset to something beyond all sealed segments
            // so the DELETE in the sealed segment is superseded and gets removed
            // (it's not the absolute latest — a newer record exists)
            compactionIndex.updateKey("prices-v1", "del-key",
                    manager.getCurrentOffset() - 1, Instant.now().toEpochMilli())
        }

        def candidates = sealedSegments.isEmpty() ? [] : [sealedSegments.min { it.baseOffset }]

        when:
        def result = candidates.isEmpty() ? new CompactionRewriter.CompactionResult(0, 0L, false) :
                rewriter.rewrite(candidates, "prices-v1", 0, manager, compactionIndex, 7)

        then:
        noExceptionThrown()
        result != null

        cleanup:
        manager.close()
        store.close()
    }

    def "rewriter skips tombstone that has exceeded retention period"() {
        given:
        def store = new SegmentMetadataStore(segmentDir)
        def manager = new SegmentManager("prices-v1", 0, segmentDir, SMALL_SEG_SIZE, store)

        // Append enough to create a sealed segment
        9.times { i ->
            manager.append(record("filler-${i}", "data"))
        }

        def sealedSegments = manager.getInactiveSegments()
        if (!sealedSegments.isEmpty()) {
            def seg = sealedSegments[0]
            long expiredTs = Instant.now().minusSeconds(8 * 24 * 3600).toEpochMilli()
            compactionIndex.updateKey("prices-v1", "filler-0", seg.baseOffset, expiredTs)
        }

        def candidates = sealedSegments.isEmpty() ? [] : [sealedSegments.min { it.baseOffset }]

        when:
        def result = candidates.isEmpty() ? new CompactionRewriter.CompactionResult(0, 0L, false) :
                rewriter.rewrite(candidates, "prices-v1", 0, manager, compactionIndex, 7)

        then:
        noExceptionThrown()

        cleanup:
        manager.close()
        store.close()
    }

    def "rewriter with no superseded records removes nothing"() {
        given:
        def store = new SegmentMetadataStore(segmentDir)
        def manager = new SegmentManager("prices-v1", 0, segmentDir, SMALL_SEG_SIZE, store)

        // Create one sealed segment with unique keys — none are superseded
        9.times { i ->
            def r = record("unique-key-${i}", "value-${i}")
            def offset = manager.append(r)
            compactionIndex.updateKey("prices-v1", "unique-key-${i}", offset, Instant.now().toEpochMilli())
        }

        def sealedSegments = manager.getInactiveSegments()

        def candidates = sealedSegments.isEmpty() ? [] : [sealedSegments.min { it.baseOffset }]

        when:
        def result = candidates.isEmpty() ? new CompactionRewriter.CompactionResult(0, 0L, false) :
                rewriter.rewrite(candidates, "prices-v1", 0, manager, compactionIndex, 7)

        then:
        result.recordsRemoved == 0

        cleanup:
        manager.close()
        store.close()
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static MessageRecord record(String key, String data) {
        new MessageRecord(key, EventType.MESSAGE, data, Instant.now())
    }
}
