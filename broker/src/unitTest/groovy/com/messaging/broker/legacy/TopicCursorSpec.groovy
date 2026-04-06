package com.messaging.broker.legacy

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * Unit tests for TopicCursor.
 *
 * Covers:
 *  - Fix 1: O(log n) binary seek in seekToOffset()
 *  - Fix 2: Bulk index reads via reusable ByteBuffer (READ_CHUNK=64)
 *  - V1 legacy (20-byte) and V2 current (16-byte) index formats
 *  - peek/advance contract
 */
class TopicCursorSpec extends Specification {

    @TempDir
    Path tempDir

    // ──────────────────────────────────────────────────────────────
    // Index file helpers
    // ──────────────────────────────────────────────────────────────

    /** Write a v2 (16-byte entries) index file to tempDir. */
    private Path writeV2Index(String name, List<Long> offsets) {
        Path path = tempDir.resolve(name)
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        try {
            ByteBuffer hdr = ByteBuffer.allocate(6)
            hdr.put("MIDX".getBytes("UTF-8"))
            hdr.putShort((short) 2)
            hdr.flip()
            ch.write(hdr, 0L)

            long pos = 6L
            offsets.eachWithIndex { long offset, int i ->
                ByteBuffer e = ByteBuffer.allocate(16)
                e.putLong(offset)
                e.putInt(i * 100)   // logPosition
                e.putInt(50)        // recordSize
                e.flip()
                ch.write(e, pos)
                pos += 16
            }
        } finally { ch.close() }
        return path
    }

    /** Write a v1 (20-byte entries with crc32) index file to tempDir. */
    private Path writeV1Index(String name, List<Long> offsets) {
        Path path = tempDir.resolve(name)
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        try {
            ByteBuffer hdr = ByteBuffer.allocate(6)
            hdr.put("MIDX".getBytes("UTF-8"))
            hdr.putShort((short) 1)
            hdr.flip()
            ch.write(hdr, 0L)

            long pos = 6L
            offsets.eachWithIndex { long offset, int i ->
                ByteBuffer e = ByteBuffer.allocate(20)
                e.putLong(offset)
                e.putInt(i * 100)   // logPosition
                e.putInt(50)        // recordSize
                e.putInt(0xDEAD)    // crc32
                e.flip()
                ch.write(e, pos)
                pos += 20
            }
        } finally { ch.close() }
        return path
    }

    // ──────────────────────────────────────────────────────────────
    // Fix 1 — Binary seek correctness
    // ──────────────────────────────────────────────────────────────

    def "seekToOffset positions at exact target in a 1000-entry index"() {
        given: "v2 index with offsets 0..999"
        Path idx = writeV2Index("large.index", (0L..999L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 500L)

        then: "peek returns offset 500, not 0"
        cursor.peek().offset == 500L

        cleanup: cursor.close()
    }

    def "seekToOffset positions at first entry when target is below all offsets"() {
        given: "entries starting at offset 100"
        Path idx = writeV2Index("below.index", [100L, 200L, 300L])

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 0L)

        then: "cursor positioned at first entry (100)"
        cursor.peek().offset == 100L

        cleanup: cursor.close()
    }

    def "seekToOffset positions at first entry >= target when target falls between entries"() {
        given: "entries at offsets 0, 10, 20, 30, 40"
        Path idx = writeV2Index("gap.index", [0L, 10L, 20L, 30L, 40L])

        when: "seek to 15 — between 10 and 20"
        TopicCursor cursor = new TopicCursor("t", idx, 15L)

        then: "lands on 20 (first entry >= 15)"
        cursor.peek().offset == 20L

        cleanup: cursor.close()
    }

    def "seekToOffset exhausts cursor when target exceeds all entries"() {
        given:
        Path idx = writeV2Index("exhaust.index", (0L..9L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 1000L)

        then:
        !cursor.hasMore()
        cursor.peek() == null

        cleanup: cursor.close()
    }

    def "seekToOffset handles single-entry index"() {
        given:
        Path idx = writeV2Index("single.index", [42L])

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 42L)

        then:
        cursor.peek().offset == 42L
        cursor.hasMore()

        cleanup: cursor.close()
    }

    def "empty index file exhausts cursor immediately"() {
        given: "header only, no entries"
        Path idx = writeV2Index("empty.index", [])

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 0L)

        then:
        !cursor.hasMore()
        cursor.advance() == null

        cleanup: cursor.close()
    }

    def "seekToOffset on last entry of a large index"() {
        given: "1000 entries, seek to last offset 999"
        Path idx = writeV2Index("seek-last.index", (0L..999L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 999L)

        then:
        cursor.peek().offset == 999L

        when: "advance consumes the last entry"
        cursor.advance()

        then: "cursor exhausted"
        !cursor.hasMore()

        cleanup: cursor.close()
    }

    // ──────────────────────────────────────────────────────────────
    // Fix 2 — Bulk buffer correctness
    // ──────────────────────────────────────────────────────────────

    def "bulk buffer reads all 130 entries correctly across the READ_CHUNK=64 boundary"() {
        given: "130 sequential entries (crosses two full buffer fills)"
        Path idx = writeV2Index("bulk-130.index", (0L..129L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 0L)
        List<Long> result = []
        while (cursor.hasMore()) { result << cursor.advance().offset }

        then: "all 130 entries in order"
        result.size() == 130
        result == (0L..129L).toList()

        cleanup: cursor.close()
    }

    def "bulk buffer reads exactly READ_CHUNK=64 entries"() {
        given:
        Path idx = writeV2Index("bulk-64.index", (0L..63L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 0L)
        List<Long> result = []
        while (cursor.hasMore()) { result << cursor.advance().offset }

        then:
        result.size() == 64
        result == (0L..63L).toList()

        cleanup: cursor.close()
    }

    def "bulk buffer combined with binary seek returns correct tail of index"() {
        given: "200 entries; seek to offset 100"
        Path idx = writeV2Index("seek-bulk.index", (0L..199L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 100L)
        List<Long> result = []
        while (cursor.hasMore()) { result << cursor.advance().offset }

        then: "entries 100..199 only"
        result.size() == 100
        result[0]  == 100L
        result[-1] == 199L

        cleanup: cursor.close()
    }

    def "bulk buffer: seek into second buffer page then reads rest correctly"() {
        given: "150 entries; seek to offset 80 (inside second 64-entry page)"
        Path idx = writeV2Index("seek-page2.index", (0L..149L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 80L)
        List<Long> result = []
        while (cursor.hasMore()) { result << cursor.advance().offset }

        then:
        result[0]  == 80L
        result[-1] == 149L
        result.size() == 70

        cleanup: cursor.close()
    }

    // ──────────────────────────────────────────────────────────────
    // V1 legacy format
    // ──────────────────────────────────────────────────────────────

    def "v1 legacy 20-byte entries are read correctly"() {
        given: "v1 index with 5 entries"
        Path idx = writeV1Index("v1.index", [10L, 20L, 30L, 40L, 50L])

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 0L)
        List<IndexEntry> entries = []
        while (cursor.hasMore()) { entries << cursor.advance() }

        then:
        entries.size() == 5
        entries*.offset == [10L, 20L, 30L, 40L, 50L]
        entries[0].crc32 == 0xDEAD   // crc32 field populated for v1

        cleanup: cursor.close()
    }

    def "v1 format: binary seekToOffset works with 20-byte entries"() {
        given: "v1 index with entries at 0, 10, 20, … 90 (10 entries)"
        List<Long> offsets = (0L..9L).collect { it * 10L }
        Path idx = writeV1Index("v1-seek.index", offsets)

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 50L)

        then:
        cursor.peek().offset == 50L

        cleanup: cursor.close()
    }

    def "v1 format: bulk buffer reads 130 entries correctly"() {
        given:
        Path idx = writeV1Index("v1-bulk.index", (0L..129L).toList())

        when:
        TopicCursor cursor = new TopicCursor("t", idx, 0L)
        List<Long> result = []
        while (cursor.hasMore()) { result << cursor.advance().offset }

        then:
        result.size() == 130
        result == (0L..129L).toList()

        cleanup: cursor.close()
    }

    // ──────────────────────────────────────────────────────────────
    // peek / advance contract
    // ──────────────────────────────────────────────────────────────

    def "peek does not advance the cursor"() {
        given:
        Path idx = writeV2Index("peek.index", [1L, 2L, 3L])
        TopicCursor cursor = new TopicCursor("t", idx, 0L)

        when: "peek called twice"
        IndexEntry a = cursor.peek()
        IndexEntry b = cursor.peek()

        then: "same entry returned both times"
        a.offset == b.offset
        a.offset == 1L

        cleanup: cursor.close()
    }

    def "advance updates lastOffsetDelivered after each call"() {
        given:
        Path idx = writeV2Index("adv.index", [5L, 10L, 15L])
        TopicCursor cursor = new TopicCursor("t", idx, 0L)

        expect: "initial lastOffsetDelivered is startOffset - 1"
        cursor.getLastOffsetDelivered() == -1L

        when: cursor.advance()
        then: cursor.getLastOffsetDelivered() == 5L

        when: cursor.advance()
        then: cursor.getLastOffsetDelivered() == 10L

        when: cursor.advance()
        then: cursor.getLastOffsetDelivered() == 15L

        cleanup: cursor.close()
    }

    def "advance returns null and leaves lastOffsetDelivered unchanged when exhausted"() {
        given:
        Path idx = writeV2Index("null-adv.index", [7L])
        TopicCursor cursor = new TopicCursor("t", idx, 0L)
        cursor.advance()  // consume the only entry

        when:
        IndexEntry result = cursor.advance()

        then:
        result == null
        cursor.getLastOffsetDelivered() == 7L

        cleanup: cursor.close()
    }
}
