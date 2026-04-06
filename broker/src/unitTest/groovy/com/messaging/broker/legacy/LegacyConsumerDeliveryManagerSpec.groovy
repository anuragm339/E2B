package com.messaging.broker.legacy

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant

/**
 * Unit tests for LegacyConsumerDeliveryManager.buildMergedBatch().
 *
 * Covers:
 *  - Fix 3: storage.read() called with MSG_CHUNK=50 instead of 1 per message
 *  - Global offset ordering preserved across topics
 *  - maxBytes limit respected
 *  - Empty / exhausted topic handling
 */
class LegacyConsumerDeliveryManagerSpec extends Specification {

    @TempDir
    Path tempDir

    StorageEngine storage
    ConsumerOffsetTracker offsetTracker
    BrokerMetrics metrics
    LegacyConsumerDeliveryManager manager

    def setup() {
        storage = Mock(StorageEngine)
        offsetTracker = Mock(ConsumerOffsetTracker)
        metrics = Mock(BrokerMetrics)
        // Point the manager at our temp directory
        System.setProperty("broker.storage.dataDir", tempDir.toString())
        manager = new LegacyConsumerDeliveryManager(storage, offsetTracker, metrics, tempDir.toString())
    }

    def cleanup() {
        System.clearProperty("broker.storage.dataDir")
    }

    // ──────────────────────────────────────────────────────────────
    // Index + message helpers
    // ──────────────────────────────────────────────────────────────

    /**
     * Create topic directory structure and write a v2 index file.
     * Returns the index file path (not normally needed by callers).
     */
    private void prepareTopicIndex(String topic, List<Long> offsets) {
        Path partDir = tempDir.resolve("${topic}/partition-0")
        partDir.toFile().mkdirs()
        Path idxPath = partDir.resolve("00000000000000000000.index")

        FileChannel ch = FileChannel.open(idxPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)
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
                e.putInt(i * 100)  // logPosition
                e.putInt(50)       // recordSize
                e.flip()
                ch.write(e, pos)
                pos += 16
            }
        } finally { ch.close() }
    }

    /** Create a minimal MessageRecord at the given offset. */
    private static MessageRecord makeMsg(String topic, long offset) {
        def msg = new MessageRecord(offset, topic, 0, "key-${offset}",
                EventType.MESSAGE, '{"v":' + offset + '}', Instant.EPOCH)
        return msg
    }

    /** Build a list of MSG_CHUNK (50) messages starting at fromOffset. */
    private static List<MessageRecord> chunkOf(String topic, long fromOffset, int count = 50) {
        return (fromOffset..<(fromOffset + count)).collect { makeMsg(topic, it) }
    }

    // ──────────────────────────────────────────────────────────────
    // Fix 3 — chunked storage.read() calls
    // ──────────────────────────────────────────────────────────────

    def "storage.read is called with MSG_CHUNK=50, not once per message"() {
        given: "100-entry index for one topic"
        prepareTopicIndex("prices-v1", (0L..99L).toList())
        offsetTracker.getOffset("test-group:prices-v1") >> -1L
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        storage.getCurrentOffset("prices-v1", 0) >> 99L

        when:
        def batch = manager.buildMergedBatch(["prices-v1"], "test-group", Long.MAX_VALUE)

        then: "two chunk reads (50+50), never single-record reads"
        1 * storage.read("prices-v1", 0, 0L, 50) >> chunkOf("prices-v1", 0L)
        1 * storage.read("prices-v1", 0, 50L, 50) >> chunkOf("prices-v1", 50L)
        0 * storage.read("prices-v1", 0, _, 1)

        and:
        batch.messageCount == 100
    }

    def "storage.read called once when index has fewer than MSG_CHUNK entries"() {
        given: "only 10 entries — fits in one chunk"
        prepareTopicIndex("small-topic", (0L..9L).toList())
        offsetTracker.getOffset("g:small-topic") >> -1L
        storage.getEarliestOffset("small-topic", 0) >> 0L
        storage.getCurrentOffset("small-topic", 0) >> 9L

        when:
        def batch = manager.buildMergedBatch(["small-topic"], "g", Long.MAX_VALUE)

        then: "one chunk read, no single-record reads"
        1 * storage.read("small-topic", 0, 0L, 50) >> chunkOf("small-topic", 0L, 10)
        0 * storage.read("small-topic", 0, _, 1)

        and:
        batch.messageCount == 10
    }

    def "committed offset advances start position and chunk read begins at correct offset"() {
        given: "75-entry index, consumer has committed up to offset 49"
        prepareTopicIndex("topic-a", (0L..74L).toList())
        offsetTracker.getOffset("g:topic-a") >> 49L  // committed
        storage.getEarliestOffset("topic-a", 0) >> 0L
        storage.getCurrentOffset("topic-a", 0) >> 74L

        when:
        def batch = manager.buildMergedBatch(["topic-a"], "g", Long.MAX_VALUE)

        then: "first (and only) chunk starts at offset 50 — not 0"
        1 * storage.read("topic-a", 0, 50L, 50) >> chunkOf("topic-a", 50L, 25)
        0 * storage.read("topic-a", 0, 0L, _)

        and:
        batch.messageCount == 25
    }

    // ──────────────────────────────────────────────────────────────
    // Global offset ordering
    // ──────────────────────────────────────────────────────────────

    def "two-topic merge produces globally sorted output"() {
        given: "topic-A: offsets 0,2,4,6  | topic-B: offsets 1,3,5,7"
        prepareTopicIndex("topic-a", [0L, 2L, 4L, 6L])
        prepareTopicIndex("topic-b", [1L, 3L, 5L, 7L])

        offsetTracker.getOffset("g:topic-a") >> -1L
        offsetTracker.getOffset("g:topic-b") >> -1L
        storage.getEarliestOffset("topic-a", 0) >> 0L
        storage.getEarliestOffset("topic-b", 0) >> 1L
        storage.getCurrentOffset("topic-a", 0) >> 6L
        storage.getCurrentOffset("topic-b", 0) >> 7L

        storage.read("topic-a", 0, 0L, 50) >> [makeMsg("topic-a", 0L), makeMsg("topic-a", 2L),
                                                makeMsg("topic-a", 4L), makeMsg("topic-a", 6L)]
        storage.read("topic-b", 0, 1L, 50) >> [makeMsg("topic-b", 1L), makeMsg("topic-b", 3L),
                                                makeMsg("topic-b", 5L), makeMsg("topic-b", 7L)]

        when:
        def batch = manager.buildMergedBatch(["topic-a", "topic-b"], "g", Long.MAX_VALUE)

        then: "8 messages in globally ascending offset order"
        batch.messageCount == 8
        batch.messages*.offset == [0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L]
    }

    def "uneven topics: large topic interleaves correctly with small topic"() {
        given: "topic-big: offsets 0..9  |  topic-small: offsets 5,6 only"
        prepareTopicIndex("topic-big",   (0L..9L).toList())
        prepareTopicIndex("topic-small", [5L, 6L])

        offsetTracker.getOffset("g:topic-big")   >> -1L
        offsetTracker.getOffset("g:topic-small") >> -1L
        storage.getEarliestOffset("topic-big", 0)   >> 0L
        storage.getEarliestOffset("topic-small", 0) >> 5L
        storage.getCurrentOffset("topic-big", 0)   >> 9L
        storage.getCurrentOffset("topic-small", 0) >> 6L

        storage.read("topic-big", 0, 0L, 50)   >> (0L..9L).collect { makeMsg("topic-big", it) }
        storage.read("topic-small", 0, 5L, 50) >> [makeMsg("topic-small", 5L), makeMsg("topic-small", 6L)]

        when:
        def batch = manager.buildMergedBatch(["topic-big", "topic-small"], "g", Long.MAX_VALUE)

        then: "12 messages globally sorted — small topic entries appear at positions 5 and 6"
        batch.messageCount == 12
        def offsets = batch.messages*.offset
        offsets == offsets.sort()  // globally ascending
        batch.messages.findAll { it.topic == "topic-small" }*.offset == [5L, 6L]
    }

    def "messages within a single topic remain in offset order"() {
        given: "100-entry single-topic index"
        prepareTopicIndex("ordered", (0L..99L).toList())
        offsetTracker.getOffset("g:ordered") >> -1L
        storage.getEarliestOffset("ordered", 0) >> 0L
        storage.getCurrentOffset("ordered", 0) >> 99L

        storage.read("ordered", 0, 0L, 50)  >> chunkOf("ordered", 0L)
        storage.read("ordered", 0, 50L, 50) >> chunkOf("ordered", 50L)

        when:
        def batch = manager.buildMergedBatch(["ordered"], "g", Long.MAX_VALUE)

        then: "offsets are strictly ascending"
        def offsets = batch.messages*.offset
        offsets == offsets.sort()
        offsets.first() == 0L
        offsets.last()  == 99L
    }

    // ──────────────────────────────────────────────────────────────
    // Edge cases
    // ──────────────────────────────────────────────────────────────

    def "empty topic list returns empty batch without calling storage"() {
        when:
        def batch = manager.buildMergedBatch([], "g", Long.MAX_VALUE)

        then:
        batch.isEmpty()
        0 * storage.read(_, _, _, _)
    }

    def "topic with no data (currentOffset = -1) is skipped gracefully"() {
        given: "storage reports no data"
        storage.getCurrentOffset("empty-topic", 0) >> -1L
        storage.getEarliestOffset("empty-topic", 0) >> 0L
        offsetTracker.getOffset("g:empty-topic") >> -1L

        when:
        def batch = manager.buildMergedBatch(["empty-topic"], "g", Long.MAX_VALUE)

        then:
        batch.isEmpty()
        0 * storage.read(_, _, _, _)
    }

    def "startOffset beyond currentOffset produces empty batch"() {
        given: "consumer is fully caught up (committed == current)"
        prepareTopicIndex("caught-up", (0L..9L).toList())
        offsetTracker.getOffset("g:caught-up") >> 9L  // committed at last
        storage.getEarliestOffset("caught-up", 0) >> 0L
        storage.getCurrentOffset("caught-up", 0) >> 9L  // startOffset would be 10 > 9

        when:
        def batch = manager.buildMergedBatch(["caught-up"], "g", Long.MAX_VALUE)

        then:
        batch.isEmpty()
        0 * storage.read(_, _, _, _)
    }

    def "maxBytes limit stops the merge loop early"() {
        given: "100-entry index"
        prepareTopicIndex("big", (0L..99L).toList())
        offsetTracker.getOffset("g:big") >> -1L
        storage.getEarliestOffset("big", 0) >> 0L
        storage.getCurrentOffset("big", 0) >> 99L
        // Each message is ~60 bytes (key + data + 50 overhead per MergedBatch.estimateMessageSize)
        storage.read("big", 0, 0L, 50) >> chunkOf("big", 0L)

        when: "maxBytes set very small — only allows a few messages"
        def batch = manager.buildMergedBatch(["big"], "g", 200L)

        then: "batch stops well before 100 messages"
        batch.messageCount < 100
        batch.totalBytes >= 200L  // stopped at or just over the limit
    }
}
