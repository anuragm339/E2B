package com.messaging.broker.consumer

import com.messaging.broker.ack.AckRecord
import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.legacy.LegacyConsumerDeliveryManager
import com.messaging.broker.legacy.MergedBatch
import com.messaging.broker.model.DeliveryKey
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.broker.monitoring.ConsumerEventLogger
import com.messaging.broker.model.ConsumerKey
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.ExecutorService

class BatchAckServiceRocksDbSpec extends Specification {

    ConsumerStateService stateService = Mock()
    PendingAckStore pendingAckStore = Mock()
    ConsumerOffsetTracker offsetTracker = Mock()
    BrokerMetrics metrics = Mock()
    StorageEngine storage = Mock()
    ConsumerRegistrationService registrationService = Mock()
    LegacyConsumerDeliveryManager legacyDeliveryManager = Mock()
    ConsumerEventLogger consumerLogger = Mock()
    RocksDbAckStore ackStore = Mock()

    // Synchronous executor so async code runs inline in tests
    ExecutorService syncStorageExecutor = Mock(ExecutorService) {
        execute(_ as Runnable) >> { Runnable r -> r.run() }
    }

    BatchAckService service

    def setup() {
        service = new BatchAckService(
                stateService, pendingAckStore, offsetTracker, metrics, storage,
                registrationService, legacyDeliveryManager, consumerLogger,
                ackStore, syncStorageExecutor)
    }

    // ── Modern ACK path ───────────────────────────────────────────────────────

    def "modern ACK path calls ackStore.putBatch with correct msgKeys"() {
        given:
        def deliveryKey = DeliveryKey.of("group1", "prices-v1")
        stateService.getTraceId(deliveryKey) >> "trace-1"
        stateService.getBatchSendTime(deliveryKey) >> 1000L
        stateService.removePendingOffset(deliveryKey) >> 5L       // nextOffset = 5 (atomic remove)
        stateService.getFromOffset(deliveryKey) >> 2L             // startOffset = 2

        def consumerKey = ConsumerKey.of("client-1", "prices-v1", "group1")
        def consumer = Mock(RemoteConsumer)
        consumer.getCurrentOffset() >> 5L
        registrationService.getConsumer(consumerKey) >> Optional.of(consumer)

        // storage.read returns 3 messages starting at offset 2
        def msg1 = new MessageRecord(2L, "prices-v1", 0, "key-A", null, null, Instant.now())
        def msg2 = new MessageRecord(3L, "prices-v1", 0, "key-B", null, null, Instant.now())
        def msg3 = new MessageRecord(4L, "prices-v1", 0, null, null, null, Instant.now()) // null msgKey
        storage.read("prices-v1", 0, 2L, 3) >> [msg1, msg2, msg3]
        storage.getCurrentOffset("prices-v1", 0) >> 5L

        when:
        service.handleModernBatchAck("client-1", "prices-v1", "group1")

        then:
        1 * ackStore.putBatch(
                { String[] t -> t.length == 2 && t[0] == "prices-v1" && t[1] == "prices-v1" },
                { String[] g -> g[0] == "group1" && g[1] == "group1" },
                { String[] k -> k[0] == "key-A" && k[1] == "key-B" },
                { AckRecord[] r -> r[0].offset == 2L && r[1].offset == 3L }
        )
    }

    def "modern ACK path does not call ackStore when fromOffset is null"() {
        given:
        def deliveryKey = DeliveryKey.of("group1", "prices-v1")
        stateService.getTraceId(deliveryKey) >> "trace-1"
        stateService.getBatchSendTime(deliveryKey) >> 1000L
        stateService.removePendingOffset(deliveryKey) >> 5L       // atomic remove
        stateService.getFromOffset(deliveryKey) >> null           // not set

        def consumerKey = ConsumerKey.of("client-1", "prices-v1", "group1")
        def consumer = Mock(RemoteConsumer)
        consumer.getCurrentOffset() >> 5L
        registrationService.getConsumer(consumerKey) >> Optional.of(consumer)
        storage.getCurrentOffset("prices-v1", 0) >> 5L

        when:
        service.handleModernBatchAck("client-1", "prices-v1", "group1")

        then:
        0 * ackStore.putBatch(*_)
    }

    def "modern ACK path skips null msgKey without error"() {
        given:
        def deliveryKey = DeliveryKey.of("group1", "prices-v1")
        stateService.getTraceId(deliveryKey) >> "trace-1"
        stateService.getBatchSendTime(deliveryKey) >> 1000L
        stateService.getPendingOffset(deliveryKey) >> 3L
        stateService.getFromOffset(deliveryKey) >> 2L

        def consumerKey = ConsumerKey.of("client-1", "prices-v1", "group1")
        def consumer = Mock(RemoteConsumer)
        consumer.getCurrentOffset() >> 3L
        registrationService.getConsumer(consumerKey) >> Optional.of(consumer)

        def msgNullKey = new MessageRecord(2L, "prices-v1", 0, null, null, null, Instant.now())
        storage.read("prices-v1", 0, 2L, 1) >> [msgNullKey]
        storage.getCurrentOffset("prices-v1", 0) >> 3L

        when:
        service.handleModernBatchAck("client-1", "prices-v1", "group1")

        then:
        noExceptionThrown()
        0 * ackStore.putBatch(*_)
    }

    def "storage.read() size limit causes multi-chunk iteration — all records reach RocksDB"() {
        given: "a 101-record batch from offset 1000 to 1100 (nextOffset = 1101)"
        // Simulates the production bug: storage.read() stops early due to the 1MB internal
        // size cap (readWithSizeLimit default = 1MB). The first call returns only 60 records
        // (simulating truncation), the second call returns the remaining 41.
        // Without the loop fix, only the first 60 records would reach RocksDB.
        def deliveryKey = DeliveryKey.of("group-large", "prices-v1")
        stateService.getTraceId(deliveryKey)         >> "trace-large"
        stateService.getBatchSendTime(deliveryKey)   >> 9000L
        stateService.removePendingOffset(deliveryKey) >> 1101L   // nextOffset (lastOffset=1100)
        stateService.getFromOffset(deliveryKey)       >> 1000L   // first record offset

        def consumerKey = ConsumerKey.of("client-large", "prices-v1", "group-large")
        def consumer = Mock(RemoteConsumer) { getCurrentOffset() >> 1101L }
        registrationService.getConsumer(consumerKey) >> Optional.of(consumer)
        storage.getCurrentOffset("prices-v1", 0) >> 1101L

        // First read: simulate 1MB cut-off — returns only 60 of the 101 requested records
        def firstChunk = (1000..1059).collect { i ->
            new MessageRecord((long) i, "prices-v1", 0, "lk-${i}", null, null, Instant.now())
        }
        // chunkSize for 1st call = min(1101-1000, 500) = 101
        storage.read("prices-v1", 0, 1000L, 101) >> firstChunk

        // Second read: remaining 41 records (offsets 1060..1100)
        def secondChunk = (1060..1100).collect { i ->
            new MessageRecord((long) i, "prices-v1", 0, "lk-${i}", null, null, Instant.now())
        }
        // chunkSize for 2nd call = min(1101-1060, 500) = 41
        storage.read("prices-v1", 0, 1060L, 41) >> secondChunk

        when:
        service.handleModernBatchAck("client-large", "prices-v1", "group-large")

        then: "putBatch is called exactly once with all 101 msgKeys from both storage reads"
        1 * ackStore.putBatch(
            { String[] t -> t.length == 101 && t.every { it == "prices-v1" } },
            { String[] g -> g.length == 101 && g.every { it == "group-large" } },
            { String[] k ->
                // Verify keys from both chunks are present: first chunk ends at lk-1059,
                // second chunk starts at lk-1060 — a single storage.read() would miss lk-1060..lk-1100
                k.length == 101 &&
                k[0]   == 'lk-1000' &&   // first key from first chunk
                k[59]  == 'lk-1059' &&   // last key from first chunk (1MB cut-off point)
                k[60]  == 'lk-1060' &&   // first key from second chunk — proves loop ran
                k[100] == 'lk-1100'      // last key from second chunk
            },
            { AckRecord[] r ->
                r.length == 101 &&
                r[0].offset   == 1000L &&
                r[100].offset == 1100L
            }
        )
    }

    def "records at or beyond toOffset are excluded from the RocksDB write"() {
        given: "storage returns a record beyond the batch boundary (offset == toOffset)"
        def deliveryKey = DeliveryKey.of("group-boundary", "prices-v1")
        stateService.getTraceId(deliveryKey)          >> "trace-b"
        stateService.getBatchSendTime(deliveryKey)    >> 5000L
        stateService.removePendingOffset(deliveryKey) >> 10L   // nextOffset = 10 (toOffset)
        stateService.getFromOffset(deliveryKey)        >> 7L   // fromOffset

        def consumerKey = ConsumerKey.of("client-boundary", "prices-v1", "group-boundary")
        registrationService.getConsumer(consumerKey) >> Optional.of(Mock(RemoteConsumer) {
            getCurrentOffset() >> 10L
        })
        storage.getCurrentOffset("prices-v1", 0) >> 10L

        // Storage returns records including one at offset == toOffset (10), which must be excluded
        storage.read("prices-v1", 0, 7L, 3) >> [
            new MessageRecord(7L,  "prices-v1", 0, "in-batch-1", null, null, Instant.now()),
            new MessageRecord(8L,  "prices-v1", 0, "in-batch-2", null, null, Instant.now()),
            new MessageRecord(10L, "prices-v1", 0, "beyond",     null, null, Instant.now()),  // offset == toOffset, excluded
        ]

        when:
        service.handleModernBatchAck("client-boundary", "prices-v1", "group-boundary")

        then: "only the 2 in-batch records reach RocksDB; the beyond-boundary record is excluded"
        1 * ackStore.putBatch(
            { String[] t -> t.length == 2 },
            { String[] g -> g.length == 2 },
            { String[] k -> k as Set == ['in-batch-1', 'in-batch-2'] as Set },
            { AckRecord[] r -> r.length == 2 && r[0].offset == 7L && r[1].offset == 8L }
        )
    }

    // ── Legacy ACK path ───────────────────────────────────────────────────────

    def "legacy ACK path calls ackStore.putBatch directly from MergedBatch messages"() {
        given:
        def batch = new MergedBatch()
        def msg1 = new MessageRecord(10L, "prices-v1", 0, "prod-001", null, null, Instant.now())
        def msg2 = new MessageRecord(11L, "prices-v1", 0, "prod-002", null, null, Instant.now())
        batch.add("prices-v1", msg1)
        batch.add("prices-v1", msg2)

        pendingAckStore.getSendTime("client-legacy") >> 1000L
        pendingAckStore.removeTimer("client-legacy") >> null
        pendingAckStore.removePendingBatch("client-legacy") >> batch

        when:
        service.handleLegacyBatchAck("client-legacy", "price-quote-group")

        then:
        1 * legacyDeliveryManager.handleMergedBatchAck("price-quote-group", batch)
        1 * ackStore.putBatch(
                { String[] t -> t.length == 2 },
                { String[] g -> g[0] == "price-quote-group" && g[1] == "price-quote-group" },
                { String[] k -> k as Set == ["prod-001", "prod-002"] as Set },
                _
        )
    }

    def "legacy ACK path skips null msgKey without error"() {
        given:
        // Use a real message with a non-null key to add it to the batch,
        // then verify the null check in our ACK code is exercised by testing
        // that a batch with only null-key messages writes nothing to ackStore.
        def batch = Mock(MergedBatch)
        def msgNullKey = new MessageRecord(10L, "prices-v1", 0, null, null, null, Instant.now())
        batch.getMessages() >> [msgNullKey]
        batch.isEmpty() >> false
        batch.getMaxOffsetPerTopic() >> ["prices-v1": 10L]
        batch.getBytesPerTopic() >> ["prices-v1": 50L]
        batch.getMessageCountPerTopic() >> ["prices-v1": 1]
        batch.getMessageCount() >> 1
        batch.getTotalBytes() >> 50L

        pendingAckStore.getSendTime("client-legacy") >> 1000L
        pendingAckStore.removeTimer("client-legacy") >> null
        pendingAckStore.removePendingBatch("client-legacy") >> batch
        storage.getCurrentOffset("prices-v1", 0) >> 11L

        when:
        service.handleLegacyBatchAck("client-legacy", "price-quote-group")

        then:
        noExceptionThrown()
        0 * ackStore.putBatch(*_)
    }
}
