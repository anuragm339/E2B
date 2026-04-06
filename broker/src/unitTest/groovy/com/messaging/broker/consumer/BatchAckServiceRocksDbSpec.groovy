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
        stateService.getPendingOffset(deliveryKey) >> 5L          // nextOffset = 5
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
        stateService.getPendingOffset(deliveryKey) >> 5L
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
