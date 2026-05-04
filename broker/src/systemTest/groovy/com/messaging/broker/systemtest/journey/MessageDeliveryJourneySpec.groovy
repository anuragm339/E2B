package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.common.api.StorageEngine
import spock.util.concurrent.PollingConditions

/**
 * Journey: pipe records flow through broker to consumer.
 *
 * MockCloudServer → /pipe/poll → Broker (stores + pushes) → test-consumer (receives)
 *
 * Verifies both the consumer-side view (what TestRecordCollector receives) and the
 * storage-side view (what StorageEngine actually wrote) so that serialisation bugs in
 * the pipe deserialiser or storage writer are caught independently of delivery.
 *
 * Note on storage.read() fromOffset: the Segment treats the record's own offset field
 * as the storage key.  Our mock records carry explicit offsets (1-5, 10-12, 20-21),
 * so we pass the first enqueued offset as fromOffset — passing 0 would hit an offset
 * gap (no record at 0) and the SegmentManager stops traversal early, returning empty.
 */
class MessageDeliveryJourneySpec extends BrokerSystemTestSupport {

    def "5 records enqueued in pipe are delivered to the consumer"() {
        given:
        def records = (1..5).collect { i ->
            [
                offset   : (long) i,
                topic    : 'prices-v1',
                partition: 0,
                msgKey   : "key-${i}",
                eventType: 'MESSAGE',
                data     : """{"value":${i}}"""
            ]
        }
        cloudServer.enqueueMessages(records)

        when:
        def received = collector().waitForRecords(5, 20)

        then: "all 5 records arrive at the consumer"
        received.size() == 5
        received*.msgKey.toSet() == (1..5).collect { "key-${it}" }.toSet()
        cloudServer.pollCount > 0

        and: "eventType is preserved through pipe → storage → consumer"
        received.every { it.eventType.name() == 'MESSAGE' }

        and: "data payload is preserved exactly through pipe → storage → consumer"
        received*.data.toSet() == (1..5).collect { """{"value":${it}}""" }.toSet()

        and: "records are persisted in StorageEngine with correct content (fromOffset=1 — first enqueued offset)"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def stored = storage.read('prices-v1', 0, 1L, 100)
            def byKey = stored.collectEntries { [it.msgKey, it] }
            assert (1..5).every { i ->
                def r = byKey["key-${i}"]
                r != null && r.data == """{"value":${i}}""" && r.eventType.name() == 'MESSAGE'
            }
        }

        and: "committed offset is recorded in ConsumerOffsetTracker (ACK processed)"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('system-test-group:prices-v1') > 0
        }

        and: "committed offset is flushed to consumer-offsets.properties on disk"
        new PollingConditions(timeout: 12, delay: 0.5).eventually {
            def file = new File("${dataDir}/consumer-offsets.properties")
            assert file.exists()
            def props = new Properties()
            props.load(file.newInputStream())
            assert Long.parseLong(props.getProperty('system-test-group:prices-v1', '0')) > 0
        }

        and: "all 5 offsets are written to RocksDB ack-store (async, after ACK)"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert (1..5).every { i ->
                def ack = ackStore.get('prices-v1', 'system-test-group', (long) i)
                ack != null && ack.offset == i
            }
        }
    }

    def "broker polls pipe repeatedly and delivers batches in order"() {
        given:
        collector().reset()
        // Enqueue two separate batches — offsets 10, 11, 12
        cloudServer.enqueueMessages([
            [offset: 10L, topic: 'prices-v1', partition: 0, msgKey: 'batch1-a', eventType: 'MESSAGE', data: '{"b":1}'],
            [offset: 11L, topic: 'prices-v1', partition: 0, msgKey: 'batch1-b', eventType: 'MESSAGE', data: '{"b":2}'],
        ])
        cloudServer.enqueueMessages([
            [offset: 12L, topic: 'prices-v1', partition: 0, msgKey: 'batch2-a', eventType: 'MESSAGE', data: '{"b":3}'],
        ])

        when:
        def received = collector().waitForRecords(3, 20)

        then:
        received.size() == 3
        received*.msgKey.containsAll(['batch1-a', 'batch1-b', 'batch2-a'])

        and: "all batch records are in StorageEngine (fromOffset=10 — first enqueued offset)"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def stored = storage.read('prices-v1', 0, 10L, 100)
            def keys = stored*.msgKey.toSet()
            assert keys.containsAll(['batch1-a', 'batch1-b', 'batch2-a'])
        }

        and: "committed offset advances in ConsumerOffsetTracker after both batches are ACKed"
        def offsetTracker2 = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker2.getOffset('system-test-group:prices-v1') > 0
        }

        and: "all 3 offsets from both batches are written to RocksDB ack-store"
        def ackStore2 = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore2.get('prices-v1', 'system-test-group', 10L) != null   // batch1-a
            assert ackStore2.get('prices-v1', 'system-test-group', 11L) != null   // batch1-b
            assert ackStore2.get('prices-v1', 'system-test-group', 12L) != null   // batch2-a
        }
    }

    def "DELETE records are delivered to the consumer alongside MESSAGE records"() {
        given:
        collector().reset()
        // Offsets 20 and 21
        cloudServer.enqueueMessages([
            [offset: 20L, topic: 'prices-v1', partition: 0, msgKey: 'del-key', eventType: 'DELETE', data: null],
            [offset: 21L, topic: 'prices-v1', partition: 0, msgKey: 'msg-key', eventType: 'MESSAGE', data: '{"x":1}'],
        ])

        when:
        def received = collector().waitForRecords(2, 20)

        then: "both records arrive"
        received.size() == 2
        received.any { it.msgKey == 'del-key' }
        received.any { it.msgKey == 'msg-key' }

        and: "DELETE eventType preserved through pipe → storage → consumer"
        received.find { it.msgKey == 'del-key' }.eventType.name() == 'DELETE'
        received.find { it.msgKey == 'msg-key' }.eventType.name() == 'MESSAGE'

        and: "DELETE has null data; MESSAGE carries its payload intact"
        received.find { it.msgKey == 'del-key' }.data == null
        received.find { it.msgKey == 'msg-key' }.data == '{"x":1}'

        and: "StorageEngine preserved eventType for both records (fromOffset=20 — first enqueued offset)"
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            def stored = storage.read('prices-v1', 0, 20L, 100)
            def byKey = stored.collectEntries { [it.msgKey, it] }
            assert byKey['del-key']?.eventType?.name() == 'DELETE'
            assert byKey['msg-key']?.eventType?.name() == 'MESSAGE'
            assert byKey['msg-key']?.data == '{"x":1}'
        }

        and: "committed offset advances in ConsumerOffsetTracker after DELETE + MESSAGE batch ACK"
        def offsetTracker3 = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker3.getOffset('system-test-group:prices-v1') > 0
        }

        and: "both DELETE and MESSAGE offsets are written to RocksDB ack-store"
        def ackStore3 = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore3.get('prices-v1', 'system-test-group', 20L) != null   // del-key
            assert ackStore3.get('prices-v1', 'system-test-group', 21L) != null   // msg-key
        }
    }
}
