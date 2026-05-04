package com.messaging.broker.legacy

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant

class MergedBatchSpec extends Specification {

    def "add tracks total bytes counts and max offset per topic"() {
        given:
        def batch = new MergedBatch()

        when:
        batch.add('prices-v1', record(10L, 'k1', '{"v":1}'))
        batch.add('prices-v1', record(12L, 'k2', '{"v":2}'))
        batch.add('orders-v1', record(20L, 'k3', null))

        then:
        batch.messageCount == 3
        batch.maxOffsetPerTopic == ['prices-v1': 12L, 'orders-v1': 20L]
        batch.messageCountPerTopic == ['prices-v1': 2, 'orders-v1': 1]
        batch.bytesPerTopic['prices-v1'] > 0
        batch.totalBytes >= batch.bytesPerTopic.values().sum()
        !batch.empty
    }

    def "add stamps topic onto MessageRecord even when record has no topic set"() {
        // Regression: storage.read() returns MessageRecord with topic=null because topic is a
        // partition-path concern, not stored in the record bytes.
        // BatchAckService.handleLegacyBatchAck() calls r.getTopic() to build the RocksDB key —
        // if topic is null the key becomes "null|group|msgKey" instead of "prices-v1|group|msgKey",
        // so the reconciler finds no ACK entry and reports INCONSISTENT for every legacy topic.
        given:
        def batch = new MergedBatch()
        def recordWithNullTopic = recordNoTopic(5L, 'msg-key-1', '{"price":42}')

        when:
        batch.add('prices-v1', recordWithNullTopic)

        then: "topic is stamped onto the MessageRecord by add()"
        batch.messages[0].topic == 'prices-v1'   // NOT null — RocksDB key will be correct

        and: "maxOffsetPerTopic also uses the correct topic"
        batch.maxOffsetPerTopic == ['prices-v1': 5L]
    }

    def "add with multiple topics stamps each record with its correct topic"() {
        given:
        def batch = new MergedBatch()
        def p1 = recordNoTopic(1L, 'k1', 'd1')
        def p2 = recordNoTopic(2L, 'k2', 'd2')
        def r1 = recordNoTopic(3L, 'k3', 'd3')

        when:
        batch.add('prices-v1', p1)
        batch.add('prices-v1', p2)
        batch.add('ref-data-v5', r1)

        then:
        p1.topic == 'prices-v1'
        p2.topic == 'prices-v1'
        r1.topic == 'ref-data-v5'
        // These are the exact topic strings that BatchAckService will use to build RocksDB keys
        batch.messages*.topic == ['prices-v1', 'prices-v1', 'ref-data-v5']
    }

    // Simulates what storage.read() returns — no topic field populated
    private static MessageRecord recordNoTopic(long offset, String key, String data) {
        def r = new MessageRecord()
        r.offset = offset
        r.msgKey = key
        r.data = data
        r.eventType = EventType.MESSAGE
        r.createdAt = Instant.parse('2024-01-01T00:00:00Z')
        // topic intentionally NOT set — mirrors storage.read() output
        return r
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'topic', 0, key, data == null ? EventType.DELETE : EventType.MESSAGE, data, Instant.parse('2024-01-01T00:00:00Z'))
    }
}
