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

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'topic', 0, key, data == null ? EventType.DELETE : EventType.MESSAGE, data, Instant.parse('2024-01-01T00:00:00Z'))
    }
}
