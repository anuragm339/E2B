package com.messaging.common.model

import spock.lang.Specification

import java.time.Instant

class ConsumerRecordSpec extends Specification {

    def "equals ignores payload data and internal offset"() {
        given:
        def ts = Instant.parse('2024-01-01T00:00:00Z')
        def left = new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":1}', ts)
        def right = new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":2}', ts)

        when:
        left.setInternalOffset(10L)
        right.setInternalOffset(20L)

        then:
        left == right
        left.hashCode() == right.hashCode()
    }

    def "toString hides payload content"() {
        given:
        def record = new ConsumerRecord('key-1', EventType.MESSAGE, '{"sensitive":true}', Instant.parse('2024-01-01T00:00:00Z'))

        expect:
        record.toString().contains('data=...')
        !record.toString().contains('sensitive')
    }
}
