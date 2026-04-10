package com.messaging.common.model

import spock.lang.Specification

import java.time.Instant

class MessageRecordSpec extends Specification {

    def "setData infers DELETE when event type is not already set"() {
        given:
        def record = new MessageRecord()

        when:
        record.setData(null)

        then:
        record.eventType == EventType.DELETE
    }

    def "setOffset parses string values"() {
        given:
        def record = new MessageRecord()

        when:
        record.setOffset('42')

        then:
        record.offset == 42L
    }

    def "toString reports hasData instead of embedding full payload"() {
        given:
        def record = new MessageRecord(1L, 'prices-v1', 0, 'key-1', EventType.MESSAGE, '{"secret":true}', Instant.parse('2024-01-01T00:00:00Z'))

        expect:
        record.toString().contains('hasData=true')
        !record.toString().contains('secret')
    }
}
