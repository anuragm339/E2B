package com.messaging.common.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

import java.time.Instant

@MicronautTest(startApplication = false)
class MessageRecordIntegrationSpec extends Specification {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule())

    def "message record deserializes API payload and infers DELETE when data is null"() {
        given:
        def json = '''
        {
          "offset": "42",
          "topic": "prices-v1",
          "partition": 0,
          "msgKey": "key-1",
          "data": null,
          "createdAt": "2024-01-01T00:00:00Z",
          "contentType": "application/json"
        }
        '''

        when:
        MessageRecord record = objectMapper.readValue(json, MessageRecord)

        then:
        record.offset == 42L
        record.topic == 'prices-v1'
        record.partition == 0
        record.msgKey == 'key-1'
        record.eventType == EventType.DELETE
        record.createdAt == Instant.parse('2024-01-01T00:00:00Z')
        record.contentType == 'application/json'
    }
}
