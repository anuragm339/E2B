package com.messaging.common.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

import java.time.Instant

@MicronautTest(startApplication = false)
class ConsumerRecordIntegrationSpec extends Specification {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule())

    def "consumer record serializes and deserializes through Jackson"() {
        given:
        def original = new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.parse('2024-01-01T00:00:00Z'))

        when:
        def json = objectMapper.writeValueAsString(original)
        def restored = objectMapper.readValue(json, ConsumerRecord)

        then:
        restored.msgKey == 'key-1'
        restored.eventType == EventType.MESSAGE
        restored.data == '{"v":1}'
        restored.createdAt == Instant.parse('2024-01-01T00:00:00Z')
    }
}
