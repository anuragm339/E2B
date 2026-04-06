package com.messaging.broker.handler

import com.messaging.broker.handler.ParseException
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.util.Base64

class JsonDataMessageRequestParserSpec extends Specification {

    def parser = new JsonDataMessageRequestParser()

    def "should parse data message request"() {
        given:
        def messagePayload = "test message content".bytes
        def encodedPayload = Base64.encoder.encodeToString(messagePayload)
        def json = """{"topic":"prices-v1","payload":"${encodedPayload}"}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse(payload)

        then:
        request.topic() == "prices-v1"
        request.payload() == messagePayload
        request.partition() == 0
    }

    def "should parse with explicit partition"() {
        given:
        def messagePayload = "test".bytes
        def encodedPayload = Base64.encoder.encodeToString(messagePayload)
        def json = """{"topic":"prices-v1","payload":"${encodedPayload}","partition":5}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse(payload)

        then:
        request.topic() == "prices-v1"
        request.partition() == 5
    }

    def "should parse JSON payload"() {
        given:
        def jsonMessage = '{"event":"price_update","product_id":"12345"}'
        def encodedPayload = Base64.encoder.encodeToString(jsonMessage.bytes)
        def json = """{"topic":"prices-v1","payload":"${encodedPayload}"}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse(payload)

        then:
        new String(request.payload()) == jsonMessage
    }

    def "should handle empty payload"() {
        given:
        def encodedPayload = Base64.encoder.encodeToString(new byte[0])
        def json = """{"topic":"prices-v1","payload":"${encodedPayload}"}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse(payload)

        then:
        request.payload().length == 0
    }

    def "should reject missing topic"() {
        given:
        def encodedPayload = Base64.encoder.encodeToString("test".bytes)
        def json = """{"payload":"${encodedPayload}"}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topic")
    }

    def "should reject null topic"() {
        given:
        def encodedPayload = Base64.encoder.encodeToString("test".bytes)
        def json = """{"topic":null,"payload":"${encodedPayload}"}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topic")
    }

    def "should reject missing payload"() {
        given:
        def json = '{"topic":"prices-v1"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("payload")
    }

    def "should reject null payload"() {
        given:
        def json = '{"topic":"prices-v1","payload":null}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("payload")
    }

    def "should reject invalid base64 payload"() {
        given:
        def json = '{"topic":"prices-v1","payload":"not-valid-base64!!!"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        thrown(ParseException)
    }

    def "should reject malformed JSON"() {
        given:
        def json = '{"topic":"prices-v1","payload":'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        thrown(ParseException)
    }

    def "should reject empty JSON payload"() {
        given:
        def payload = new byte[0]

        when:
        parser.parse(payload)

        then:
        thrown(ParseException)
    }

    def "should ignore extra fields"() {
        given:
        def encodedPayload = Base64.encoder.encodeToString("test".bytes)
        def json = """{"topic":"prices-v1","payload":"${encodedPayload}","extra":"ignored"}"""
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse(payload)

        then:
        request.topic() == "prices-v1"
    }
}
