package com.messaging.broker.handler

import com.messaging.broker.handler.ParseException
import com.messaging.broker.model.SubscribeRequest
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class JsonSubscribeRequestParserSpec extends Specification {

    def parser = new JsonSubscribeRequestParser()

    def "should parse modern subscribe request"() {
        given:
        def json = '{"topic":"prices-v1","group":"price-quote-group"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        request.clientId() == "client-1"
        request.isModern()
        !request.isLegacy()
        request instanceof SubscribeRequest.Modern
        with(request as SubscribeRequest.Modern) {
            topic() == "prices-v1"
            group() == "price-quote-group"
        }
    }

    def "should parse modern request with whitespace"() {
        given:
        def json = '''
        {
            "topic": "prices-v1",
            "group": "price-quote-group"
        }
        '''
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        request.isModern()
        with(request as SubscribeRequest.Modern) {
            topic() == "prices-v1"
            group() == "price-quote-group"
        }
    }

    def "should reject missing topic field"() {
        given:
        def json = '{"group":"price-quote-group"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topic")
    }

    def "should reject missing group field"() {
        given:
        def json = '{"topic":"prices-v1"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("group")
    }

    def "should reject null topic value"() {
        given:
        def json = '{"topic":null,"group":"price-quote-group"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topic")
    }

    def "should reject null group value"() {
        given:
        def json = '{"topic":"prices-v1","group":null}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("group")
    }

    def "should reject empty topic string"() {
        given:
        def json = '{"topic":"","group":"price-quote-group"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topic")
    }

    def "should reject empty group string"() {
        given:
        def json = '{"topic":"prices-v1","group":""}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("group")
    }

    def "should reject malformed JSON"() {
        given:
        def json = '{"topic":"prices-v1","group":'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        thrown(ParseException)
    }

    def "should reject empty payload"() {
        given:
        def payload = new byte[0]

        when:
        parser.parse("client-1", payload)

        then:
        thrown(ParseException)
    }

    def "should handle special characters in topic and group"() {
        given:
        def json = '{"topic":"topic-with-dashes_underscores.dots","group":"group-123-test"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        with(request as SubscribeRequest.Modern) {
            topic() == "topic-with-dashes_underscores.dots"
            group() == "group-123-test"
        }
    }

    def "should handle real-world subscribe request"() {
        given:
        def json = '{"topic":"prices-v1","group":"price-quote-group"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-abc-123", payload)

        then:
        request.clientId() == "client-abc-123"
        request.isModern()
        with(request as SubscribeRequest.Modern) {
            topic() == "prices-v1"
            group() == "price-quote-group"
        }
    }

    def "should ignore extra fields in JSON"() {
        given:
        def json = '{"topic":"prices-v1","group":"price-quote-group","extra":"ignored","another":123}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        request.isModern()
        with(request as SubscribeRequest.Modern) {
            topic() == "prices-v1"
            group() == "price-quote-group"
        }
    }
}
