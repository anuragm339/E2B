package com.messaging.broker.handler

import com.messaging.broker.handler.ParseException
import com.messaging.broker.model.SubscribeRequest
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class LegacySubscribeRequestParserSpec extends Specification {

    def parser = new LegacySubscribeRequestParser()

    def "should parse legacy subscribe request"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":["prices-v1","reference-data-v5"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        request.clientId() == "client-1"
        !request.isModern()
        request.isLegacy()
        request instanceof SubscribeRequest.Legacy
        with(request as SubscribeRequest.Legacy) {
            serviceName() == "price-quote"
            topics().size() == 2
            topics() == ["prices-v1", "reference-data-v5"]
        }
    }

    def "should parse legacy request with single topic"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"service-1","topics":["topic-1"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        with(request as SubscribeRequest.Legacy) {
            serviceName() == "service-1"
            topics().size() == 1
            topics()[0] == "topic-1"
        }
    }

    def "should parse legacy request with multiple topics"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":["prices-v1","reference-data-v5","non-promotable-products","prices-v4","minimum-price","deposit"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        with(request as SubscribeRequest.Legacy) {
            serviceName() == "price-quote"
            topics().size() == 6
            topics().contains("prices-v1")
            topics().contains("deposit")
        }
    }

    def "should reject missing isLegacy field"() {
        given:
        def json = '{"serviceName":"price-quote","topics":["topic-1"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("legacy")
    }

    def "should reject isLegacy=false"() {
        given:
        def json = '{"isLegacy":false,"serviceName":"price-quote","topics":["topic-1"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("legacy")
    }

    def "should reject missing serviceName field"() {
        given:
        def json = '{"isLegacy":true,"topics":["topic-1"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("serviceName")
    }

    def "should reject null serviceName"() {
        given:
        def json = '{"isLegacy":true,"serviceName":null,"topics":["topic-1"]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("serviceName")
    }

    def "should reject missing topics field"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topics")
    }

    def "should reject null topics array"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":null}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topics")
    }

    def "should reject empty topics array"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":[]}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topics")
        e.message.contains("empty")
    }

    def "should reject non-array topics field"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":"not-an-array"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("topics")
    }

    def "should reject malformed JSON"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":'
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

    def "should handle whitespace in JSON"() {
        given:
        def json = '''
        {
            "isLegacy": true,
            "serviceName": "price-quote",
            "topics": [
                "prices-v1",
                "reference-data-v5"
            ]
        }
        '''
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        with(request as SubscribeRequest.Legacy) {
            serviceName() == "price-quote"
            topics().size() == 2
        }
    }

    def "should ignore extra fields in JSON"() {
        given:
        def json = '{"isLegacy":true,"serviceName":"price-quote","topics":["topic-1"],"extra":"ignored","another":123}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", payload)

        then:
        request.isLegacy()
        with(request as SubscribeRequest.Legacy) {
            serviceName() == "price-quote"
            topics().size() == 1
        }
    }
}
