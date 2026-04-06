package com.messaging.broker.handler

import com.messaging.broker.handler.ParseException
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class JsonCommitOffsetRequestParserSpec extends Specification {

    def parser = new JsonCommitOffsetRequestParser()

    def "should parse commit offset request"() {
        given:
        def json = '{"offset":12345}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        request.clientId() == "client-1"
        request.topic() == "topic-1"
        request.group() == "group-1"
        request.offset() == 12345L
    }

    def "should parse offset zero"() {
        given:
        def json = '{"offset":0}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        request.offset() == 0L
    }

    def "should parse large offset"() {
        given:
        def json = '{"offset":9876543210}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        request.offset() == 9876543210L
    }

    def "should parse negative offset"() {
        given:
        def json = '{"offset":-1}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        request.offset() == -1L
    }

    def "should parse JSON with whitespace"() {
        given:
        def json = '''
        {
            "offset": 12345
        }
        '''
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        request.offset() == 12345L
    }

    def "should reject missing offset field"() {
        given:
        def json = '{}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("offset")
    }

    def "should reject null offset value"() {
        given:
        def json = '{"offset":null}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("offset")
    }

    def "should reject non-numeric offset"() {
        given:
        def json = '{"offset":"not-a-number"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("offset")
    }

    def "should reject malformed JSON"() {
        given:
        def json = '{"offset":'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        thrown(ParseException)
    }

    def "should reject empty payload"() {
        given:
        def payload = new byte[0]

        when:
        parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        thrown(ParseException)
    }

    def "should ignore extra fields in JSON"() {
        given:
        def json = '{"offset":12345,"extra":"ignored","another":123}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse("client-1", "topic-1", "group-1", payload)

        then:
        request.offset() == 12345L
    }

    def "should handle real-world commit request"() {
        given:
        def json = '{"offset":987654321}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def request = parser.parse(
            "client-abc-123",
            "prices-v1",
            "price-quote-group",
            payload
        )

        then:
        request.clientId() == "client-abc-123"
        request.topic() == "prices-v1"
        request.group() == "price-quote-group"
        request.offset() == 987654321L
    }
}
