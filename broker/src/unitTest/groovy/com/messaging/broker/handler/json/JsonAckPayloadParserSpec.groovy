package com.messaging.broker.handler

import com.messaging.broker.handler.ParseException
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class JsonAckPayloadParserSpec extends Specification {

    def parser = new JsonAckPayloadParser()

    def "should parse successful ack"() {
        given:
        def json = '{"offset":12345,"success":true}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 12345L
        ack.success()
        ack.isSuccess()
        !ack.isFailure()
        !ack.hasErrorMessage()
    }

    def "should parse failed ack with error message"() {
        given:
        def json = '{"offset":12345,"success":false,"errorMessage":"Processing failed"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 12345L
        !ack.success()
        ack.isFailure()
        !ack.isSuccess()
        ack.hasErrorMessage()
        ack.errorMessage() == "Processing failed"
    }

    def "should parse failed ack without error message"() {
        given:
        def json = '{"offset":12345,"success":false}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 12345L
        ack.isFailure()
        !ack.hasErrorMessage()
        ack.errorMessage() == null
    }

    def "should parse ack with zero offset"() {
        given:
        def json = '{"offset":0,"success":true}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 0L
        ack.isSuccess()
    }

    def "should parse ack with large offset"() {
        given:
        def json = '{"offset":9876543210,"success":true}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 9876543210L
    }

    def "should reject missing offset field"() {
        given:
        def json = '{"success":true}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("offset")
    }

    def "should reject null offset"() {
        given:
        def json = '{"offset":null,"success":true}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("offset")
    }

    def "should reject missing success field"() {
        given:
        def json = '{"offset":12345}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("success")
    }

    def "should reject null success"() {
        given:
        def json = '{"offset":12345,"success":null}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("success")
    }

    def "should reject non-boolean success"() {
        given:
        def json = '{"offset":12345,"success":"yes"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        def e = thrown(ParseException)
        e.message.contains("success")
    }

    def "should reject malformed JSON"() {
        given:
        def json = '{"offset":12345,"success":'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        parser.parse(payload)

        then:
        thrown(ParseException)
    }

    def "should reject empty payload"() {
        given:
        def payload = new byte[0]

        when:
        parser.parse(payload)

        then:
        thrown(ParseException)
    }

    def "should handle whitespace in JSON"() {
        given:
        def json = '''
        {
            "offset": 12345,
            "success": true
        }
        '''
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 12345L
        ack.isSuccess()
    }

    def "should ignore extra fields"() {
        given:
        def json = '{"offset":12345,"success":true,"extra":"ignored","another":123}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.offset() == 12345L
        ack.isSuccess()
    }

    def "should handle detailed error message"() {
        given:
        def json = '{"offset":12345,"success":false,"errorMessage":"Database connection timeout after 30s attempting to save records"}'
        def payload = json.getBytes(StandardCharsets.UTF_8)

        when:
        def ack = parser.parse(payload)

        then:
        ack.isFailure()
        ack.errorMessage().contains("Database")
        ack.errorMessage().contains("30s")
    }
}
