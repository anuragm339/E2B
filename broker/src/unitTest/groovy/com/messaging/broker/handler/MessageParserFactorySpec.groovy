package com.messaging.broker.handler

import com.messaging.broker.handler.*
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class MessageParserFactorySpec extends Specification {

    def modernParser = new JsonSubscribeRequestParser()
    def legacyParser = new LegacySubscribeRequestParser()
    def commitOffsetParser = new JsonCommitOffsetRequestParser()
    def dataMessageParser = new JsonDataMessageRequestParser()
    def ackPayloadParser = new JsonAckPayloadParser()

    def factory = new MessageParserFactory(
        modernParser,
        legacyParser,
        commitOffsetParser,
        dataMessageParser,
        ackPayloadParser
    )

    def "should detect modern subscribe request"() {
        given:
        def modernJson = '{"topic":"prices-v1","group":"price-quote-group"}'
        def payload = modernJson.getBytes(StandardCharsets.UTF_8)

        when:
        def parser = factory.getSubscribeParser(payload)

        then:
        parser.is(modernParser)
    }

    def "should detect legacy subscribe request"() {
        given:
        def legacyJson = '{"isLegacy":true,"serviceName":"price-quote","topics":["prices-v1"]}'
        def payload = legacyJson.getBytes(StandardCharsets.UTF_8)

        when:
        def parser = factory.getSubscribeParser(payload)

        then:
        parser.is(legacyParser)
    }

    def "should detect legacy with whitespace"() {
        given:
        def legacyJson = '{"isLegacy": true, "serviceName":"price-quote","topics":["prices-v1"]}'
        def payload = legacyJson.getBytes(StandardCharsets.UTF_8)

        when:
        def parser = factory.getSubscribeParser(payload)

        then:
        parser.is(legacyParser)
    }

    def "should default to modern for ambiguous payload"() {
        given:
        def ambiguousJson = '{}'
        def payload = ambiguousJson.getBytes(StandardCharsets.UTF_8)

        when:
        def parser = factory.getSubscribeParser(payload)

        then:
        parser.is(modernParser)
    }

    def "should default to modern for malformed payload"() {
        given:
        def malformedJson = 'not json at all'
        def payload = malformedJson.getBytes(StandardCharsets.UTF_8)

        when:
        def parser = factory.getSubscribeParser(payload)

        then:
        parser.is(modernParser)
    }

    def "should return modern parser directly"() {
        when:
        def parser = factory.getModernSubscribeParser()

        then:
        parser.is(modernParser)
    }

    def "should return legacy parser directly"() {
        when:
        def parser = factory.getLegacySubscribeParser()

        then:
        parser.is(legacyParser)
    }

    def "should return commit offset parser"() {
        when:
        def parser = factory.getCommitOffsetParser()

        then:
        parser.is(commitOffsetParser)
    }

    def "should return data message parser"() {
        when:
        def parser = factory.getDataMessageParser()

        then:
        parser.is(dataMessageParser)
    }

    def "should return ack payload parser"() {
        when:
        def parser = factory.getAckPayloadParser()

        then:
        parser.is(ackPayloadParser)
    }

    def "should handle null payload gracefully"() {
        when:
        def parser = factory.getSubscribeParser(null)

        then:
        parser.is(modernParser)  // Defaults to modern
    }

    def "should handle empty payload gracefully"() {
        when:
        def parser = factory.getSubscribeParser(new byte[0])

        then:
        parser.is(modernParser)  // Defaults to modern
    }
}
