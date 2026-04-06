package com.messaging.network.legacy

import com.messaging.network.legacy.ProtocolDetectionService
import com.messaging.network.legacy.ProtocolDetectionService.ProtocolType
import spock.lang.Specification

class DefaultProtocolDetectionServiceSpec extends Specification {

    ProtocolDetectionService service

    def setup() {
        service = new DefaultProtocolDetectionService()
    }

    def "should detect legacy protocol for REGISTER (0x00)"() {
        when:
        def result = service.detectProtocol((byte) 0x00)

        then:
        result == ProtocolType.LEGACY
    }

    def "should detect modern protocol for DATA (0x01)"() {
        when:
        def result = service.detectProtocol((byte) 0x01)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for ACK (0x02)"() {
        when:
        def result = service.detectProtocol((byte) 0x02)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for SUBSCRIBE (0x03)"() {
        when:
        def result = service.detectProtocol((byte) 0x03)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for COMMIT_OFFSET (0x04)"() {
        when:
        def result = service.detectProtocol((byte) 0x04)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for RESET (0x05)"() {
        when:
        def result = service.detectProtocol((byte) 0x05)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for READY (0x06)"() {
        when:
        def result = service.detectProtocol((byte) 0x06)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for DISCONNECT (0x07)"() {
        when:
        def result = service.detectProtocol((byte) 0x07)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for HEARTBEAT (0x08)"() {
        when:
        def result = service.detectProtocol((byte) 0x08)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for BATCH_HEADER (0x09)"() {
        when:
        def result = service.detectProtocol((byte) 0x09)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for BATCH_ACK (0x0A)"() {
        when:
        def result = service.detectProtocol((byte) 0x0A)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for RESET_ACK (0x0B)"() {
        when:
        def result = service.detectProtocol((byte) 0x0B)

        then:
        result == ProtocolType.MODERN
    }

    def "should detect modern protocol for READY_ACK (0x0C)"() {
        when:
        def result = service.detectProtocol((byte) 0x0C)

        then:
        result == ProtocolType.MODERN
    }

    def "should default to modern for unknown byte"() {
        when:
        def result = service.detectProtocol((byte) 0xFF)

        then:
        result == ProtocolType.MODERN
    }

    def "should default to modern for negative byte"() {
        when:
        def result = service.detectProtocol((byte) -1)

        then:
        result == ProtocolType.MODERN
    }
}
