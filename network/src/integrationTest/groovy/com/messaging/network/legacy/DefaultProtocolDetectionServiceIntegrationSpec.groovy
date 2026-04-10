package com.messaging.network.legacy

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

@MicronautTest(startApplication = false)
class DefaultProtocolDetectionServiceIntegrationSpec extends Specification {

    @Inject DefaultProtocolDetectionService detectionService

    def "protocol detection classifies legacy modern and unknown first bytes"() {
        expect:
        detectionService.detectProtocol(firstByte) == expected

        where:
        firstByte      || expected
        (byte) 0x00    || ProtocolDetectionService.ProtocolType.LEGACY
        (byte) 0x01    || ProtocolDetectionService.ProtocolType.MODERN
        (byte) 0x03    || ProtocolDetectionService.ProtocolType.MODERN
        (byte) 0x7F    || ProtocolDetectionService.ProtocolType.MODERN
    }
}
