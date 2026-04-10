package com.messaging.common.model

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

@MicronautTest(startApplication = false)
class BrokerMessageIntegrationSpec extends Specification {

    def "broker message type codes round-trip"() {
        expect:
        BrokerMessage.MessageType.fromCode(type.code) == type

        where:
        type << BrokerMessage.MessageType.values()
    }
}
