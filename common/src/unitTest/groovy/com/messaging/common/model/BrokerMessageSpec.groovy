package com.messaging.common.model

import com.messaging.common.exception.NetworkException
import spock.lang.Specification

class BrokerMessageSpec extends Specification {

    def "fromCode returns the matching message type"() {
        expect:
        BrokerMessage.MessageType.fromCode((byte) 0x0C) == BrokerMessage.MessageType.READY_ACK
    }

    def "fromCode throws NetworkException with context for unknown code"() {
        when:
        BrokerMessage.MessageType.fromCode((byte) 0x55)

        then:
        def ex = thrown(NetworkException)
        ex.context.messageTypeCode == (byte) 0x55
    }
}
