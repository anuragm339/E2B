package com.messaging.common.exception

import spock.lang.Specification

class NetworkExceptionSpec extends Specification {

    def "connectionFailed adds remote address context"() {
        given:
        def cause = new RuntimeException('connect failed')

        when:
        def ex = NetworkException.connectionFailed('10.0.0.5', 19092, cause)

        then:
        ex.errorCode == ErrorCode.NETWORK_CONNECTION_FAILED
        ex.context.remoteAddress == '10.0.0.5'
        ex.context.remotePort == 19092
        ex.cause.is(cause)
    }

    def "encodingError records the message type in context"() {
        when:
        def ex = NetworkException.encodingError('BATCH_HEADER', new IllegalArgumentException('bad frame'))

        then:
        ex.errorCode == ErrorCode.NETWORK_ENCODING_ERROR
        ex.context.messageType == 'BATCH_HEADER'
    }
}
