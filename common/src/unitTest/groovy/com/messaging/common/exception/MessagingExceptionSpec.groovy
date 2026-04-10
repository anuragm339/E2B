package com.messaging.common.exception

import spock.lang.Specification

class MessagingExceptionSpec extends Specification {

    def "structured message includes code category retriable and context"() {
        given:
        def ex = new MessagingException(ErrorCode.STORAGE_IO_ERROR, 'disk failed')
            .withContext('topic', 'prices-v1')
            .withContext('partition', 0)

        when:
        def message = ex.getStructuredMessage()

        then:
        ex.retriable
        ex.category == 'STORAGE'
        ex.code == 1001
        message.contains('[STORAGE_IO_ERROR]')
        message.contains('category=STORAGE')
        message.contains('topic=prices-v1')
        message.contains('partition=0')
    }

    def "getContext returns a defensive copy"() {
        given:
        def ex = new MessagingException(ErrorCode.NETWORK_TIMEOUT, 'timeout')
        def context = ex.withContext('remoteAddress', '127.0.0.1').getContext()

        when:
        context['remotePort'] = 8080

        then:
        !ex.getContext().containsKey('remotePort')
        ex.getContext().remoteAddress == '127.0.0.1'
    }
}
