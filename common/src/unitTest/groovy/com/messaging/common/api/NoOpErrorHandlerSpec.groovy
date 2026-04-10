package com.messaging.common.api

import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import spock.lang.Specification

import java.time.Instant

class NoOpErrorHandlerSpec extends Specification {

    def "onError does not throw for a normal record"() {
        given:
        def handler = new NoOpErrorHandler()
        def record = new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.now())

        when:
        handler.onError(record, new IllegalStateException('boom'))

        then:
        noExceptionThrown()
    }
}
