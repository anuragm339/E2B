package com.messaging.common.api

import com.messaging.common.model.ConsumerRecord
import com.messaging.common.model.EventType
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

import java.time.Instant

@MicronautTest(startApplication = false)
class NoOpErrorHandlerIntegrationSpec extends Specification {

    @Inject NoOpErrorHandler handler

    def "NoOpErrorHandler bean is available and handles an error without throwing"() {
        given:
        def record = new ConsumerRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.now())

        when:
        handler.onError(record, new RuntimeException('failed to process'))

        then:
        handler != null
        noExceptionThrown()
    }
}
