package com.messaging.broker.handler

import spock.lang.Specification

class ValidationWarningSpec extends Specification {

    def "factory without context uses an empty immutable map"() {
        when:
        def warning = ValidationWarning.of(ValidationWarningCode.AUTO_CORRECTED, 'adjusted offset')

        then:
        warning.code() == ValidationWarningCode.AUTO_CORRECTED
        warning.context().isEmpty()
        when:
        warning.context().put('x', 1)
        then:
        thrown(UnsupportedOperationException)
    }

    def "constructor makes a defensive copy of context"() {
        given:
        def input = [offset: 5L]

        when:
        def warning = ValidationWarning.of(ValidationWarningCode.OFFSET_CLAMPED_TO_ZERO, 'clamped', input)
        input.offset = 10L

        then:
        warning.context().offset == 5L
    }
}
