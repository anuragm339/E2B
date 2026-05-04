package com.messaging.broker.handler

import spock.lang.Specification

class ParseExceptionSpec extends Specification {

    def "constructor stores message and cause"() {
        given:
        def cause = new IllegalArgumentException('bad payload')

        when:
        def ex = new ParseException('parse failed', cause)

        then:
        ex.message == 'parse failed'
        ex.cause.is(cause)
    }
}
