package com.messaging.broker.monitoring

import spock.lang.Specification

class TraceIdsSpec extends Specification {

    def "newTraceId returns a non-empty UUID-like string"() {
        when:
        def first = TraceIds.newTraceId()
        def second = TraceIds.newTraceId()

        then:
        first
        second
        first != second
        first ==~ /[0-9a-f\\-]{36}/
    }
}
