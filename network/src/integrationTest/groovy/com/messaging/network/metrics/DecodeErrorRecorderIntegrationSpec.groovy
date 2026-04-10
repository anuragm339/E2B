package com.messaging.network.metrics

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

@MicronautTest(startApplication = false)
class DecodeErrorRecorderIntegrationSpec extends Specification {

    def "noop recorder returns a usable recorder instance"() {
        expect:
        DecodeErrorRecorder.noop() != null
    }

    def "noop recorder ignores all record calls"() {
        given:
        def noop = DecodeErrorRecorder.noop()

        expect:
        noop.record('any-topic', 'any-reason')
        noop.record(null, null)
        noop.record('', '')
    }
}
