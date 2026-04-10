package com.messaging.network.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

@MicronautTest(startApplication = false)
class ConsumerDecodeMetricsIntegrationSpec extends Specification {

    def "record increments the decode error counter"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record('my-topic', 'parse-error')

        then:
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'my-topic', 'reason', 'parse-error')
            .counter()
            .count() == 1.0d
    }

    def "record accumulates counts for the same topic and reason"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics = new ConsumerDecodeMetrics(registry)

        when:
        3.times { metrics.record('my-topic', 'parse-error') }

        then:
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'my-topic', 'reason', 'parse-error')
            .counter()
            .count() == 3.0d
    }

    def "different topics and reasons are tracked independently"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record('topic-a', 'bad-frame')
        metrics.record('topic-a', 'bad-frame')
        metrics.record('topic-b', 'bad-frame')
        metrics.record('topic-b', 'crc-mismatch')

        then:
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'topic-a', 'reason', 'bad-frame')
            .counter().count() == 2.0d
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'topic-b', 'reason', 'bad-frame')
            .counter().count() == 1.0d
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'topic-b', 'reason', 'crc-mismatch')
            .counter().count() == 1.0d
    }

    def "null topic and reason are normalised to unknown"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record(null, null)

        then:
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'unknown', 'reason', 'unknown')
            .counter().count() == 1.0d
    }

    def "counter lookup is cached per topic and reason"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics = new ConsumerDecodeMetrics(registry)

        when:
        50.times { metrics.record('cache-test', 'err') }

        then:
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'cache-test', 'reason', 'err')
            .counters().size() == 1
        registry.find('consumer_decode_errors_total')
            .tags('topic', 'cache-test', 'reason', 'err')
            .counter().count() == 50.0d
    }
}
