package com.messaging.network.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

/**
 * Integration tests for network-layer metrics components.
 *
 * Tests:
 *   - ConsumerDecodeMetrics: decode error counters with topic + reason tags
 *   - DecodeErrorRecorder.noop(): the no-op factory method
 *
 * Uses Micrometer's in-memory SimpleMeterRegistry — no external monitoring infra required.
 */
@MicronautTest
class NetworkMetricsIntegrationSpec extends Specification {

    // =========================================================================
    // ConsumerDecodeMetrics
    // =========================================================================

    def "record() increments the consumer_decode_errors_total counter"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record("my-topic", "parse-error")

        then:
        registry.find("consumer_decode_errors_total")
                .tags("topic", "my-topic", "reason", "parse-error")
                .counter()
                .count() == 1.0d
    }

    def "record() called multiple times accumulates on the same counter"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        3.times { metrics.record("my-topic", "parse-error") }

        then:
        registry.find("consumer_decode_errors_total")
                .tags("topic", "my-topic", "reason", "parse-error")
                .counter()
                .count() == 3.0d
    }

    def "different topics are tracked with independent counters"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record("topic-a", "bad-frame")
        metrics.record("topic-a", "bad-frame")
        metrics.record("topic-b", "bad-frame")

        then:
        registry.find("consumer_decode_errors_total")
                .tags("topic", "topic-a", "reason", "bad-frame")
                .counter().count() == 2.0d

        registry.find("consumer_decode_errors_total")
                .tags("topic", "topic-b", "reason", "bad-frame")
                .counter().count() == 1.0d
    }

    def "different reasons on the same topic are tracked independently"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record("my-topic", "invalid-length")
        metrics.record("my-topic", "crc-mismatch")
        metrics.record("my-topic", "crc-mismatch")

        then:
        registry.find("consumer_decode_errors_total")
                .tags("topic", "my-topic", "reason", "invalid-length")
                .counter().count() == 1.0d

        registry.find("consumer_decode_errors_total")
                .tags("topic", "my-topic", "reason", "crc-mismatch")
                .counter().count() == 2.0d
    }

    def "null topic is normalised to 'unknown'"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record(null, "some-error")

        then:
        registry.find("consumer_decode_errors_total")
                .tags("topic", "unknown", "reason", "some-error")
                .counter().count() == 1.0d
    }

    def "null reason is normalised to 'unknown'"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        metrics.record("my-topic", null)

        then:
        registry.find("consumer_decode_errors_total")
                .tags("topic", "my-topic", "reason", "unknown")
                .counter().count() == 1.0d
    }

    def "counter lookup is cached: same topic+reason reuses the same counter instance"() {
        given:
        def registry = new SimpleMeterRegistry()
        def metrics  = new ConsumerDecodeMetrics(registry)

        when:
        50.times { metrics.record("cache-test", "err") }

        then:
        // Only one counter should exist for this tag combination
        registry.find("consumer_decode_errors_total")
                .tags("topic", "cache-test", "reason", "err")
                .counters().size() == 1

        registry.find("consumer_decode_errors_total")
                .tags("topic", "cache-test", "reason", "err")
                .counter().count() == 50.0d
    }

    // =========================================================================
    // DecodeErrorRecorder.noop()
    // =========================================================================

    def "DecodeErrorRecorder.noop() returns a non-null recorder"() {
        expect:
        DecodeErrorRecorder.noop() != null
    }

    def "DecodeErrorRecorder.noop() record() does not throw for any input"() {
        given:
        def noop = DecodeErrorRecorder.noop()

        expect:
        noop.record("any-topic", "any-reason")
        noop.record(null, null)
        noop.record("", "")
    }
}
