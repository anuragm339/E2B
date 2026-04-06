package com.messaging.pipe

import com.messaging.common.api.StorageEngine
import com.messaging.pipe.metrics.PipeMetrics
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

/**
 * Integration tests for PipeMetrics — Prometheus metric registration and recording.
 *
 * Uses @MicronautTest so PipeMetrics is the real singleton bean wired into the
 * Micronaut-managed MeterRegistry (same registry used in production).
 * rebuildContext = true ensures each test starts with fresh counters/timers.
 */
@MicronautTest(rebuildContext = true)
class PipeMetricsIntegrationSpec extends Specification {

    @Inject PipeMetrics   metrics
    @Inject MeterRegistry registry

    // StorageEngine is not used by PipeMetrics, but the pipe module depends on
    // :storage which registers a StorageEngine bean — mock it to avoid needing
    // a real data directory.
    @MockBean(StorageEngine)
    StorageEngine mockStorage() { Mock(StorageEngine) }

    // =========================================================================
    // Metric registration
    // =========================================================================

    def "pipe_fetch_latency_seconds timer is registered"() {
        expect:
        registry.find('pipe_fetch_latency_seconds').timer() != null
    }

    def "pipe_messages_received_total counter is registered"() {
        expect:
        registry.find('pipe_messages_received_total').counter() != null
    }

    def "pipe_fetch_errors_total counter is registered"() {
        expect:
        registry.find('pipe_fetch_errors_total').counter() != null
    }

    def "pipe_fetch_empty_total counter is registered"() {
        expect:
        registry.find('pipe_fetch_empty_total').counter() != null
    }

    // =========================================================================
    // Counter increments
    // =========================================================================

    def "recordMessagesReceived increments counter by given count"() {
        when:
        metrics.recordMessagesReceived(5)

        then:
        registry.find('pipe_messages_received_total').counter().count() == 5.0d
    }

    def "recordMessagesReceived is additive across calls"() {
        when:
        metrics.recordMessagesReceived(3)
        metrics.recordMessagesReceived(7)

        then:
        registry.find('pipe_messages_received_total').counter().count() == 10.0d
    }

    def "recordFetchError increments error counter by one"() {
        when:
        metrics.recordFetchError()

        then:
        registry.find('pipe_fetch_errors_total').counter().count() == 1.0d
    }

    def "recordFetchError is cumulative"() {
        when:
        3.times { metrics.recordFetchError() }

        then:
        registry.find('pipe_fetch_errors_total').counter().count() == 3.0d
    }

    def "recordEmptyFetch increments empty counter by one"() {
        when:
        metrics.recordEmptyFetch()

        then:
        registry.find('pipe_fetch_empty_total').counter().count() == 1.0d
    }

    def "recordEmptyFetch is cumulative"() {
        when:
        4.times { metrics.recordEmptyFetch() }

        then:
        registry.find('pipe_fetch_empty_total').counter().count() == 4.0d
    }

    def "independent counters do not interfere with each other"() {
        when:
        metrics.recordMessagesReceived(10)
        metrics.recordFetchError()
        metrics.recordEmptyFetch()
        metrics.recordEmptyFetch()

        then:
        registry.find('pipe_messages_received_total').counter().count() == 10.0d
        registry.find('pipe_fetch_errors_total').counter().count() == 1.0d
        registry.find('pipe_fetch_empty_total').counter().count() == 2.0d
    }

    // =========================================================================
    // Timer
    // =========================================================================

    def "startFetchTimer returns a non-null sample"() {
        when:
        def sample = metrics.startFetchTimer()

        then:
        sample != null
    }

    def "recordFetchLatency stops sample and records to timer"() {
        given:
        def sample = metrics.startFetchTimer()

        when:
        metrics.recordFetchLatency(sample)

        then: "timer has one recorded observation"
        registry.find('pipe_fetch_latency_seconds').timer().count() == 1L
    }

    def "multiple latency recordings accumulate in timer"() {
        when:
        3.times {
            def s = metrics.startFetchTimer()
            metrics.recordFetchLatency(s)
        }

        then:
        registry.find('pipe_fetch_latency_seconds').timer().count() == 3L
    }
}
