package com.messaging.broker.monitoring

import com.messaging.broker.consumer.RefreshContext
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

import java.time.Instant

class DataRefreshMetricsSpec extends Specification {

    SimpleMeterRegistry registry
    DataRefreshMetrics metrics

    def setup() {
        registry = new SimpleMeterRegistry()
        metrics = new DataRefreshMetrics(registry)
    }

    def cleanup() {
        registry.close()
    }

    def "records refresh lifecycle timing and completion metrics"() {
        given:
        def context = new RefreshContext("prices-v1", ["group-a:prices-v1"] as Set)
        context.setRefreshId("refresh-1")
        context.addDowntimePeriod(Instant.now().minusSeconds(8), Instant.now().minusSeconds(5))

        when:
        metrics.recordRefreshStarted("prices-v1", "LOCAL", "refresh-1")
        Thread.sleep(5)
        metrics.recordRefreshCompleted("prices-v1", "LOCAL", "success", "refresh-1", context)

        then:
        counter("data_refresh_started_total").count() == 1d
        counter("data_refresh_completed_total", "topic", "prices-v1", "refresh_type", "LOCAL", "status", "success", "refresh_id", "refresh-1").count() == 1d
        gauge("data_refresh_start_time_seconds", "topic", "prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").value() > 0d
        gauge("data_refresh_end_time_seconds", "topic", "prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").value() > 0d
        gauge("data_refresh_downtime_seconds", "topic", "prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").value() == 3d
        !Double.isNaN(gauge("data_refresh_active_processing_seconds", "topic", "prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").value())
        timer("data_refresh_duration_seconds", "topic", "prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").count() == 1L
    }

    def "records reset ready replay and transfer metrics and clears state"() {
        when:
        metrics.recordResetSent("prices-v1", "group-a:prices-v1", "refresh-1")
        Thread.sleep(5)
        metrics.recordResetAckReceived("prices-v1", "group-a:prices-v1", "refresh-1")
        metrics.recordResetAckDuration("prices-v1", "group-b:prices-v1", "refresh-1", 25)
        metrics.recordReadySent("prices-v1", "group-a:prices-v1", "refresh-1")
        metrics.recordReplayStarted("prices-v1", "group-a:prices-v1", "refresh-1")
        Thread.sleep(5)
        metrics.recordReadyAckReceived("prices-v1", "group-a:prices-v1", "refresh-1")
        metrics.recordReadyAckDuration("prices-v1", "group-b:prices-v1", "refresh-1", 40)
        metrics.initializeTransferMetrics("prices-v1", "group-a:prices-v1", "LOCAL", "refresh-1")
        metrics.recordDataTransferred("prices-v1", "group-a:prices-v1", 100, 3, "refresh-1", "LOCAL")
        metrics.updateTransferRate("prices-v1", "group-a:prices-v1", 55.5d, "refresh-1")
        metrics.clearTopicState("prices-v1")
        metrics.removeConsumerTimingData("prices-v1", "group-a:prices-v1")

        then:
        timer("data_refresh_reset_ack_duration_seconds", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_id", "refresh-1").count() == 1L
        timer("data_refresh_reset_ack_duration_seconds", "topic", "prices-v1", "consumer", "group-b:prices-v1", "refresh_id", "refresh-1").count() == 1L
        timer("data_refresh_ready_ack_duration_seconds", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_id", "refresh-1").count() == 1L
        timer("data_refresh_ready_ack_duration_seconds", "topic", "prices-v1", "consumer", "group-b:prices-v1", "refresh_id", "refresh-1").count() == 1L
        timer("data_refresh_replay_duration_seconds", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_id", "refresh-1").count() == 1L
        gauge("data_refresh_bytes_transferred_total", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").value() == 100d
        gauge("data_refresh_messages_transferred_total", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-1").value() == 3d
        gauge("data_refresh_transfer_rate_bytes_per_second", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_id", "refresh-1").value() == 55.5d
    }

    def "repopulates sent timestamps and reset clears gauges and timers"() {
        when:
        metrics.recordResetSentAt("prices-v1", "group-a:prices-v1", "refresh-2", Instant.now().minusMillis(20).toEpochMilli())
        metrics.recordReadySentAt("prices-v1", "group-a:prices-v1", "refresh-2", Instant.now().minusMillis(20).toEpochMilli())
        metrics.initializeTransferMetrics("prices-v1", "group-a:prices-v1", "LOCAL", "refresh-2")
        metrics.recordDataTransferred("prices-v1", "group-a:prices-v1", 200, 4, "refresh-2", "LOCAL")
        metrics.recordRefreshStarted("prices-v1", "LOCAL", "refresh-2")
        metrics.recordResetAckDuration("prices-v1", "group-a:prices-v1", "refresh-2", 12)
        metrics.recordReadyAckDuration("prices-v1", "group-a:prices-v1", "refresh-2", 14)
        metrics.recordReplayStarted("prices-v1", "group-a:prices-v1", "refresh-2")
        metrics.updateTransferRate("prices-v1", "group-a:prices-v1", 99.9d, "refresh-2")
        metrics.resetMetricsForNewRefresh()

        then:
        gauge("data_refresh_bytes_transferred_total", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-2").value() == 0d
        gauge("data_refresh_messages_transferred_total", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_type", "LOCAL", "refresh_id", "refresh-2").value() == 0d
        gauge("data_refresh_transfer_rate_bytes_per_second", "topic", "prices-v1", "consumer", "group-a:prices-v1", "refresh_id", "refresh-2").value() == 0d
        registry.find("data_refresh_reset_ack_duration_seconds").timers().isEmpty()
        registry.find("data_refresh_ready_ack_duration_seconds").timers().isEmpty()
        registry.find("data_refresh_replay_duration_seconds").timers().isEmpty()
        registry.find("data_refresh_duration_seconds").timers().isEmpty()
    }

    private def counter(String name, String... tags) {
        registry.find(name).tags(tags).counter()
    }

    private Gauge gauge(String name, String... tags) {
        registry.find(name).tags(tags).gauge()
    }

    private Timer timer(String name, String... tags) {
        registry.find(name).tags(tags).timer()
    }
}
