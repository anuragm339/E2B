package com.messaging.broker.monitoring

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

import java.time.Duration

class BrokerMetricsSpec extends Specification {

    SimpleMeterRegistry registry
    BrokerMetrics metrics

    def setup() {
        registry = new SimpleMeterRegistry()
        metrics = new BrokerMetrics(registry)
    }

    def cleanup() {
        registry.close()
    }

    def "records broker level counters gauges timers and summaries"() {
        when:
        metrics.recordMessageReceived()
        metrics.recordMessageReceived(128)
        metrics.recordMessageSent()
        metrics.recordMessageSent(256)
        metrics.recordBatchMessagesSent(3, 512)
        metrics.recordMessageStored()
        metrics.recordStorageRead()
        metrics.recordStorageWrite()
        metrics.recordConsumerConnection()
        metrics.recordConsumerDisconnection()
        metrics.updateStorageSize(2048)
        metrics.updateActiveSegments(7)
        metrics.recordTopicLastMessageTime(null)
        metrics.recordTopicLastMessageTime("prices-v1")
        metrics.recordMessageSize(99)
        metrics.recordBatchSize(5)

        def readSample = metrics.startStorageReadTimer()
        def writeSample = metrics.startStorageWriteTimer()
        def deliverySample = metrics.startMessageDeliveryTimer()
        def e2eSample = metrics.startE2ETimer()
        def binarySample = metrics.startBinarySearchTimer()

        readSample.stop(registry.timer("tmp.read"))
        writeSample.stop(registry.timer("tmp.write"))
        deliverySample.stop(registry.timer("tmp.delivery"))
        e2eSample.stop(registry.timer("tmp.e2e"))
        binarySample.stop(registry.timer("tmp.binary"))

        metrics.stopStorageReadTimer(Timer.start(registry))
        metrics.stopStorageWriteTimer(Timer.start(registry))
        metrics.stopMessageDeliveryTimer(Timer.start(registry))
        metrics.stopE2ETimer(Timer.start(registry))
        metrics.stopBinarySearchTimer(Timer.start(registry))

        then:
        metrics.messagesReceived == 2
        metrics.messagesSent == 5
        metrics.activeConsumers == 0

        counter("broker.messages.received", "type", "all").count() == 2d
        counter("broker.bytes.received").count() == 128d
        counter("broker.messages.sent", "type", "all").count() == 5d
        counter("broker.bytes.sent").count() == 768d
        counter("broker.messages.stored").count() == 1d
        counter("broker.storage.reads").count() == 1d
        counter("broker.storage.writes").count() == 1d
        counter("broker.consumer.connections").count() == 1d
        counter("broker.consumer.disconnections").count() == 1d
        gauge("broker.storage.size.bytes").value() == 2048d
        gauge("broker.storage.segments.active").value() == 7d
        gauge("broker.consumer.active").value() == 0d
        gauge("broker_topic_last_message_time_seconds", "topic", "unknown").value() > 0d
        gauge("broker_topic_last_message_time_seconds", "topic", "prices-v1").value() > 0d
        summary("broker.message.size.bytes").count() == 1L
        summary("broker.batch.size").count() == 1L
        timer("broker.storage.read.latency").count() == 1L
        timer("broker.storage.write.latency").count() == 1L
        timer("broker.message.delivery.latency").count() == 1L
        timer("broker.message.e2e.latency").count() == 1L
        timer("broker.storage.binary_search.latency").count() == 1L
    }

    def "records per consumer delivery metrics and cleanup paths"() {
        when:
        metrics.recordConsumerMessageSent("client-1", "prices-v1", "group-a", 100)
        metrics.recordConsumerBatchSent("client-2", "prices-v1", "group-a", 4, 400)
        metrics.recordConsumerAck("client-1", "prices-v1", "group-a")
        metrics.recordConsumerFailure("client-1", "prices-v1", "group-a")
        metrics.recordConsumerRetry("client-1", "prices-v1", "group-a")
        metrics.updateConsumerOffset("client-1", "prices-v1", "group-a", 42)
        metrics.updateConsumerLag("client-1", "prices-v1", "group-a", 7)
        metrics.stopConsumerDeliveryTimer(metrics.startConsumerDeliveryTimer(), "client-1", "prices-v1", "group-a")
        metrics.recordAckTimeout("prices-v1", "group-a")
        metrics.recordOffsetGapDetected("prices-v1", "0")
        metrics.recordConsumerTransferFailed("client-1", "prices-v1", "group-a", 2, 300)
        metrics.updateConsumerLastDeliveryTime("client-1", "prices-v1", "group-a")
        metrics.updateConsumerLastAckTime("client-1", "prices-v1", "group-a")
        metrics.updateReconciliationMissingKeys("prices-v1", "group-a", 3)
        metrics.updateReconciliationGapOffsets("prices-v1", "group-a", 11, 29)
        metrics.startPendingAck("prices-v1", "group-a")
        Thread.sleep(5)
        def pendingGaugeBefore = gauge("broker.consumer.pending_ack_age_seconds", "topic", "prices-v1", "group", "group-a").value()
        def timeSinceDelivery = metrics.getTimeSinceLastDelivery("group-a", "prices-v1")
        def timeSinceAck = metrics.getTimeSinceLastAck("group-a", "prices-v1")
        metrics.completePendingAck("prices-v1", "group-a")
        metrics.removeConsumerMetrics("client-1", "prices-v1", "group-a")

        then:
        counter("broker.consumer.messages.sent", "topic", "prices-v1", "group", "group-a").count() == 5d
        counter("broker.consumer.bytes.sent", "topic", "prices-v1", "group", "group-a").count() == 500d
        counter("broker.consumer.acks", "topic", "prices-v1", "group", "group-a").count() == 1d
        counter("broker.consumer.failures", "topic", "prices-v1", "group", "group-a").count() == 1d
        counter("broker.consumer.retries", "topic", "prices-v1", "group", "group-a").count() == 1d
        counter("broker.consumer.ack.timeouts", "topic", "prices-v1", "group", "group-a").count() == 1d
        counter("broker.storage.offset_gaps_detected", "topic", "prices-v1", "partition", "0").count() == 1d
        counter("broker.consumer.bytes.failed", "topic", "prices-v1", "group", "group-a").count() == 300d
        counter("broker.consumer.messages.failed", "topic", "prices-v1", "group", "group-a").count() == 2d
        gauge("broker.consumer.offset", "topic", "prices-v1", "group", "group-a").value() == 42d
        gauge("broker.consumer.lag", "topic", "prices-v1", "group", "group-a").value() == 7d
        gauge("ack.reconciliation.missing.keys", "topic", "prices-v1", "group", "group-a").value() == 3d
        gauge("ack.reconciliation.gap.min.offset", "topic", "prices-v1", "group", "group-a").value() == 11d
        gauge("ack.reconciliation.gap.max.offset", "topic", "prices-v1", "group", "group-a").value() == 29d
        gauge("broker.consumer.last_delivery_time_ms", "topic", "prices-v1", "group", "group-a").value() > 0d
        gauge("broker.consumer.last_ack_time_ms", "topic", "prices-v1", "group", "group-a").value() > 0d
        timer("broker.consumer.delivery.latency", "topic", "prices-v1", "group", "group-a").count() == 1L
        pendingGaugeBefore >= 0d
        gauge("broker.consumer.pending_ack_age_seconds", "topic", "prices-v1", "group", "group-a").value() == 0d
        timeSinceDelivery >= 0
        timeSinceAck >= 0
        metrics.getTimeSinceLastDelivery("group-a", "unknown-topic") == -1
        metrics.getTimeSinceLastAck("group-a", "unknown-topic") == -1
    }

    private Counter counter(String name, String... tags) {
        registry.find(name).tags(tags).counter()
    }

    private Gauge gauge(String name, String... tags) {
        registry.find(name).tags(tags).gauge()
    }

    private Timer timer(String name, String... tags) {
        registry.find(name).tags(tags).timer()
    }

    private DistributionSummary summary(String name, String... tags) {
        registry.find(name).tags(tags).summary()
    }
}
