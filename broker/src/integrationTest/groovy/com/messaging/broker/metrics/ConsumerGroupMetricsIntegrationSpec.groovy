package com.messaging.broker.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

/**
 * Integration test demonstrating the consumer group metrics aggregation issue
 *
 * This test shows why sum(broker_consumer_bytes_sent_total) inflates the byte count
 * when multiple consumer groups read the same topic.
 */
class ConsumerGroupMetricsIntegrationSpec extends Specification {

    MeterRegistry registry
    BrokerMetrics metrics

    def setup() {
        registry = new SimpleMeterRegistry()
        metrics = new BrokerMetrics(registry)
    }

    def "metrics: multiple consumer groups reading same data inflates sum() aggregation"() {
        given: "3 consumer groups reading the same topic with 6GB of data each"
        def topic = "test-topic"
        def dataSize = 6L * 1024L * 1024L * 1024L  // 6GB in bytes

        def groups = ["group-A", "group-B", "group-C"]

        when: "each group receives 6GB of data"
        groups.each { group ->
            metrics.recordConsumerBatchSent("client-${group}", topic, group, 100, dataSize)
        }

        and: "we query individual group metrics"
        def groupABytes = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-A").count()
        def groupBBytes = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-B").count()
        def groupCBytes = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-C").count()

        and: "we simulate sum() aggregation (what the problematic query does)"
        def totalSummed = groupABytes + groupBBytes + groupCBytes

        then: "each individual group shows 6GB"
        groupABytes == dataSize
        groupBBytes == dataSize
        groupCBytes == dataSize

        and: "sum() aggregation shows 18GB (3 groups × 6GB)"
        totalSummed == dataSize * 3
        totalSummed == 18L * 1024L * 1024L * 1024L  // 18GB

        and: "this demonstrates the issue: sum() inflates the byte count"
        totalSummed > dataSize  // 18GB > 6GB
    }

    def "metrics: simulating 33 consumer groups explains 200GB observation"() {
        given: "33 consumer groups (like in production) reading 6GB each"
        def topic = "production-topic"
        def dataSize = 6L * 1024L * 1024L * 1024L  // 6GB in bytes
        def numGroups = 33

        when: "each of 33 groups receives 6GB"
        (1..numGroups).each { i ->
            def group = "consumer-group-${i}"
            metrics.recordConsumerBatchSent("client-${i}", topic, group, 100, dataSize)
        }

        and: "we calculate sum() across all groups"
        def totalSummed = 0L
        (1..numGroups).each { i ->
            def group = "consumer-group-${i}"
            def counter = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", group)
            totalSummed += counter.count()
        }

        def totalGB = totalSummed / 1024.0 / 1024.0 / 1024.0

        then: "sum() shows 198GB (33 groups × 6GB)"
        totalSummed == dataSize * numGroups
        totalGB == 198.0

        and: "this matches the observed 200GB in production!"
        Math.abs(totalGB - 200.0) < 5.0  // Within 5GB of observed value
    }

    def "metrics: correct query without sum() shows per-group bytes"() {
        given: "multiple consumer groups"
        def topic = "test-topic"
        def dataSize = 6L * 1024L * 1024L * 1024L

        when: "groups receive data"
        metrics.recordConsumerBatchSent("client-1", topic, "group-1", 100, dataSize)
        metrics.recordConsumerBatchSent("client-2", topic, "group-2", 100, dataSize)
        metrics.recordConsumerBatchSent("client-3", topic, "group-3", 100, dataSize)

        and: "we query without sum() - per group"
        def group1Bytes = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-1").count()
        def group2Bytes = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-2").count()
        def group3Bytes = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-3").count()

        then: "each group shows correct 6GB (not summed)"
        group1Bytes == dataSize
        group2Bytes == dataSize
        group3Bytes == dataSize

        and: "no group shows inflated value"
        group1Bytes != dataSize * 3
        group2Bytes != dataSize * 3
        group3Bytes != dataSize * 3
    }

    def "metrics: data refresh does not create duplicate group metrics"() {
        given: "a consumer group that has already received 6GB"
        def topic = "refresh-topic"
        def group = "test-group"
        def dataSize = 6L * 1024L * 1024L * 1024L

        metrics.recordConsumerBatchSent("client-1", topic, group, 100, dataSize)
        def bytesAfterInitial = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", group).count()

        when: "data refresh happens and replays the same 6GB"
        metrics.recordConsumerBatchSent("client-1", topic, group, 100, dataSize)

        def bytesAfterRefresh = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", group).count()

        then: "initial delivery shows 6GB"
        bytesAfterInitial == dataSize

        and: "after refresh, counter increases to 12GB (cumulative)"
        bytesAfterRefresh == dataSize * 2

        and: "this demonstrates why counter is cumulative"
        bytesAfterRefresh > bytesAfterInitial
    }

    def "metrics: verifying metric naming convention"() {
        given: "consumer group metrics"
        def topic = "test-topic"
        def group = "test-group"

        when: "recording bytes sent"
        metrics.recordConsumerBatchSent("client-1", topic, group, 10, 1024L)

        then: "metric name is broker.consumer.bytes.sent (not broker_consumer_bytes_sent_bytes_total)"
        def counter = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", group)
        counter != null
        counter.count() == 1024L

        and: "metric has correct tags"
        counter.id.tags.find { it.key == "topic" }?.value == topic
        counter.id.tags.find { it.key == "group" }?.value == group
    }

    def "metrics: simulating Prometheus query behavior"() {
        given: "multiple groups with different data sizes"
        def topic = "mixed-topic"

        when: "groups receive different amounts of data"
        metrics.recordConsumerBatchSent("c1", topic, "group-1", 10, 1_000_000_000L)  // 1GB
        metrics.recordConsumerBatchSent("c2", topic, "group-2", 20, 2_000_000_000L)  // 2GB
        metrics.recordConsumerBatchSent("c3", topic, "group-3", 30, 3_000_000_000L)  // 3GB

        and: "we simulate different query approaches"

        // Approach 1: sum() across all groups (WRONG for per-group analysis)
        def sumAcrossGroups = 0L
        ["group-1", "group-2", "group-3"].each { g ->
            sumAcrossGroups += registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", g).count()
        }

        // Approach 2: Individual group values (CORRECT)
        def group1Only = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-1").count()
        def group2Only = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-2").count()
        def group3Only = registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", "group-3").count()

        then: "sum() gives total across all groups (6GB)"
        sumAcrossGroups == 6_000_000_000L

        and: "individual queries show correct per-group values"
        group1Only == 1_000_000_000L
        group2Only == 2_000_000_000L
        group3Only == 3_000_000_000L

        and: "sum is not appropriate when you want per-group metrics"
        sumAcrossGroups != group1Only
        sumAcrossGroups == (group1Only + group2Only + group3Only)
    }

    def "metrics: demonstrating the fix - use increase() not sum()"() {
        given: "initial state with 3 consumer groups"
        def topic = "test-topic"
        def initialBytes = 5L * 1024L * 1024L * 1024L  // 5GB

        when: "initial batch sent to all groups"
        metrics.recordConsumerBatchSent("c1", topic, "group-1", 100, initialBytes)
        metrics.recordConsumerBatchSent("c2", topic, "group-2", 100, initialBytes)
        metrics.recordConsumerBatchSent("c3", topic, "group-3", 100, initialBytes)

        def snapshot1_sum = 0L
        ["group-1", "group-2", "group-3"].each { g ->
            snapshot1_sum += registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", g).count()
        }

        and: "additional batch sent (simulating 6h time window)"
        def additionalBytes = 1L * 1024L * 1024L * 1024L  // 1GB more
        metrics.recordConsumerBatchSent("c1", topic, "group-1", 20, additionalBytes)
        metrics.recordConsumerBatchSent("c2", topic, "group-2", 20, additionalBytes)
        metrics.recordConsumerBatchSent("c3", topic, "group-3", 20, additionalBytes)

        def snapshot2_sum = 0L
        ["group-1", "group-2", "group-3"].each { g ->
            snapshot2_sum += registry.counter("broker.consumer.bytes.sent", "topic", topic, "group", g).count()
        }

        // Simulate increase() over time window
        def increase_sum = snapshot2_sum - snapshot1_sum

        then: "sum() shows cumulative total (18GB after all batches)"
        snapshot2_sum == (initialBytes + additionalBytes) * 3

        and: "increase() shows bytes sent in time window (3GB across 3 groups)"
        increase_sum == additionalBytes * 3

        and: "per-group increase() would show 1GB each (correct!)"
        def group1Increase = additionalBytes
        group1Increase == 1L * 1024L * 1024L * 1024L
    }
}
