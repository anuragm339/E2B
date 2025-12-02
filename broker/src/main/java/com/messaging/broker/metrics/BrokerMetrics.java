package com.messaging.broker.metrics;

import io.micrometer.core.instrument.*;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom Prometheus metrics for messaging broker
 * Tracks latency, throughput, memory, and disk I/O
 */
@Singleton
public class BrokerMetrics {
    private static final Logger log = LoggerFactory.getLogger(BrokerMetrics.class);

    private final MeterRegistry registry;

    // Counters
    private final Counter messagesReceived;
    private final Counter messagesSent;
    private final Counter messagesStored;
    private final Counter storageReads;
    private final Counter storageWrites;
    private final Counter consumerConnections;
    private final Counter consumerDisconnections;

    // Gauges
    private final AtomicLong activeConsumers = new AtomicLong(0);
    private final AtomicLong storageSize = new AtomicLong(0);
    private final AtomicLong activeSegments = new AtomicLong(0);

    // Timers
    private final Timer storageReadLatency;
    private final Timer storageWriteLatency;
    private final Timer messageDeliveryLatency;
    private final Timer endToEndLatency;

    // Distribution Summaries
    private final DistributionSummary messageSizeBytes;
    private final DistributionSummary batchSize;

    public BrokerMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Counters
        this.messagesReceived = Counter.builder("broker.messages.received")
            .description("Total number of messages received by broker")
            .tag("type", "all")
            .register(registry);

        this.messagesSent = Counter.builder("broker.messages.sent")
            .description("Total number of messages sent to consumers")
            .tag("type", "all")
            .register(registry);

        this.messagesStored = Counter.builder("broker.messages.stored")
            .description("Total number of messages stored to disk")
            .register(registry);

        this.storageReads = Counter.builder("broker.storage.reads")
            .description("Total number of storage read operations")
            .register(registry);

        this.storageWrites = Counter.builder("broker.storage.writes")
            .description("Total number of storage write operations")
            .register(registry);

        this.consumerConnections = Counter.builder("broker.consumer.connections")
            .description("Total number of consumer connections")
            .register(registry);

        this.consumerDisconnections = Counter.builder("broker.consumer.disconnections")
            .description("Total number of consumer disconnections")
            .register(registry);

        // Gauges
        Gauge.builder("broker.consumer.active", activeConsumers, AtomicLong::get)
            .description("Number of currently active consumers")
            .register(registry);

        Gauge.builder("broker.storage.size.bytes", storageSize, AtomicLong::get)
            .description("Total storage size in bytes")
            .register(registry);

        Gauge.builder("broker.storage.segments.active", activeSegments, AtomicLong::get)
            .description("Number of active storage segments")
            .register(registry);

        // Timers
        this.storageReadLatency = Timer.builder("broker.storage.read.latency")
            .description("Latency of storage read operations")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        this.storageWriteLatency = Timer.builder("broker.storage.write.latency")
            .description("Latency of storage write operations")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        this.messageDeliveryLatency = Timer.builder("broker.message.delivery.latency")
            .description("Latency of message delivery to consumers")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        this.endToEndLatency = Timer.builder("broker.message.e2e.latency")
            .description("End-to-end message latency (receive to delivery)")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        // Distribution Summaries
        this.messageSizeBytes = DistributionSummary.builder("broker.message.size.bytes")
            .description("Distribution of message sizes in bytes")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        this.batchSize = DistributionSummary.builder("broker.batch.size")
            .description("Distribution of batch sizes")
            .register(registry);

        log.info("Broker metrics initialized and registered with Prometheus");
    }

    // Counter methods
    public void recordMessageReceived() {
        messagesReceived.increment();
    }

    public void recordMessageSent() {
        messagesSent.increment();
    }

    public void recordMessageStored() {
        messagesStored.increment();
    }

    public void recordStorageRead() {
        storageReads.increment();
    }

    public void recordStorageWrite() {
        storageWrites.increment();
    }

    public void recordConsumerConnection() {
        consumerConnections.increment();
        activeConsumers.incrementAndGet();
    }

    public void recordConsumerDisconnection() {
        consumerDisconnections.increment();
        activeConsumers.decrementAndGet();
    }

    // Gauge update methods
    public void updateStorageSize(long bytes) {
        storageSize.set(bytes);
    }

    public void updateActiveSegments(long count) {
        activeSegments.set(count);
    }

    // Timer methods (returns Timer.Sample for start/stop pattern)
    public Timer.Sample startStorageReadTimer() {
        return Timer.start(registry);
    }

    public void stopStorageReadTimer(Timer.Sample sample) {
        sample.stop(storageReadLatency);
    }

    public Timer.Sample startStorageWriteTimer() {
        return Timer.start(registry);
    }

    public void stopStorageWriteTimer(Timer.Sample sample) {
        sample.stop(storageWriteLatency);
    }

    public Timer.Sample startMessageDeliveryTimer() {
        return Timer.start(registry);
    }

    public void stopMessageDeliveryTimer(Timer.Sample sample) {
        sample.stop(messageDeliveryLatency);
    }

    public Timer.Sample startE2ETimer() {
        return Timer.start(registry);
    }

    public void stopE2ETimer(Timer.Sample sample) {
        sample.stop(endToEndLatency);
    }

    // Distribution methods
    public void recordMessageSize(long bytes) {
        messageSizeBytes.record(bytes);
    }

    public void recordBatchSize(int size) {
        batchSize.record(size);
    }

    // Getters for raw metrics (for testing/debugging)
    public long getMessagesReceived() {
        return (long) messagesReceived.count();
    }

    public long getMessagesSent() {
        return (long) messagesSent.count();
    }

    public long getActiveConsumers() {
        return activeConsumers.get();
    }
}
