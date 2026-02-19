package com.messaging.broker.metrics;

import io.micrometer.core.instrument.*;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom Prometheus metrics for messaging broker
 * Tracks latency, throughput, memory, and disk I/O
 * Supports per-consumer metrics with labels
 */
@Singleton
public class BrokerMetrics {
    private static final Logger log = LoggerFactory.getLogger(BrokerMetrics.class);

    private final MeterRegistry registry;

    // Per-consumer metric caches
    private final ConcurrentHashMap<String, Counter> consumerMessagesSent = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> consumerBytesSent = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> consumerAcks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> consumerFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> consumerRetries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> consumerOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> consumerLag = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> consumerDeliveryLatency = new ConcurrentHashMap<>();

    // Failed transfer metrics - track bytes/messages that failed to send
    private final ConcurrentHashMap<String, Counter> consumerBytesFailed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> consumerMessagesFailed = new ConcurrentHashMap<>();

    // Consumer stuck detection - track last successful delivery and ACK times
    private final ConcurrentHashMap<String, AtomicLong> consumerLastDeliveryTime = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> consumerLastAckTime = new ConcurrentHashMap<>();

    // Counters
    private final Counter messagesReceived;
    private final Counter messagesSent;
    private final Counter messagesStored;
    private final Counter bytesReceived;
    private final Counter bytesSent;
    private final Counter storageReads;
    private final Counter storageWrites;
    private final Counter consumerConnections;
    private final Counter consumerDisconnections;
    private final ConcurrentHashMap<String, Counter> consumerAckTimeouts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> offsetGapsDetected = new ConcurrentHashMap<>();

    // Gauges
    private final AtomicLong activeConsumers = new AtomicLong(0);
    private final AtomicLong storageSize = new AtomicLong(0);
    private final AtomicLong activeSegments = new AtomicLong(0);

    // Timers
    private final Timer storageReadLatency;
    private final Timer storageWriteLatency;
    private final Timer messageDeliveryLatency;
    private final Timer endToEndLatency;
    private final Timer binarySearchLatency;

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

        this.bytesReceived = Counter.builder("broker.bytes.received")
            .description("Total bytes received from upstream")
            .baseUnit("bytes")
            .register(registry);

        this.bytesSent = Counter.builder("broker.bytes.sent")
            .description("Total bytes sent to consumers")
            .baseUnit("bytes")
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

        this.binarySearchLatency = Timer.builder("broker.storage.binary_search.latency")
            .description("Latency of binary search through index file for offset lookup")
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

    public void recordMessageReceived(long bytes) {
        messagesReceived.increment();
        bytesReceived.increment(bytes);
    }

    public void recordMessageSent() {
        messagesSent.increment();
    }

    public void recordMessageSent(long bytes) {
        messagesSent.increment();
        bytesSent.increment(bytes);
    }

    /**
     * Record multiple messages sent in a batch (efficient version)
     */
    public void recordBatchMessagesSent(int messageCount, long totalBytes) {
        messagesSent.increment(messageCount);
        bytesSent.increment(totalBytes);
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

    public Timer.Sample startBinarySearchTimer() {
        return Timer.start(registry);
    }

    public void stopBinarySearchTimer(Timer.Sample sample) {
        sample.stop(binarySearchLatency);
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

    // ==================== PER-CONSUMER METRICS ====================

    /**
     * Record a message sent to a specific consumer group
     * Uses topic+group as key for stable metrics across consumer reconnections
     */
    public void recordConsumerMessageSent(String consumerId, String topic, String group, long bytes) {
        String key = group + ":" + topic;

        // Get or create counter for this group+topic
        Counter counter = consumerMessagesSent.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.messages.sent")
                .description("Messages sent to consumer group for topic")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        counter.increment();

        // Track bytes
        Counter bytesCounter = consumerBytesSent.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.bytes.sent")
                .description("Bytes sent to consumer group for topic")
                .tag("topic", topic)
                .tag("group", group)
                .baseUnit("bytes")
                .register(registry)
        );
        bytesCounter.increment(bytes);
    }

    /**
     * Record a batch of messages sent to a specific consumer (efficient version for zero-copy batches)
     * Uses topic+group as key for stable metrics across consumer reconnections
     */
    public void recordConsumerBatchSent(String consumerId, String topic, String group, int messageCount, long totalBytes) {
        String key = group + ":" + topic;

        // Get or create counter for this group+topic
        Counter counter = consumerMessagesSent.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.messages.sent")
                .description("Messages sent to consumer group for topic")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        counter.increment(messageCount);

        // Track bytes
        Counter bytesCounter = consumerBytesSent.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.bytes.sent")
                .description("Bytes sent to consumer group for topic")
                .tag("topic", topic)
                .tag("group", group)
                .baseUnit("bytes")
                .register(registry)
        );
        bytesCounter.increment(totalBytes);
    }

    /**
     * Record an ACK from a specific consumer group
     */
    public void recordConsumerAck(String consumerId, String topic, String group) {
        String key = group + ":" + topic;
        Counter counter = consumerAcks.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.acks")
                .description("ACKs received from consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        counter.increment();
    }

    /**
     * Record a delivery failure for a specific consumer group
     */
    public void recordConsumerFailure(String consumerId, String topic, String group) {
        String key = group + ":" + topic;
        Counter counter = consumerFailures.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.failures")
                .description("Failed message deliveries to consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        counter.increment();
    }

    /**
     * Record a retry for a specific consumer group
     */
    public void recordConsumerRetry(String consumerId, String topic, String group) {
        String key = group + ":" + topic;
        Counter counter = consumerRetries.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.retries")
                .description("Message retry attempts for consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        counter.increment();
    }

    /**
     * Update the current offset for a consumer group
     */
    public void updateConsumerOffset(String consumerId, String topic, String group, long offset) {
        String key = group + ":" + topic;
        AtomicLong gauge = consumerOffsets.computeIfAbsent(key, k -> {
            AtomicLong atomicOffset = new AtomicLong(0);
            Gauge.builder("broker.consumer.offset", atomicOffset, AtomicLong::get)
                .description("Current offset for consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return atomicOffset;
        });
        gauge.set(offset);
    }

    /**
     * Update the lag for a consumer group (difference between head and consumer offset)
     */
    public void updateConsumerLag(String consumerId, String topic, String group, long lag) {
        String key = group + ":" + topic;
        AtomicLong gauge = consumerLag.computeIfAbsent(key, k -> {
            AtomicLong atomicLag = new AtomicLong(0);
            Gauge.builder("broker.consumer.lag", atomicLag, AtomicLong::get)
                .description("Message lag for consumer group (head - consumer offset)")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return atomicLag;
        });
        gauge.set(lag);
    }

    /**
     * Start timing delivery to a specific consumer
     */
    public Timer.Sample startConsumerDeliveryTimer() {
        return Timer.start(registry);
    }

    /**
     * Stop timing delivery to a specific consumer group
     */
    public void stopConsumerDeliveryTimer(Timer.Sample sample, String consumerId, String topic, String group) {
        String key = group + ":" + topic;
        Timer timer = consumerDeliveryLatency.computeIfAbsent(key, k ->
            Timer.builder("broker.consumer.delivery.latency")
                .description("Delivery latency to consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
        );
        sample.stop(timer);
    }

    /**
     * Remove all metrics for a consumer group when they disconnect
     * NOTE: With stable group+topic identifiers, metrics should persist across reconnections.
     * This method may no longer be necessary and could be deprecated in the future.
     */
    public void removeConsumerMetrics(String consumerId, String topic, String group) {
        String key = group + ":" + topic;

        // NOTE: Removing metrics on disconnect causes graph breaks.
        // With stable identifiers, we should NOT remove metrics to preserve historical data.
        // Commenting out removal - metrics will persist across consumer reconnections.

        // consumerMessagesSent.remove(key);
        // consumerBytesSent.remove(key);
        // consumerAcks.remove(key);
        // consumerFailures.remove(key);
        // consumerRetries.remove(key);
        // consumerOffsets.remove(key);
        // consumerLag.remove(key);
        // consumerDeliveryLatency.remove(key);

        log.debug("Consumer disconnected but metrics retained for group: {} topic: {} (stable identifier: {})",
                  group, topic, key);
    }

    /**
     * Record successful adaptive poll (data found and delivered)
     */
    public void recordAdaptivePollSuccess(String topic) {
        // Could add specific metrics here if needed
        log.trace("Adaptive poll success: topic={}", topic);
    }

    /**
     * Record skipped adaptive poll (no data or delivery blocked)
     */
    public void recordAdaptivePollSkipped(String topic) {
        // Could add specific metrics here if needed
        log.trace("Adaptive poll skipped: topic={}", topic);
    }

    /**
     * Record ACK timeout for a specific topic and consumer group.
     * B2-7 fix: added group parameter so Grafana can filter ACK timeout alerts by consumer group.
     */
    public void recordAckTimeout(String topic, String group) {
        String key = topic + ":" + group;
        Counter counter = consumerAckTimeouts.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.ack.timeouts")
                .description("ACK timeouts for consumer deliveries")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        counter.increment();
        log.debug("Recorded ACK timeout for topic={} group={}", topic, group);
    }

    /**
     * Record when an offset gap is detected during binary search
     */
    public void recordOffsetGapDetected(String topic, String partition) {
        String key = topic + ":" + partition;
        Counter counter = offsetGapsDetected.computeIfAbsent(key, k ->
            Counter.builder("broker.storage.offset_gaps_detected")
                .description("Number of offset gaps detected during reads")
                .tag("topic", topic)
                .tag("partition", partition)
                .register(registry)
        );
        counter.increment();
        log.debug("Recorded offset gap for topic: {}, partition: {}", topic, partition);
    }

    /**
     * Record failed transfer (bytes and messages that failed to send to consumer)
     * This allows calculation of successful bytes = total bytes - failed bytes
     */
    public void recordConsumerTransferFailed(String consumerId, String topic, String group, int messageCount, long totalBytes) {
        String key = group + ":" + topic;

        // Track failed bytes
        Counter bytesCounter = consumerBytesFailed.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.bytes.failed")
                .description("Bytes that failed to send to consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .baseUnit("bytes")
                .register(registry)
        );
        bytesCounter.increment(totalBytes);

        // Track failed messages
        Counter messagesCounter = consumerMessagesFailed.computeIfAbsent(key, k ->
            Counter.builder("broker.consumer.messages.failed")
                .description("Messages that failed to send to consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry)
        );
        messagesCounter.increment(messageCount);

        log.debug("Recorded failed transfer for group: {} topic: {} - {} messages, {} bytes",
                 group, topic, messageCount, totalBytes);
    }

    /**
     * Update last successful delivery timestamp for stuck detection
     * This tracks when the broker last successfully sent data to a consumer
     */
    public void updateConsumerLastDeliveryTime(String consumerId, String topic, String group) {
        String key = group + ":" + topic;
        long currentTime = System.currentTimeMillis();

        AtomicLong gauge = consumerLastDeliveryTime.computeIfAbsent(key, k -> {
            AtomicLong atomicTime = new AtomicLong(currentTime);
            Gauge.builder("broker.consumer.last_delivery_time_ms", atomicTime, AtomicLong::get)
                .description("Timestamp (epoch ms) of last successful delivery to consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return atomicTime;
        });
        gauge.set(currentTime);
    }

    /**
     * Update last ACK timestamp for stuck detection
     * This tracks when the broker last received an ACK from a consumer
     */
    public void updateConsumerLastAckTime(String consumerId, String topic, String group) {
        String key = group + ":" + topic;
        long currentTime = System.currentTimeMillis();

        AtomicLong gauge = consumerLastAckTime.computeIfAbsent(key, k -> {
            AtomicLong atomicTime = new AtomicLong(currentTime);
            Gauge.builder("broker.consumer.last_ack_time_ms", atomicTime, AtomicLong::get)
                .description("Timestamp (epoch ms) of last ACK received from consumer group")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return atomicTime;
        });
        gauge.set(currentTime);
    }

    /**
     * Calculate time since last delivery (for stuck detection in Grafana)
     * This is a derived metric: (current_time - last_delivery_time_ms) / 1000
     * Can be used in Grafana with query:
     * (time() * 1000 - broker_consumer_last_delivery_time_ms) / 1000
     */
    public long getTimeSinceLastDelivery(String group, String topic) {
        String key = group + ":" + topic;
        AtomicLong lastTime = consumerLastDeliveryTime.get(key);
        if (lastTime == null) {
            return -1; // No delivery yet
        }
        return (System.currentTimeMillis() - lastTime.get()) / 1000; // Return seconds
    }

    /**
     * Calculate time since last ACK (for stuck detection in Grafana)
     * This is a derived metric: (current_time - last_ack_time_ms) / 1000
     * Can be used in Grafana with query:
     * (time() * 1000 - broker_consumer_last_ack_time_ms) / 1000
     */
    public long getTimeSinceLastAck(String group, String topic) {
        String key = group + ":" + topic;
        AtomicLong lastTime = consumerLastAckTime.get(key);
        if (lastTime == null) {
            return -1; // No ACK yet
        }
        return (System.currentTimeMillis() - lastTime.get()) / 1000; // Return seconds
    }
}
