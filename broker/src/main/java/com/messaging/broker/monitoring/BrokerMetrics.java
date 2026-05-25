package com.messaging.broker.monitoring;

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
    private final ConcurrentHashMap<String, Counter> consumerDeliveryBlocked = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> topicMessagesStored = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> consumerOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> consumerLag = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> consumerDeliveryLatency = new ConcurrentHashMap<>();

    // Failed transfer metrics - track bytes/messages that failed to send
    private final ConcurrentHashMap<String, Counter> consumerBytesFailed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> consumerMessagesFailed = new ConcurrentHashMap<>();

    // Consumer stuck detection - track last successful delivery and ACK times
    private final ConcurrentHashMap<String, AtomicLong> consumerLastDeliveryTime = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> consumerLastAckTime = new ConcurrentHashMap<>();

    // Pending ACK age tracking - how long current ACK has been pending (milliseconds since epoch)
    private final ConcurrentHashMap<String, AtomicLong> pendingAckStartTime = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge> pendingAckAgeGauges = new ConcurrentHashMap<>();

    // Legacy merged-batch observability - per legacy consumer group
    private final ConcurrentHashMap<String, AtomicLong> legacyLastBatchSendTime = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> legacyLastBatchAckTime = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> legacyPendingBatchStartTime = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> legacyPendingBatchMessages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> legacyPendingBatchTopics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge> legacyPendingBatchAgeGauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> legacyDeliveryBlocked = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> legacyBatchEvents = new ConcurrentHashMap<>();

    // ACK reconciliation: number of msgKeys in sealed segments with no RocksDB ACK record
    private final ConcurrentHashMap<String, AtomicLong> reconciliationMissingKeys = new ConcurrentHashMap<>();
    // ACK reconciliation: offset range of the gap (min/max offset among unACKed msgKeys; -1 when fully consistent)
    private final ConcurrentHashMap<String, AtomicLong> reconciliationGapMinOffset = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> reconciliationGapMaxOffset = new ConcurrentHashMap<>();

    // Topic freshness - track last message time per topic (seconds since epoch)
    private final ConcurrentHashMap<String, AtomicLong> topicLastMessageTimeSeconds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge> topicLastMessageTimeGauges = new ConcurrentHashMap<>();

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

    // Compaction metrics — global
    private final Counter compactionRunsTotal;
    private final Timer compactionDuration;

    // Compaction metrics — per-topic
    private final ConcurrentHashMap<String, Counter> compactionRunsByTopic        = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionErrorsByTopic      = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionRecordsRemoved     = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionTombstonesRemoved  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionBytesRead          = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionBytesWritten       = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionBytesReclaimed     = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> compactionSegmentsReplaced   = new ConcurrentHashMap<>();

    // Compaction gauges — per-topic (epoch seconds of last run; 0 = never)
    private final ConcurrentHashMap<String, AtomicLong> compactionLastRunTimestamp = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge>      compactionLastRunGauges    = new ConcurrentHashMap<>();

    // Compaction active flag — per-topic (1 = compaction in progress, 0 = idle)
    private final ConcurrentHashMap<String, AtomicLong> compactionActiveFlag  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge>      compactionActiveGauges = new ConcurrentHashMap<>();

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

        // Compaction counters/timers — global (no topic tag)
        this.compactionRunsTotal = Counter.builder("broker.compaction.runs.total")
            .description("Total number of compaction runs (all topics combined)")
            .register(registry);

        this.compactionDuration = Timer.builder("broker.compaction.duration.seconds")
            .description("Wall-clock duration of each full compaction sweep across all topics")
            .publishPercentiles(0.5, 0.95, 0.99)
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

    /**
     * Record a stored message for a specific topic.
     * This allows Grafana to compute a message-count backlog estimate that is
     * independent of sparse offset spacing.
     */
    public void recordMessageStored(String topic) {
        messagesStored.increment();

        String topicLabel = (topic == null || topic.isBlank()) ? "unknown" : topic;
        topicMessagesStored.computeIfAbsent(topicLabel, key ->
                Counter.builder("broker.topic.messages.stored")
                        .description("Messages stored by topic since broker start")
                        .tag("topic", topicLabel)
                        .register(registry)
        ).increment();
    }

    /**
     * Update last message time for a topic (seconds since epoch).
     * Used for data freshness SLA dashboards.
     */
    public void recordTopicLastMessageTime(String topic) {
        if (topic == null || topic.isBlank()) {
            topic = "unknown";
        }
        final String topicLabel = topic;
        String key = topicLabel;
        AtomicLong value = topicLastMessageTimeSeconds.computeIfAbsent(key, k -> {
            AtomicLong atomic = new AtomicLong(0);
            topicLastMessageTimeGauges.computeIfAbsent(key, gk ->
                    Gauge.builder("broker_topic_last_message_time_seconds", atomic, AtomicLong::get)
                            .description("Last message time per topic (seconds since epoch)")
                            .tag("topic", topicLabel)
                            .register(registry)
            );
            return atomic;
        });
        value.set(System.currentTimeMillis() / 1000);
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

    public void recordConsumerDeliveryBlocked(String topic, String group, String reason) {
        String groupLabel = group == null || group.isBlank() ? "unknown" : group;
        String topicLabel = topic == null || topic.isBlank() ? "unknown" : topic;
        String reasonLabel = reason == null || reason.isBlank() ? "unknown" : reason;
        String key = groupLabel + ":" + topicLabel + ":" + reasonLabel;
        consumerDeliveryBlocked.computeIfAbsent(key, k ->
                Counter.builder("broker.consumer.delivery.blocked")
                        .description("Number of delivery attempts blocked before a batch send")
                        .tag("topic", topicLabel)
                        .tag("group", groupLabel)
                        .tag("reason", reasonLabel)
                        .register(registry)
        ).increment();
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
     *
     * OOM FIX: Re-enabled cleanup to prevent memory leak from ephemeral port changes.
     * Despite using "stable" group:topic keys, the underlying clientId (with ephemeral port)
     * causes duplicate metric registrations on reconnect, leaking Counter/Gauge/Timer objects.
     *
     * Trade-off: Prometheus graphs may show gaps on disconnect, but this prevents unbounded
     * memory growth and cardinality explosion.
     */
    public void removeConsumerMetrics(String consumerId, String topic, String group) {
        String key = group + ":" + topic;

        // OOM FIX: Re-enabled removal to prevent memory leak from ephemeral port reconnections
        consumerMessagesSent.remove(key);
        consumerBytesSent.remove(key);
        consumerAcks.remove(key);
        consumerFailures.remove(key);
        consumerRetries.remove(key);
        consumerDeliveryBlocked.entrySet().removeIf(entry -> entry.getKey().startsWith(key + ":"));
        consumerOffsets.remove(key);
        consumerLag.remove(key);
        consumerDeliveryLatency.remove(key);
        consumerBytesFailed.remove(key);
        consumerMessagesFailed.remove(key);
        consumerLastDeliveryTime.remove(key);
        consumerLastAckTime.remove(key);
        consumerAckTimeouts.remove(key);

        // Note: offsetGapsDetected uses topic:partition key, not group:topic, so not removed here

        log.info("OOM FIX: Removed metrics for disconnected consumer: group={}, topic={}, key={}",
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
     * Start tracking pending ACK age for a consumer group/topic.
     * Called when a batch is sent to the consumer (before ACK is received).
     * Exposes a gauge: broker.consumer.pending_ack_age_seconds
     */
    public void startPendingAck(String topic, String group) {
        String key = group + ":" + topic;

        // Store the start time
        pendingAckStartTime.computeIfAbsent(key, k -> new AtomicLong(0)).set(System.currentTimeMillis());

        // Register gauge if not already registered
        pendingAckAgeGauges.computeIfAbsent(key, k ->
            Gauge.builder("broker.consumer.pending_ack_age_seconds", () -> {
                AtomicLong startTime = pendingAckStartTime.get(k);
                if (startTime == null || startTime.get() == 0) {
                    return 0.0; // No pending ACK
                }
                return (System.currentTimeMillis() - startTime.get()) / 1000.0;
            })
            .description("Age of pending ACK in seconds (0 if no pending ACK)")
            .tag("topic", topic)
            .tag("group", group)
            .register(registry)
        );

        log.trace("Started pending ACK tracking for topic={} group={}", topic, group);
    }

    /**
     * Complete pending ACK tracking for a consumer group/topic.
     * Called when ACK is received or timeout occurs.
     * Resets the pending age gauge to 0.
     */
    public void completePendingAck(String topic, String group) {
        String key = group + ":" + topic;

        // Clear the start time (gauge will return 0)
        AtomicLong startTime = pendingAckStartTime.get(key);
        if (startTime != null) {
            startTime.set(0);
            log.trace("Completed pending ACK tracking for topic={} group={}", topic, group);
        }
    }

    public void recordLegacyBatchSent(String group, int messageCount, int topicCount) {
        String groupLabel = normalizeLegacyGroup(group);
        long currentTime = System.currentTimeMillis();

        legacyLastBatchSendTime.computeIfAbsent(groupLabel, key -> {
            AtomicLong atomic = new AtomicLong(0);
            Gauge.builder("broker.legacy.batch.last_send_time_ms", atomic, AtomicLong::get)
                    .description("Timestamp (epoch ms) of the last legacy merged batch send attempt")
                    .tag("group", groupLabel)
                    .register(registry);
            return atomic;
        }).set(currentTime);

        legacyPendingBatchStartTime.computeIfAbsent(groupLabel, key -> {
            AtomicLong atomic = new AtomicLong(0);
            legacyPendingBatchAgeGauges.computeIfAbsent(groupLabel, gaugeKey ->
                    Gauge.builder("broker.legacy.batch.pending_age_seconds", () -> {
                                AtomicLong startTime = legacyPendingBatchStartTime.get(gaugeKey);
                                if (startTime == null || startTime.get() == 0) {
                                    return 0.0;
                                }
                                return (System.currentTimeMillis() - startTime.get()) / 1000.0;
                            })
                            .description("Age of the current legacy merged batch awaiting ACK (0 if none pending)")
                            .tag("group", groupLabel)
                            .register(registry)
            );
            return atomic;
        }).set(currentTime);

        legacyPendingBatchMessages.computeIfAbsent(groupLabel, key -> {
            AtomicLong atomic = new AtomicLong(0);
            Gauge.builder("broker.legacy.batch.pending_messages", atomic, AtomicLong::get)
                    .description("Number of messages in the current pending legacy merged batch")
                    .tag("group", groupLabel)
                    .register(registry);
            return atomic;
        }).set(messageCount);

        legacyPendingBatchTopics.computeIfAbsent(groupLabel, key -> {
            AtomicLong atomic = new AtomicLong(0);
            Gauge.builder("broker.legacy.batch.pending_topics", atomic, AtomicLong::get)
                    .description("Number of topics covered by the current pending legacy merged batch")
                    .tag("group", groupLabel)
                    .register(registry);
            return atomic;
        }).set(topicCount);

        recordLegacyBatchEvent(groupLabel, "sent");
    }

    public void recordLegacyBatchAck(String group) {
        String groupLabel = normalizeLegacyGroup(group);
        long currentTime = System.currentTimeMillis();

        legacyLastBatchAckTime.computeIfAbsent(groupLabel, key -> {
            AtomicLong atomic = new AtomicLong(0);
            Gauge.builder("broker.legacy.batch.last_ack_time_ms", atomic, AtomicLong::get)
                    .description("Timestamp (epoch ms) of the last ACK for a legacy merged batch")
                    .tag("group", groupLabel)
                    .register(registry);
            return atomic;
        }).set(currentTime);

        clearLegacyPendingBatch(groupLabel);
        recordLegacyBatchEvent(groupLabel, "acked");
    }

    public void recordLegacyBatchTimeout(String group) {
        String groupLabel = normalizeLegacyGroup(group);
        clearLegacyPendingBatch(groupLabel);
        recordLegacyBatchEvent(groupLabel, "timeout");
    }

    public void clearLegacyPendingBatch(String group) {
        String groupLabel = normalizeLegacyGroup(group);
        legacyPendingBatchStartTime.computeIfAbsent(groupLabel, key -> new AtomicLong(0)).set(0);
        legacyPendingBatchMessages.computeIfAbsent(groupLabel, key -> new AtomicLong(0)).set(0);
        legacyPendingBatchTopics.computeIfAbsent(groupLabel, key -> new AtomicLong(0)).set(0);
    }

    public void recordLegacyDeliveryBlocked(String group, String reason) {
        String groupLabel = normalizeLegacyGroup(group);
        String reasonLabel = (reason == null || reason.isBlank()) ? "unknown" : reason;
        String key = groupLabel + ":" + reasonLabel;
        legacyDeliveryBlocked.computeIfAbsent(key, k ->
                Counter.builder("broker.legacy.delivery.blocked")
                        .description("Number of legacy merged-delivery attempts blocked before send")
                        .tag("group", groupLabel)
                        .tag("reason", reasonLabel)
                        .register(registry)
        ).increment();
    }

    private void recordLegacyBatchEvent(String group, String event) {
        String eventLabel = (event == null || event.isBlank()) ? "unknown" : event;
        String key = group + ":" + eventLabel;
        legacyBatchEvents.computeIfAbsent(key, k ->
                Counter.builder("broker.legacy.batch.events")
                        .description("Legacy merged batch lifecycle events")
                        .tag("group", group)
                        .tag("event", eventLabel)
                        .register(registry)
        ).increment();
    }

    private String normalizeLegacyGroup(String group) {
        return (group == null || group.isBlank()) ? "unknown" : group;
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
     * Update the count of msgKeys in sealed segments that have no ACK record in RocksDB.
     * Called by AckReconciliationScheduler after each reconciliation run.
     */
    public void updateReconciliationMissingKeys(String topic, String group, long count) {
        String key = group + ":" + topic;
        AtomicLong counter = reconciliationMissingKeys.computeIfAbsent(key, k -> {
            AtomicLong val = new AtomicLong(0);
            Gauge.builder("ack.reconciliation.missing.keys", val, AtomicLong::get)
                .description("Number of msgKeys in sealed segments with no ACK record in RocksDB")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return val;
        });
        counter.set(count);
    }

    /**
     * Report the offset range of the ACK gap for a (topic, group) pair.
     * When the pair is fully consistent (no missing keys) pass -1 for both min and max.
     * These two gauges together tell you exactly which slice of the log has not been ACKed.
     */
    public void updateReconciliationGapOffsets(String topic, String group, long minOffset, long maxOffset) {
        String key = group + ":" + topic;
        reconciliationGapMinOffset.computeIfAbsent(key, k -> {
            AtomicLong val = new AtomicLong(-1);
            Gauge.builder("ack.reconciliation.gap.min.offset", val, AtomicLong::get)
                .description("Earliest offset with no ACK record in RocksDB (-1 when fully consistent)")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return val;
        }).set(minOffset);

        reconciliationGapMaxOffset.computeIfAbsent(key, k -> {
            AtomicLong val = new AtomicLong(-1);
            Gauge.builder("ack.reconciliation.gap.max.offset", val, AtomicLong::get)
                .description("Latest offset with no ACK record in RocksDB (-1 when fully consistent)")
                .tag("topic", topic)
                .tag("group", group)
                .register(registry);
            return val;
        }).set(maxOffset);
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

    // ── Compaction metrics ────────────────────────────────────────────────────

    /** Increment the global compaction run counter (called once per full sweep). */
    public void recordCompactionRun() {
        compactionRunsTotal.increment();
    }

    /**
     * Record a successful per-topic compaction run and update all associated counters/gauges.
     *
     * @param topic              topic that was compacted
     * @param recordsRemoved     total records physically deleted (superseded + expired tombstones)
     * @param tombstonesRemoved  subset of recordsRemoved that were DELETE tombstones
     * @param bytesRead          total bytes read from all candidate segments before compaction
     * @param bytesWritten       total bytes written to the resulting compacted segment
     * @param bytesReclaimed     estimated bytes freed (bytesRead − bytesWritten approximation)
     * @param segmentsReplaced   number of source segments replaced by the compacted output
     */
    public void recordCompactionTopicRun(
            String topic,
            int recordsRemoved,
            int tombstonesRemoved,
            long bytesRead,
            long bytesWritten,
            long bytesReclaimed,
            int segmentsReplaced) {

        // Per-topic run counter (status=success)
        compactionRunsByTopic.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.topic.runs.total")
                        .description("Per-topic successful compaction runs")
                        .tag("topic", t)
                        .tag("status", "success")
                        .register(registry)
        ).increment();

        // All per-topic counters are registered unconditionally on first run so that
        // the topic label appears in Prometheus immediately, even when nothing was removed.
        // Grafana's $topic variable and every panel depend on these labels existing.
        Counter recRemoved = compactionRecordsRemoved.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.records.removed")
                        .description("Records physically removed during compaction (superseded + expired tombstones)")
                        .tag("topic", t)
                        .register(registry));
        if (recordsRemoved > 0) recRemoved.increment(recordsRemoved);

        Counter tombRemoved = compactionTombstonesRemoved.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.tombstones.removed")
                        .description("Expired DELETE tombstones physically removed during compaction")
                        .tag("topic", t)
                        .register(registry));
        if (tombstonesRemoved > 0) tombRemoved.increment(tombstonesRemoved);

        Counter bRead = compactionBytesRead.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.bytes.read")
                        .description("Bytes read from source segments during compaction")
                        .tag("topic", t)
                        .baseUnit("bytes")
                        .register(registry));
        if (bytesRead > 0) bRead.increment(bytesRead);

        Counter bWritten = compactionBytesWritten.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.bytes.written")
                        .description("Bytes written to compacted output segment")
                        .tag("topic", t)
                        .baseUnit("bytes")
                        .register(registry));
        if (bytesWritten > 0) bWritten.increment(bytesWritten);

        Counter bReclaimed = compactionBytesReclaimed.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.bytes.reclaimed")
                        .description("Estimated bytes reclaimed (freed) during compaction")
                        .tag("topic", t)
                        .baseUnit("bytes")
                        .register(registry));
        if (bytesReclaimed > 0) bReclaimed.increment(bytesReclaimed);

        Counter segsReplaced = compactionSegmentsReplaced.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.segments.replaced")
                        .description("Number of segments replaced by compacted output per run")
                        .tag("topic", t)
                        .register(registry));
        if (segmentsReplaced > 0) segsReplaced.increment(segmentsReplaced);

        // Last-run timestamp gauge (epoch seconds)
        long nowSeconds = System.currentTimeMillis() / 1000;
        AtomicLong tsHolder = compactionLastRunTimestamp.computeIfAbsent(topic, t -> {
            AtomicLong atomic = new AtomicLong(0);
            compactionLastRunGauges.computeIfAbsent(t, gk ->
                    Gauge.builder("broker.compaction.last.run.timestamp", atomic, AtomicLong::get)
                            .description("Epoch seconds of the last successful compaction run for this topic")
                            .tag("topic", t)
                            .register(registry)
            );
            return atomic;
        });
        tsHolder.set(nowSeconds);
    }

    /**
     * Record a per-topic compaction error (used when compacting a topic throws an exception).
     */
    public void recordCompactionError(String topic) {
        compactionErrorsByTopic.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.topic.runs.total")
                        .description("Per-topic failed compaction runs")
                        .tag("topic", t)
                        .tag("status", "error")
                        .register(registry)
        ).increment();
    }

    /**
     * Signal that compaction is actively running for a topic (set gauge to 1).
     * Must be followed by a corresponding {@link #markCompactionComplete(String)} call.
     */
    public void markCompactionActive(String topic) {
        AtomicLong flag = compactionActiveFlag.computeIfAbsent(topic, t -> {
            AtomicLong atomic = new AtomicLong(0);
            compactionActiveGauges.computeIfAbsent(t, gk ->
                    Gauge.builder("broker.compaction.active", atomic, AtomicLong::get)
                            .description("1 when compaction is actively running for this topic, 0 when idle")
                            .tag("topic", t)
                            .register(registry)
            );
            return atomic;
        });
        flag.set(1L);
    }

    /**
     * Signal that compaction has finished for a topic (set gauge to 0).
     */
    public void markCompactionComplete(String topic) {
        AtomicLong flag = compactionActiveFlag.get(topic);
        if (flag != null) {
            flag.set(0L);
        }
    }

    // ── Legacy per-topic helpers (kept for backward compatibility) ────────────

    /** @deprecated Use {@link #recordCompactionTopicRun} instead. */
    @Deprecated
    public void recordCompactionRecordsRemoved(String topic, int count) {
        compactionRecordsRemoved.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.records.removed")
                        .description("Records physically removed during compaction")
                        .tag("topic", t)
                        .register(registry)
        ).increment(count);
    }

    /** @deprecated Use {@link #recordCompactionTopicRun} instead. */
    @Deprecated
    public void recordCompactionBytesReclaimed(String topic, long bytes) {
        compactionBytesReclaimed.computeIfAbsent(topic, t ->
                Counter.builder("broker.compaction.bytes.reclaimed")
                        .description("Bytes reclaimed during compaction")
                        .tag("topic", t)
                        .register(registry)
        ).increment(bytes);
    }

    public Timer.Sample startCompactionTimer() {
        return Timer.start(registry);
    }

    public void stopCompactionTimer(Timer.Sample sample) {
        sample.stop(compactionDuration);
    }
}
