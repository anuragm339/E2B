package com.messaging.broker.consumer;

import com.messaging.broker.legacy.LegacyConsumerDeliveryManager;
import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.broker.metrics.DataRefreshMetrics;
import com.messaging.broker.refresh.DataRefreshManager;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.mmap.MMapStorageEngine;
import com.messaging.storage.segment.Segment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import io.netty.channel.FileRegion;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Registry for TCP-connected remote consumers
 */
@Singleton
public class RemoteConsumerRegistry {
    private static final Logger log = LoggerFactory.getLogger(RemoteConsumerRegistry.class);

    private final StorageEngine storage;
    private final NetworkServer server;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final DataRefreshMetrics dataRefreshMetrics;
    private final DeliveryStateStore deliveryStateStore; // OOM FIX: Added to clean up state on disconnect
    private volatile DataRefreshManager dataRefreshManager; // Lazy injection to avoid circular dependency
    private volatile AdaptiveBatchDeliveryManager adaptiveDeliveryManager; // Lazy injection to avoid circular dependency
    private final LegacyConsumerDeliveryManager legacyDeliveryManager; // For multi-topic merge delivery to legacy clients
    private final ObjectMapper objectMapper;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ExecutorService storageExecutor; // Separate executor for storage operations to prevent deadlock
    private final long maxMessageSizePerConsumer;
    private final long ackTimeoutMs; // Configurable ACK timeout
    private static final int MAX_CONSECUTIVE_FAILURES = 10; // Max retries before giving up

    // Map: "clientId:topic" -> RemoteConsumer (composite key to support multiple topic subscriptions per client)
    private final Map<String, RemoteConsumer> consumers;

    // ACK-based flow control tracking
    // Map: "clientId:topic" -> in-flight delivery status (true = delivery in progress)
    private final Map<String, AtomicBoolean> inFlightDeliveries;

    // Map: "clientId:topic:pending" -> next offset to commit on ACK
    private final Map<String, Long> pendingOffsets;

    // Map: "clientId:topic:group:pending" -> timestamp when batch was sent (for ACK latency tracking)
    private final Map<String, Long> batchSendTimestamps;
    // Map: "clientId:topic:group:pending" -> timeout task (canceled on ACK)
    private final Map<String, ScheduledFuture<?>> pendingTimeouts;

    // B2-6 fix: retain group mapping even after consumer unregisters so that a late ACK
    // (arriving after unregistration) can still persist the offset to the offset tracker.
    // Key: "clientId:topic", Value: group name
    private final Map<String, String> deliveryKeyToGroup;

    @Inject
    public RemoteConsumerRegistry(StorageEngine storage, NetworkServer server,
                                  ConsumerOffsetTracker offsetTracker, BrokerMetrics metrics,
                                  DataRefreshMetrics dataRefreshMetrics, DeliveryStateStore deliveryStateStore,
                                  LegacyConsumerDeliveryManager legacyDeliveryManager,
                                  @Value("${broker.consumer.max-message-size-per-consumer}") long maxMessageSizePerConsumer,
                                  @Value("${broker.consumer.ack-timeout}") long ackTimeoutMs) {
        this.storage = storage;
        this.server = server;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.dataRefreshMetrics = dataRefreshMetrics;
        this.deliveryStateStore = deliveryStateStore;
        this.legacyDeliveryManager = legacyDeliveryManager;
        this.maxMessageSizePerConsumer = maxMessageSizePerConsumer;
        this.ackTimeoutMs = ackTimeoutMs;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule()); // Register JSR310 module for Java 8 date/time
        // Reduced thread pool: use half of available cores (min 2) to lower CPU usage
        int schedulerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        this.scheduler = new ScheduledThreadPoolExecutor(schedulerThreads, runnable -> {
            Thread t = new Thread(runnable);
            t.setName("RemoteConsumerRegistry-" + t.getId());
            return t;
        });
        // Remove canceled timeout tasks from queue to prevent buildup.
        this.scheduler.setRemoveOnCancelPolicy(true);
        // Separate thread pool for storage operations to prevent deadlock
        // Reduced to 1x CPU cores to minimize memory usage (was 2x)
        this.storageExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), runnable -> {
            Thread t = new Thread(runnable);
            t.setName("StorageReader-" + t.getId());
            return t;
        });
        this.consumers = new ConcurrentHashMap<>();
        this.inFlightDeliveries = new ConcurrentHashMap<>();
        this.pendingOffsets = new ConcurrentHashMap<>();
        this.batchSendTimestamps = new ConcurrentHashMap<>();
        this.pendingTimeouts = new ConcurrentHashMap<>();
        this.deliveryKeyToGroup = new ConcurrentHashMap<>();
        log.warn("RemoteConsumerRegistry initialized with maxMessageSize={}bytes per consumer (size-only batching)",
                 maxMessageSizePerConsumer);
        log.warn("⚙️ ACK_TIMEOUT configured: {}ms ({}s, {}min)",
                 ackTimeoutMs, ackTimeoutMs/1000, ackTimeoutMs/60000);
    }

    /**
     * Lazy setter for DataRefreshManager to avoid circular dependency
     */
    public void setDataRefreshManager(DataRefreshManager dataRefreshManager) {
        this.dataRefreshManager = dataRefreshManager;
    }

    public void setAdaptiveBatchDeliveryManager(AdaptiveBatchDeliveryManager adaptiveDeliveryManager) {
        this.adaptiveDeliveryManager = adaptiveDeliveryManager;
        // Start legacy delivery scheduler once adaptive manager is set
        startLegacyDeliveryScheduler();
    }

    /**
     * Start periodic delivery scheduler for legacy clients.
     * Legacy clients use multi-topic merge, so they are not registered with AdaptiveBatchDeliveryManager.
     * This scheduler checks for legacy clients and delivers merged batches.
     */
    private void startLegacyDeliveryScheduler() {
        // Schedule periodic delivery check for legacy clients (every 100ms for low latency)
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                deliverToLegacyClients();
            } catch (Exception e) {
                log.error("Error in legacy delivery scheduler", e);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);

        log.info("Started legacy delivery scheduler (interval: 100ms)");
    }

    /**
     * Check all legacy clients and deliver merged batches if needed.
     * Groups legacy consumers by clientId and delivers one merged batch per client.
     */
    private void deliverToLegacyClients() {
        // Group legacy consumers by clientId
        Map<String, List<RemoteConsumer>> legacyClientGroups = consumers.values().stream()
                .filter(c -> c.isLegacy)
                .collect(Collectors.groupingBy(c -> c.clientId));

        if (!legacyClientGroups.isEmpty()) {
            log.warn("🔵 SCHEDULER: deliverToLegacyClients running - found {} legacy clients",
                    legacyClientGroups.size());
        }

        // Deliver to each legacy client
        for (Map.Entry<String, List<RemoteConsumer>> entry : legacyClientGroups.entrySet()) {
            String clientId = entry.getKey();
            List<RemoteConsumer> clientConsumers = entry.getValue();

            if (clientConsumers.isEmpty()) {
                continue;
            }

            // All consumers for a legacy client share the same group (serviceName)
            String consumerGroup = clientConsumers.get(0).group;

            // Attempt delivery (with built-in flow control checks)
            deliverMergedBatchToLegacy(clientId, consumerGroup, maxMessageSizePerConsumer);
        }
    }

    /**
     * Register a remote consumer when it subscribes
     */
    /**
     * Register a consumer for message delivery.
     * @return true if this is a new registration, false if re-registering an existing key (duplicate SUBSCRIBE)
     */
    public boolean registerConsumer(String clientId, String topic, String group) {
        return registerConsumer(clientId, topic, group, false);
    }

    public boolean registerConsumer(String clientId, String topic, String group, boolean isLegacy) {
        RemoteConsumer consumer = new RemoteConsumer(clientId, topic, group, isLegacy);

        // Load persisted offset from property file - ONLY source of truth
        String consumerId = group + ":" + topic;
        long startOffset = offsetTracker.getOffset(consumerId);

        consumer.setCurrentOffset(startOffset);

        // Initialize consumer metrics
        metrics.updateConsumerOffset(clientId, topic, group, startOffset);
        metrics.updateConsumerLag(clientId, topic, group, 0);

        // MULTI-GROUP FIX: Use composite key clientId:topic:group to support multiple groups per topic
        // This allows multiple groups to subscribe to the same topic on one connection
        // B2-3 fix: detect re-registration before put() so BrokerService can avoid double-counting activeConsumers metric
        String consumerKey = clientId + ":" + topic + ":" + group;
        boolean isNew = !consumers.containsKey(consumerKey);
        consumers.put(consumerKey, consumer);
        // Note: deliveryKeyToGroup no longer needed since group is in the key

        // For legacy consumers, DO NOT register with adaptive delivery manager
        // They will be handled by grouped delivery (multi-topic merge)
        if (!isLegacy && adaptiveDeliveryManager != null) {
            adaptiveDeliveryManager.registerConsumer(consumer);
            log.info("Triggered adaptive delivery for new consumer: {}:{}", clientId, topic);
        } else if (isLegacy) {
            log.info("Legacy consumer registered: {}:{} (will use grouped multi-topic delivery)", clientId, topic);
        }

        log.debug("Registered remote consumer: consumerKey={}, clientId={}, topic={}, group={}, startOffset={}, isNew={}, isLegacy={}",
                 consumerKey, clientId, topic, group, startOffset, isNew, isLegacy);
        return isNew;
    }

    /**
     * Unregister consumer when it disconnects
     * Removes all topic subscriptions for this client
     *
     * @return Number of topic subscriptions removed
     */
    public int unregisterConsumer(String clientId) {
        // Find and remove all consumers for this clientId (all topics)
        List<String> keysToRemove = new ArrayList<>();
        for (Map.Entry<String, RemoteConsumer> entry : consumers.entrySet()) {
            if (entry.getValue().clientId.equals(clientId)) {
                keysToRemove.add(entry.getKey());
            }
        }

        int removedCount = 0;
        for (String key : keysToRemove) {
            RemoteConsumer consumer = consumers.remove(key);
            if (consumer != null) {
                if (consumer.deliveryTask != null) {
                    consumer.deliveryTask.cancel(false);
                }
                // B1-6 fix: clear stale pendingOffsets entry so Gate 2 does not block
                // the same consumer if it reconnects and re-registers before the ACK
                // timeout fires (which could take up to 30 minutes).
                String pendingKey = key + ":pending";
                Long stalePending = pendingOffsets.remove(pendingKey);
                if (stalePending != null) {
                    log.info("Cleared stale pending offset {} for key={} on unregister", stalePending, pendingKey);
                }
                ScheduledFuture<?> staleTimeout = pendingTimeouts.remove(pendingKey);
                if (staleTimeout != null) {
                    staleTimeout.cancel(false);
                }

                // B1-8 fix: Clean up inFlightDeliveries to prevent memory leak
                // Remove all inFlightDeliveries entries for this clientId
                // Key format: "clientId:topic:group", so we match prefix "clientId:"
                String deliveryKeyPrefix = clientId + ":";
                int removedInflight = 0;
                Iterator<String> iterator = inFlightDeliveries.keySet().iterator();
                while (iterator.hasNext()) {
                    String inflightKey = iterator.next();
                    if (inflightKey.startsWith(deliveryKeyPrefix)) {
                        iterator.remove();
                        removedInflight++;
                    }
                }
                if (removedInflight > 0) {
                    log.info("B1-8 fix: Cleared {} inFlightDeliveries entries for clientId={} on unregister",
                             removedInflight, clientId);
                }

                // OOM FIX: Clean up deliveryKeyToGroup to prevent memory leak
                // Key format: "clientId:topic", so we match prefix "clientId:"
                int removedDeliveryKeys = 0;
                Iterator<String> deliveryKeyIterator = deliveryKeyToGroup.keySet().iterator();
                while (deliveryKeyIterator.hasNext()) {
                    String deliveryKey = deliveryKeyIterator.next();
                    if (deliveryKey.startsWith(deliveryKeyPrefix)) {
                        deliveryKeyIterator.remove();
                        removedDeliveryKeys++;
                    }
                }
                if (removedDeliveryKeys > 0) {
                    log.info("OOM FIX: Cleared {} deliveryKeyToGroup entries for clientId={} on unregister",
                             removedDeliveryKeys, clientId);
                }

                // Clean up metrics (OOM FIX: re-enabled cleanup in BrokerMetrics)
                metrics.removeConsumerMetrics(clientId, consumer.topic, consumer.group);

                // OOM FIX: Clean up data refresh timing metrics for this consumer
                dataRefreshMetrics.removeConsumerTimingData(consumer.topic, clientId);

                log.info("Unregistered remote consumer: consumerKey={}, clientId={}, topic={}, group={}",
                         key, clientId, consumer.topic, consumer.group);
                removedCount++;
            }
        }

        // OOM FIX: Clean up delivery state for this clientId (all topics)
        // Called once after processing all topic subscriptions for this client
        deliveryStateStore.removeConsumerState(clientId);

        log.info("Unregistered {} topic subscriptions for client: {}", removedCount, clientId);
        return removedCount;
    }

    /**
     * Get all registered consumers
     * Used by AdaptiveBatchDeliveryManager to schedule delivery tasks
     *
     * @return List of all registered consumers
     */
    public List<RemoteConsumer> getAllConsumers() {
        return new ArrayList<>(consumers.values());
    }

    /**
     * Deliver batch to consumer (for AdaptiveBatchDeliveryManager)
     * Returns true if data was found and delivered, false if skipped
     *
     * @param consumer Consumer to deliver to
     * @param batchSizeBytes Max batch size in bytes
     * @return true if data delivered, false if no data or delivery blocked
     */
    public boolean deliverBatch(RemoteConsumer consumer, long batchSizeBytes) {
        // MULTI-GROUP FIX: Include group in delivery key
        String deliveryKey = consumer.clientId + ":" + consumer.topic + ":" + consumer.group;
        String pendingKey = deliveryKey + ":pending";

        AtomicBoolean inFlight = inFlightDeliveries.computeIfAbsent(
                deliveryKey, k -> new AtomicBoolean(false)
        );

        // Gate 1: Check in-flight (per topic:consumer)
        if (!inFlight.compareAndSet(false, true)) {
            log.debug("deliverBatch: Gate 1 BLOCKED (in-flight) for {}", deliveryKey);
            return false;  // Already delivering to this consumer:topic
        }

        // Gate 2: Check pending ACK
        if (pendingOffsets.containsKey(pendingKey)) {
            log.debug("deliverBatch: Gate 2 BLOCKED (pending ACK) for {}, pendingOffset={}",
                     deliveryKey, pendingOffsets.get(pendingKey));
            inFlight.set(false);
            return false;  // Waiting for ACK
        }

        // Gate 3: Check maximum consecutive failures
        if (consumer.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
            log.error("Consumer {} has exceeded max consecutive failures ({}), unregistering",
                     deliveryKey, MAX_CONSECUTIVE_FAILURES);
            inFlight.set(false);
            unregisterConsumer(consumer.clientId);
            return false;  // Consumer has failed too many times
        }

        // Gate 4: Check exponential backoff after failures
        long backoffDelay = consumer.getBackoffDelay();
        if (backoffDelay > 0) {
            long timeSinceLastFailure = System.currentTimeMillis() - consumer.lastFailureTime;
            if (timeSinceLastFailure < backoffDelay) {
                long remainingDelay = backoffDelay - timeSinceLastFailure;
                log.debug("DEBUG deliverBatch: Gate 4 BLOCKED (backoff) for {}, consecutiveFailures={}, remainingDelay={}ms",
                         deliveryKey, consumer.consecutiveFailures, remainingDelay);
                inFlight.set(false);
                return false;  // Still in backoff period
            }
        }

        log.debug("deliverBatch: Gates passed, proceeding with batch read for {}", deliveryKey);

        // B2-2 fix: record retry metric when a previously-failed consumer is being retried
        if (consumer.consecutiveFailures > 0) {
            metrics.recordConsumerRetry(consumer.clientId, consumer.topic, consumer.group);
        }

        // Gate 3 removed: Parallel topic delivery now allowed

        long startOffset = consumer.getCurrentOffset();
        Timer.Sample readSample = null;
        Timer.Sample deliverySample = null;
        Segment.BatchFileRegion batch = null;

        try {
            /* ================= STORAGE READ METRICS ================= */
            readSample = metrics.startStorageReadTimer();

            // B1-3 fix: storage is injected as StorageEngine (interface); hard-casting to
            // FileChannelStorageEngine throws ClassCastException when MMapStorageEngine is used.
            // Use instanceof to dispatch to the correct concrete type.
            final long capturedOffset = startOffset;
            batch = storageExecutor.submit(() -> {
                if (storage instanceof com.messaging.storage.filechannel.FileChannelStorageEngine) {
                    return ((com.messaging.storage.filechannel.FileChannelStorageEngine) storage)
                            .getZeroCopyBatch(consumer.topic, 0, capturedOffset, batchSizeBytes);
                } else if (storage instanceof com.messaging.storage.mmap.MMapStorageEngine) {
                    return ((com.messaging.storage.mmap.MMapStorageEngine) storage)
                            .getZeroCopyBatch(consumer.topic, 0, capturedOffset, batchSizeBytes);
                } else {
                    throw new IllegalStateException("StorageEngine implementation does not support " +
                            "getZeroCopyBatch(): " + storage.getClass().getName());
                }
            }).get(10, TimeUnit.MINUTES);

            metrics.stopStorageReadTimer(readSample);
            metrics.recordStorageRead();

            log.debug("deliverBatch: Batch read complete for {}, recordCount={}, fileRegion={}",
                     deliveryKey, batch.recordCount, (batch.fileRegion != null ? "present" : "NULL"));

            if (batch.recordCount == 0 || batch.fileRegion == null) {
                log.debug("deliverBatch: EMPTY BATCH (recordCount={}, fileRegion={}) for {}, startOffset={}",
                         batch.recordCount, (batch.fileRegion != null ? "present" : "NULL"), deliveryKey, startOffset);
                inFlight.set(false);
                return false;  // No data available
            }

            /* ================= BATCH VISIBILITY ================= */
            metrics.recordBatchSize(batch.recordCount);

            /* ================= OFFSET RESERVATION ================= */
            // Store original offset BEFORE reservation (for timeout revert)
            long originalOffset = startOffset;
            long nextOffset = batch.lastOffset + 1;
            consumer.setCurrentOffset(nextOffset);
            pendingOffsets.put(pendingKey, nextOffset);

            /* ================= DELIVERY METRICS ================= */
            deliverySample = metrics.startConsumerDeliveryTimer();

            sendBatchToConsumer(consumer, batch, startOffset);

            // Start tracking pending ACK age for monitoring
            metrics.startPendingAck(consumer.topic, consumer.group);

            /* ================= ACK TIMEOUT SETUP ================= */
            // B2-4 fix: schedule timeout AFTER successful send so it only fires for
            // batches the consumer actually received (not for send failures).
            // Previously the timeout was scheduled before sendBatchToConsumer(), so a
            // send failure left a 30-min timer that would incorrectly revert the offset
            // for a batch the consumer never received.
            //
            // Now uses configurable timeout from application.yml (broker.consumer.ack-timeout)
            // instead of hardcoded 30 minutes. Recommended: 30-60s baseline, scale by batch size.
            long pendingStartTime = System.currentTimeMillis();
            batchSendTimestamps.put(pendingKey, pendingStartTime); // Track send time for ACK latency calculation
            log.warn("⏱️ BATCH_SENT to {} at T=0ms, startOffset={}, recordCount={}, bytes={}, ackTimeoutConfigured={}ms, pendingOffsetsSize={}, batchTimestampsSize={}",
                     deliveryKey, startOffset, batch.recordCount, batch.totalBytes, ackTimeoutMs,
                     pendingOffsets.size(), batchSendTimestamps.size());
            ScheduledFuture<?> timeoutFuture = scheduler.schedule(() -> {
                if (pendingOffsets.remove(pendingKey) != null) {
                    batchSendTimestamps.remove(pendingKey); // Clean up timestamp on timeout
                    long pendingDuration = System.currentTimeMillis() - pendingStartTime;
                    log.warn("⏰ ACK_TIMEOUT for {} after {}ms, reverting offset from {} to {}, pendingOffsetsSize={}, batchTimestampsSize={}",
                             deliveryKey, pendingDuration, nextOffset, originalOffset,
                             pendingOffsets.size(), batchSendTimestamps.size());

                    // REVERT consumer offset to prevent delivery gap
                    consumer.setCurrentOffset(originalOffset);
                    inFlight.set(false);

                    metrics.recordAckTimeout(consumer.topic, consumer.group);  // B2-7 fix: pass group
                    metrics.completePendingAck(consumer.topic, consumer.group);  // Clear pending ACK age metric
                }
                pendingTimeouts.remove(pendingKey);
            }, ackTimeoutMs, TimeUnit.MILLISECONDS);
            ScheduledFuture<?> oldTimeout = pendingTimeouts.put(pendingKey, timeoutFuture);
            if (oldTimeout != null) {
                oldTimeout.cancel(false);
            }

            metrics.stopConsumerDeliveryTimer(
                    deliverySample,
                    consumer.clientId,
                    consumer.topic,
                    consumer.group
            );

            /* ================= DATA REFRESH METRICS (if in refresh mode) ================= */
            log.debug("DEBUG: Checking data refresh metrics - dataRefreshManager={}, topic={}",
                     (dataRefreshManager != null ? "available" : "NULL"), consumer.topic);

            if (dataRefreshManager != null) {
                String refreshId = dataRefreshManager.getRefreshIdForTopic(consumer.topic);
                String refreshType = dataRefreshManager.getRefreshTypeForTopic(consumer.topic);
                log.debug("refreshId for topic {} = {}, refreshType = {}", consumer.topic, refreshId, refreshType);

                if (refreshId != null && refreshType != null) {
                    log.debug("Recording data refresh metrics - topic={}, group={}, bytes={}, messages={}, refreshId={}, refreshType={}",
                             consumer.topic, consumer.group, batch.totalBytes, batch.recordCount, refreshId, refreshType);

                    // Record data transferred during refresh
                    dataRefreshMetrics.recordDataTransferred(
                            consumer.topic,
                            consumer.group,  // Using group instead of clientId for stable identifier
                            batch.totalBytes,
                            batch.recordCount,
                            refreshId,
                            refreshType
                    );

                    log.debug("Data refresh metrics recorded successfully");
                } else {
                    log.debug("refreshId or refreshType is NULL for topic {} - not in refresh mode", consumer.topic);
                }
            } else {
                log.debug("dataRefreshManager is NULL - cannot record data refresh metrics");
            }

            // Reset failure counter on successful delivery
            consumer.resetFailures();

            // B2-1 fix: update consumer lag metric after each successful delivery.
            // lag = storageHead - newConsumerOffset (after offset was advanced to nextOffset)
            try {
                long storageHead = storage.getCurrentOffset(consumer.topic, 0);
                long lag = Math.max(0, storageHead - consumer.getCurrentOffset());
                metrics.updateConsumerLag(consumer.clientId, consumer.topic, consumer.group, lag);
            } catch (Exception lagEx) {
                log.debug("Could not update consumer lag metric for {}: {}", deliveryKey, lagEx.getMessage());
            }

            return true;  // Data delivered successfully

        } catch (Exception e) {
            log.error("Delivery failed for {}", deliveryKey, e);
            // B2-2 fix: record failure metric so broker_consumer_failures_total is non-zero
            metrics.recordConsumerFailure(consumer.clientId, consumer.topic, consumer.group);
            inFlight.set(false);

            // Revert offset reservation on error
            consumer.setCurrentOffset(startOffset);

            // Record failure for exponential backoff
            consumer.recordFailure();

            // ✅ CRITICAL FIX (Phase 5A): Only remove pending offset for PERMANENT failures
            // For transient failures (disconnect, backpressure), KEEP pending offset
            // This prevents rapid retries to disconnected consumers
            boolean isPermanentFailure = consumer.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES;

            if (isPermanentFailure) {
                // Permanent failure - remove pending offset and unregister
                pendingOffsets.remove(pendingKey);
                log.error("Permanent failure for {} (consecutiveFailures={}), removing pending offset and unregistering",
                         deliveryKey, MAX_CONSECUTIVE_FAILURES);
                unregisterConsumer(consumer.clientId);
            } else {
                // Transient failure - KEEP pending offset
                // Gate 2 will continue blocking until:
                // - Consumer reconnects and sends ACK, OR
                // - ACK timeout (30 min) fires and removes pending offset
                log.warn("Transient failure for {}, KEEPING pending offset to prevent rapid retries. " +
                        "Will wait for ACK or timeout (30 min). consecutiveFailures={}",
                        deliveryKey, consumer.consecutiveFailures);
            }

            // Record failed transfer metrics (bytes and messages that didn't make it to consumer)
            if (batch != null) {
                metrics.recordConsumerTransferFailed(
                    consumer.clientId,
                    consumer.topic,
                    consumer.group,
                    batch.recordCount,
                    batch.totalBytes
                );
                log.warn("Recorded failed transfer: topic={}, group={}, messages={}, bytes={}, " +
                        "consecutiveFailures={}, nextBackoff={}ms, isPermanentFailure={}",
                        consumer.topic, consumer.group, batch.recordCount, batch.totalBytes,
                        consumer.consecutiveFailures, consumer.getBackoffDelay(), isPermanentFailure);
            }

            return false;  // Delivery failed
        }
    }


    /**
     * Send a batch of messages to consumer using Kafka-style zero-copy (sendfile syscall)
     * This eliminates heap allocations and uses OS kernel zero-copy
     */
    private void sendBatchToConsumer(RemoteConsumer consumer, Segment.BatchFileRegion batchRegion, long startOffset)
            throws Exception {

        if (batchRegion.fileRegion == null) {
            log.debug("No data to send for consumer {}", consumer.clientId);
            return;
        }

        // Step 1: Create and send header message with batch metadata
        // FIX: Include both topic AND group so consumer can ACK with correct group identifier
        byte[] topicBytes = consumer.topic.getBytes(StandardCharsets.UTF_8);
        int topicLen = topicBytes.length;
        byte[] groupBytes = consumer.group.getBytes(StandardCharsets.UTF_8);
        int groupLen = groupBytes.length;

        // MULTI-GROUP FIX: Header format [recordCount:4][totalBytes:8][topicLen:4][topic:var][groupLen:4][group:var]
        // Group is REQUIRED for ACK routing when multiple groups share one connection
        ByteBuffer headerBuffer = ByteBuffer.allocate(12 + 4 + topicLen + 4 + groupLen);
        headerBuffer.putInt(batchRegion.recordCount);
        headerBuffer.putLong(batchRegion.totalBytes);
        headerBuffer.putInt(topicLen);
        headerBuffer.put(topicBytes);
        headerBuffer.putInt(groupLen);
        headerBuffer.put(groupBytes);
        headerBuffer.flip();

        byte[] header = new byte[12 + 4 + topicLen + 4 + groupLen];
        headerBuffer.get(header);

        // Send header using BATCH_HEADER message type
        BrokerMessage headerMsg = new BrokerMessage(
            BrokerMessage.MessageType.BATCH_HEADER,
            System.currentTimeMillis(),
            header
        );

        log.debug("Sending BATCH_HEADER to consumer {}: topic={}, group={}, recordCount={}, totalBytes={}",
                 consumer.clientId, consumer.topic, consumer.group, batchRegion.recordCount, batchRegion.totalBytes);

        long timeoutSeconds = 1 + (batchRegion.totalBytes / (1024 * 1024) * 10);
        // Timeout in seconds (1s base + 10s per MB, e.g., 2MB = 21 seconds)
        server.send(consumer.clientId, headerMsg).get(timeoutSeconds, TimeUnit.SECONDS);

        // Step 2: Send FileRegion for true zero-copy transfer (Kafka-style sendfile)
        // This uses OS sendfile() syscall - data goes from file → kernel → socket
        // NO user-space copies, NO heap allocations!
        log.debug("Sending zero-copy FileRegion: position={}, count={}, records={}, bytes={}, consumer={}, topic={}, startOffset={}",
                 batchRegion.fileRegion.position(), batchRegion.fileRegion.count(),
                 batchRegion.recordCount, batchRegion.totalBytes, consumer.clientId, consumer.topic, startOffset);

        // Timeout scales with batch size: 1s base + 10s per MB (e.g., 2MB = 21 seconds)
        log.debug("Sending FileRegion with timeout of {}s for {} bytes", timeoutSeconds, batchRegion.totalBytes);

        try {
            server.sendFileRegion(consumer.clientId, batchRegion.fileRegion)
                  .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            // CRITICAL: BATCH_HEADER was already sent — consumer decoder has transitioned to
            // READING_ZERO_COPY_BATCH and is waiting for expectedTotalBytes that will never arrive.
            // Close the connection so the consumer reconnects and decoder resets to initial state.
            log.error("FileRegion send failed after BATCH_HEADER was already sent for consumer {} topic {}." +
                      " Closing connection to reset consumer decoder state.",
                      consumer.clientId, consumer.topic, e);
            server.closeConnection(consumer.clientId);
            throw e;
        }

        log.debug("Sent zero-copy batch to consumer {}: recordCount={}, bytes={}, startOffset={}, lastOffset={}",
                 consumer.clientId, batchRegion.recordCount, batchRegion.totalBytes, startOffset, batchRegion.lastOffset);

        // Record metrics for messages and bytes sent (global counters)
        metrics.recordBatchMessagesSent(batchRegion.recordCount, batchRegion.totalBytes);

        // Record per-consumer metrics (efficient batch version)
        metrics.recordConsumerBatchSent(consumer.clientId, consumer.topic, consumer.group,
                                       batchRegion.recordCount, batchRegion.totalBytes);

        // Update last successful delivery timestamp for stuck detection
        metrics.updateConsumerLastDeliveryTime(consumer.clientId, consumer.topic, consumer.group);
    }

    /**
     * Handle BATCH_ACK for legacy merged batch.
     * Updates offsets for ALL topics that were in the merged batch.
     *
     * @param clientId The client ID that sent the ACK
     * @param group The consumer group (serviceName for legacy clients)
     */
    public void handleLegacyBatchAck(String clientId, String group) {
        String pendingKey = clientId + ":legacy-batch:pending";

        // Calculate ACK latency
        long ackReceiveTime = System.currentTimeMillis();
        Long sendTime = batchSendTimestamps.remove(pendingKey);
        long ackLatencyMs = sendTime != null ? (ackReceiveTime - sendTime) : -1;

        Long timestamp = pendingOffsets.remove(pendingKey);
        ScheduledFuture<?> timeoutFuture = pendingTimeouts.remove(pendingKey);
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }

        if (timestamp == null) {
            log.warn("⚠️ Legacy batch ACK with no pending offset: clientId={}, group={} (likely a late ACK after timeout). ACK_LATENCY={}ms.",
                    clientId, group, ackLatencyMs);
            return;
        }

        log.info("✅ Legacy batch ACK_RECEIVED for clientId={}, group={} at T={}ms",
                clientId, group, ackLatencyMs);

        // Get all legacy consumers for this client to find which topics to update
        List<RemoteConsumer> legacyConsumers = getLegacyConsumersForClient(clientId);
        if (legacyConsumers.isEmpty()) {
            log.warn("No legacy consumers found for ACK: clientId={}, group={}", clientId, group);
            return;
        }

        // We need the MergedBatch to know the max offset per topic
        // For now, we'll get it from the offsetTracker's current state
        // TODO: Store MergedBatch reference in pending state for accurate offset tracking

        // Update offsets for all topics using LegacyConsumerDeliveryManager
        // Get the topics list
        List<String> topics = legacyConsumers.stream()
                .map(c -> c.topic)
                .collect(Collectors.toList());

        try {
            // Build a fresh batch to get the max offsets (this is a temporary solution)
            // In production, we should store the MergedBatch in pending state
            MergedBatch batch = legacyDeliveryManager.buildMergedBatch(topics, group, 1); // Just peek at offsets

            // Use the handleMergedBatchAck method from LegacyConsumerDeliveryManager
            legacyDeliveryManager.handleMergedBatchAck(group, batch);

            log.info("Legacy batch ACK committed for clientId={}, group={}, topics={}",
                    clientId, group, batch.getMaxOffsetPerTopic());

            // Update metrics for each topic
            for (Map.Entry<String, Long> entry : batch.getMaxOffsetPerTopic().entrySet()) {
                String topic = entry.getKey();
                long offset = entry.getValue();
                metrics.updateConsumerOffset(clientId, topic, group, offset);
                metrics.updateConsumerLastAckTime(clientId, topic, group);
                metrics.recordConsumerAck(clientId, topic, group);
                metrics.completePendingAck(topic, group);

                // Calculate and update consumer lag
                try {
                    long storageHead = storage.getCurrentOffset(topic, 0);
                    long lag = Math.max(0, storageHead - offset);
                    metrics.updateConsumerLag(clientId, topic, group, lag);
                } catch (Exception lagEx) {
                    log.debug("Could not update consumer lag metric for topic {}: {}", topic, lagEx.getMessage());
                }
            }

        } catch (Exception e) {
            log.error("Error handling legacy batch ACK for clientId={}, group={}", clientId, group, e);
        }
    }

    /**
     * Handle BATCH_ACK from consumer
     * This is called when the consumer successfully processes a batch and sends acknowledgment
     *
     * MULTI-GROUP FIX: Now expects group in ACK for proper routing
     *
     * @param clientId The client ID that sent the ACK
     * @param topic The topic that was acknowledged
     * @param group The group that was acknowledged
     */
    public void handleBatchAck(String clientId, String topic, String group) {
        // MULTI-GROUP FIX: Include group in delivery key
        String deliveryKey = clientId + ":" + topic + ":" + group;
        String pendingKey = deliveryKey + ":pending";

        // Calculate ACK latency
        long ackReceiveTime = System.currentTimeMillis();
        Long sendTime = batchSendTimestamps.remove(pendingKey);
        long ackLatencyMs = sendTime != null ? (ackReceiveTime - sendTime) : -1;

        Long committedOffset = pendingOffsets.remove(pendingKey);
        ScheduledFuture<?> timeoutFuture = pendingTimeouts.remove(pendingKey);
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        if (committedOffset == null) {
            log.warn("⚠️ ACK with no pending offset: {} (likely a late ACK after timeout). ACK_LATENCY={}ms. Clearing inFlight to unblock delivery.",
                     deliveryKey, ackLatencyMs);
            // B4-3 fix: always clear inFlight on any ACK, even a late one.
            // Without this, Gate 1 stays permanently true and delivery stalls forever.
            AtomicBoolean lateInFlight = inFlightDeliveries.get(deliveryKey);
            if (lateInFlight != null) {
                lateInFlight.set(false);
            }
            return;
        }

        // Log ACK latency with severity based on threshold
        if (ackLatencyMs > 10000) {
            log.error("🔴 ACK_RECEIVED for {} at T={}ms (CRITICAL SLOW!), offset={}, pendingOffsetsSize={}, batchTimestampsSize={}",
                      deliveryKey, ackLatencyMs, committedOffset, pendingOffsets.size(), batchSendTimestamps.size());
        } else if (ackLatencyMs > 5000) {
            log.warn("🟡 ACK_RECEIVED for {} at T={}ms (SLOW), offset={}, pendingOffsetsSize={}, batchTimestampsSize={}",
                     deliveryKey, ackLatencyMs, committedOffset, pendingOffsets.size(), batchSendTimestamps.size());
        } else if (ackLatencyMs > 1000) {
            log.warn("🟢 ACK_RECEIVED for {} at T={}ms, offset={}, pendingOffsetsSize={}, batchTimestampsSize={}",
                     deliveryKey, ackLatencyMs, committedOffset, pendingOffsets.size(), batchSendTimestamps.size());
        } else {
            log.warn("✅ ACK_RECEIVED for {} at T={}ms (FAST), offset={}", deliveryKey, ackLatencyMs, committedOffset);
        }

        RemoteConsumer consumer = consumers.get(deliveryKey);
        if (consumer != null) {
            offsetTracker.updateOffset(group + ":" + topic, committedOffset);
            // Update Prometheus gauge metric to reflect new offset
            metrics.updateConsumerOffset(clientId, topic, group, committedOffset);

            // Update last ACK timestamp for stuck detection
            metrics.updateConsumerLastAckTime(clientId, topic, group);

            // Record consumer ACK metric
            metrics.recordConsumerAck(clientId, topic, group);

            // Complete pending ACK tracking (resets age gauge to 0)
            metrics.completePendingAck(topic, group);
        } else {
            // MULTI-GROUP FIX: Group is now in the key, so we already know it
            // If consumer was unregistered, we can still persist the offset
            log.warn("ACK for unregistered consumer {}, persisting offset {} for group {}",
                     deliveryKey, committedOffset, group);
            offsetTracker.updateOffset(group + ":" + topic, committedOffset);
        }

        AtomicBoolean inFlight = inFlightDeliveries.get(deliveryKey);
        if (inFlight != null) {
            inFlight.set(false);
        }

        log.debug("ACK committed for {} at offset {}", deliveryKey, committedOffset);
    }

    /**
     * Get all consumer clientIds subscribed to a topic (for DataRefresh)
     */
    public java.util.Set<String> getConsumersForTopic(String topic) {
        java.util.Set<String> clientIds = new java.util.HashSet<>();
        for (Map.Entry<String, RemoteConsumer> entry : consumers.entrySet()) {
            if (entry.getValue().topic.equals(topic)) {
                clientIds.add(entry.getValue().clientId);
            }
        }
        log.debug("Found {} consumers for topic: {}", clientIds.size(), topic);
        return clientIds;
    }

    /**
     * Broadcast RESET message to all consumers of a topic (for DataRefresh)
     */
    public void broadcastResetToTopic(String topic) {
        java.util.Set<String> clientIds = getConsumersForTopic(topic);
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);

        BrokerMessage resetMsg = new BrokerMessage(
            BrokerMessage.MessageType.RESET,
            System.currentTimeMillis(),
            payload
        );

        for (String clientId : clientIds) {
            try {
                server.send(clientId, resetMsg);
                log.info("Sent RESET to consumer: {} for topic: {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send RESET to consumer: {}", clientId, e);
            }
        }
    }

    /**
     * Broadcast READY message to all consumers of a topic (for DataRefresh)
     */
    public void broadcastReadyToTopic(String topic) {
        java.util.Set<String> clientIds = getConsumersForTopic(topic);
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);

        BrokerMessage readyMsg = new BrokerMessage(
            BrokerMessage.MessageType.READY,
            System.currentTimeMillis(),
            payload
        );

        for (String clientId : clientIds) {
            try {
                server.send(clientId, readyMsg);
                log.info("Sent READY to consumer: {} for topic: {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send READY to consumer: {}", clientId, e);
            }
        }
    }

    /**
     * Send READY only to consumers whose group:topic identifier is in the provided set.
     * Used by DataRefreshManager to avoid sending READY to consumers that never ACKed RESET.
     *
     * @param topic            Topic name
     * @param ackedGroupTopics Set of "group:topic" identifiers that have ACKed RESET
     */
    public void sendReadyToAckedConsumers(String topic, java.util.Set<String> ackedGroupTopics) {
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);
        BrokerMessage readyMsg = new BrokerMessage(
            BrokerMessage.MessageType.READY,
            System.currentTimeMillis(),
            payload
        );

        for (RemoteConsumer consumer : consumers.values()) {
            if (!consumer.topic.equals(topic)) continue;
            String groupTopic = consumer.group + ":" + consumer.topic;
            if (!ackedGroupTopics.contains(groupTopic)) {
                log.warn("Skipping READY for consumer {} — did not ACK RESET (groupTopic={})",
                        consumer.clientId, groupTopic);
                continue;
            }
            try {
                server.send(consumer.clientId, readyMsg);
                log.info("Sent READY to consumer: {} (groupTopic={}) for topic: {}",
                        consumer.clientId, groupTopic, topic);
            } catch (Exception e) {
                log.error("Failed to send READY to consumer: {}", consumer.clientId, e);
            }
        }
    }

    /**
     * Reset offset for a specific consumer (for DataRefresh)
     * MULTI-GROUP FIX: Now requires group parameter for correct consumer lookup
     */
    public void resetConsumerOffset(String clientId, String topic, String group, long offset) {
        // MULTI-GROUP FIX: Include group in consumer key
        String consumerKey = clientId + ":" + topic + ":" + group;
        RemoteConsumer consumer = consumers.get(consumerKey);

        if (consumer == null) {
            log.warn("Cannot reset offset - consumer not found: consumerKey={}", consumerKey);
            return;
        }

        consumer.setCurrentOffset(offset);

        String consumerId = group + ":" + topic;
        offsetTracker.resetOffset(consumerId, offset);

        log.info("Reset offset to {} for consumer: {} ({})", offset, clientId, consumerId);
    }

    /**
     * Check if all specified consumers have caught up to latest offset (for DataRefresh)
     * MULTI-GROUP FIX: Parameter is consumerGroupTopics (format: "group:topic"), not clientIds
     */
    public boolean allConsumersCaughtUp(String topic, java.util.Set<String> consumerGroupTopics) {
        // CRITICAL FIX: Use getCurrentOffset (actual storage write head) instead of getMaxOffsetFromMetadata (stale)
        // getMaxOffsetFromMetadata reads from segment metadata DB which is only updated periodically (every 1000 appends)
        // During data refresh replay, metadata can lag behind actual storage by tens of thousands of offsets
        // This was causing READY to be sent prematurely when consumers caught up to the STALE metadata offset,
        // missing the remaining records that had been written but not yet reflected in metadata
        long latestOffset = storage.getCurrentOffset(topic, 0);

        log.debug("Checking if consumers caught up for topic {}: latestOffset from CURRENT storage = {}", topic, latestOffset);

        for (String consumerGroupTopic : consumerGroupTopics) {
            // MULTI-GROUP FIX: Use COMMITTED offset from offsetTracker with consumerGroupTopic key
            // consumerGroupTopic is already in "group:topic" format which is what offsetTracker expects
            long committedOffset = offsetTracker.getOffset(consumerGroupTopic);

            if (committedOffset < latestOffset) {
                log.debug("Consumer {} not caught up: committed={} < latest={} (need {} more messages)",
                         consumerGroupTopic, committedOffset, latestOffset, latestOffset - committedOffset);
                return false;
            }
        }

        log.info("✓ All consumers caught up for topic {} at offset {} (all ACKs received)", topic, latestOffset);
        return true;
    }

    /**
     * Get consumer group:topic identifier for a client (for DataRefresh)
     * Maps dynamic clientId to stable "group:topic" identifier
     *
     * MULTI-GROUP NOTE: This method is deprecated for multi-group scenarios.
     * Use getConsumerGroupTopicPairs() instead.
     *
     * @deprecated Use getConsumerGroupTopicPairs() for multi-group support
     */
    @Deprecated
    public String getConsumerGroupTopic(String clientId, String topic) {
        // MULTI-GROUP FIX: Find first matching consumer (there may be multiple groups)
        for (RemoteConsumer consumer : consumers.values()) {
            if (consumer.clientId.equals(clientId) && consumer.topic.equals(topic)) {
                return consumer.group + ":" + consumer.topic;
            }
        }

        log.warn("Consumer not found for clientId={}, topic={}", clientId, topic);
        return null;
    }

    /**
     * Get all (clientId, consumerGroupTopic) pairs for a topic.
     * This properly handles multi-group scenarios where one clientId may be
     * registered for the same topic with multiple groups.
     *
     * MULTI-GROUP FIX: Returns pairs to support iteration over all consumer registrations.
     */
    public java.util.List<ConsumerGroupTopicPair> getConsumerGroupTopicPairs(String topic) {
        return consumers.values().stream()
                .filter(c -> c.topic.equals(topic))
                .map(c -> new ConsumerGroupTopicPair(c.clientId, c.group + ":" + c.topic))
                .collect(Collectors.toList());
    }

    /**
     * Pair of (clientId, consumerGroupTopic) for multi-group replay iteration
     */
    public static class ConsumerGroupTopicPair {
        public final String clientId;
        public final String consumerGroupTopic;

        public ConsumerGroupTopicPair(String clientId, String consumerGroupTopic) {
            this.clientId = clientId;
            this.consumerGroupTopic = consumerGroupTopic;
        }
    }

    /**
     * Shutdown registry
     */
    public void shutdown() {
        log.info("Shutting down RemoteConsumerRegistry...");

        for (RemoteConsumer consumer : consumers.values()) {
            if (consumer.deliveryTask != null) {
                consumer.deliveryTask.cancel(false);
            }
        }

        for (ScheduledFuture<?> future : pendingTimeouts.values()) {
            future.cancel(false);
        }
        pendingTimeouts.clear();

        scheduler.shutdown();
        storageExecutor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            storageExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("RemoteConsumerRegistry shutdown complete");
    }


    /**
     * Get stable "group:topic" identifiers for all registered consumers of a topic.
     * Used by DataRefreshManager.startRefresh() to build the expectedConsumers set,
     * ensuring ACK matching uses the same format that handleResetAck() receives.
     *
     * @param topic Topic name
     * @return Set of "group:topic" strings for currently registered consumers of this topic
     */
    public java.util.Set<String> getGroupTopicIdentifiers(String topic) {
        java.util.Set<String> result = new java.util.HashSet<>();
        for (RemoteConsumer consumer : consumers.values()) {
            if (consumer.topic.equals(topic)) {
                result.add(consumer.group + ":" + consumer.topic);
            }
        }
        return result;
    }

    /**
     * Get all consumer client IDs for a topic (for DataRefresh replay)
     * Returns ALL consumers subscribed to the topic, not just one
     */
    public java.util.List<String> getAllConsumerIds(String topic) {
        return this.consumers.entrySet()
                .stream()
                .filter(e -> e.getValue().topic.equals(topic))
                .peek(e -> log.debug("Found consumer for topic {}: key={}, clientId={}",
                        topic, e.getKey(), e.getValue().clientId))
                .map(e -> e.getValue().clientId)
                .collect(Collectors.toList());
    }

    /**
     * @deprecated Use getAllConsumerIds() instead - this only returns ONE consumer!
     */
    @Deprecated
    public String getRemoteConsumers(String topic) {
        RemoteConsumer remoteConsumer = this.consumers.entrySet()
                .stream()
                .filter(e -> e.getKey().contains(topic))
                .peek(e -> log.debug("Consumer key in registry: {}", e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
        return remoteConsumer != null ? remoteConsumer.clientId : null;
    }

    /**
     * Check if exception is a connection error (Broken pipe, Connection reset, etc.)
     */
    private boolean isConnectionError(Exception e) {
        // Check for IOException with connection-related messages
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof java.io.IOException) {
                String message = cause.getMessage();
                if (message != null) {
                    message = message.toLowerCase();
                    if (message.contains("broken pipe") ||
                        message.contains("connection reset") ||
                        message.contains("connection refused") ||
                        message.contains("socket closed") ||
                        message.contains("stream closed")) {
                        return true;
                    }
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Get all legacy consumers for a given clientId.
     * Legacy consumers share the same clientId but subscribe to multiple topics.
     */
    public List<RemoteConsumer> getLegacyConsumersForClient(String clientId) {
        return consumers.values().stream()
                .filter(c -> c.clientId.equals(clientId) && c.isLegacy)
                .collect(Collectors.toList());
    }

    /**
     * Deliver merged batch to legacy consumer.
     * Builds a multi-topic batch sorted by global offset and delivers as single BATCH event.
     *
     * @param clientId Client ID (same for all topics subscribed by legacy client)
     * @param consumerGroup Consumer group (serviceName for legacy clients)
     * @param maxBytes Maximum batch size in bytes
     * @return true if delivery succeeded, false otherwise
     */
    public boolean deliverMergedBatchToLegacy(String clientId, String consumerGroup, long maxBytes) {
        log.warn("🔵 DELIVERY: deliverMergedBatchToLegacy called - clientId={}, group={}, maxBytes={}",
                clientId, consumerGroup, maxBytes);

        // Get all legacy consumers for this client
        List<RemoteConsumer> legacyConsumers = getLegacyConsumersForClient(clientId);
        if (legacyConsumers.isEmpty()) {
            log.warn("No legacy consumers found for clientId: {}", clientId);
            return false;
        }

        // Extract topic list
        List<String> topics = legacyConsumers.stream()
                .map(c -> c.topic)
                .collect(Collectors.toList());

        try {
            // Build merged batch using LegacyConsumerDeliveryManager
            MergedBatch mergedBatch = legacyDeliveryManager.buildMergedBatch(
                    topics, consumerGroup, maxBytes
            );

            if (mergedBatch.isEmpty()) {
                log.warn("🔵 DELIVERY: No messages to deliver for legacy client: {}", clientId);
                return false;
            }

            log.info("Built merged batch for legacy client {}: {} messages from {} topics, {} bytes",
                    clientId, mergedBatch.getMessageCount(), topics.size(), mergedBatch.getTotalBytes());

            // Convert MergedBatch to BrokerMessage BATCH_HEADER
            BrokerMessage batchMessage = convertMergedBatchToBrokerMessage(mergedBatch);

            // Start delivery latency timer
            Timer.Sample deliverySample = metrics.startConsumerDeliveryTimer();

            // Send batch to client
            long sendStart = System.currentTimeMillis();
            server.send(clientId, batchMessage).get(10, TimeUnit.SECONDS);
            long sendDuration = System.currentTimeMillis() - sendStart;
            log.warn("🔵 DELIVERY: Sent merged batch to {} in {}ms", clientId, sendDuration);

            // Record metrics for messages and bytes sent (global counters)
            log.warn("🔵 METRICS: About to record metrics for {} messages, {} bytes",
                    mergedBatch.getMessageCount(), mergedBatch.getTotalBytes());
            metrics.recordBatchMessagesSent(mergedBatch.getMessageCount(), mergedBatch.getTotalBytes());

            // Record per-consumer metrics for EACH topic in the merged batch
            // NOTE: Messages in merged batch don't have topic field set, so we distribute
            // the total count/bytes evenly across all topics for metrics purposes
            int topicCount = mergedBatch.getMaxOffsetPerTopic().size();
            int messagesPerTopic = topicCount > 0 ? mergedBatch.getMessageCount() / topicCount : 0;
            long bytesPerTopic = topicCount > 0 ? mergedBatch.getTotalBytes() / topicCount : 0;

            for (Map.Entry<String, Long> topicEntry : mergedBatch.getMaxOffsetPerTopic().entrySet()) {
                String topic = topicEntry.getKey();

                metrics.recordConsumerBatchSent(clientId, topic, consumerGroup,
                        messagesPerTopic, bytesPerTopic);

                // Record delivery latency for this topic
                metrics.stopConsumerDeliveryTimer(deliverySample, clientId, topic, consumerGroup);

                // Update last successful delivery timestamp for stuck detection
                metrics.updateConsumerLastDeliveryTime(clientId, topic, consumerGroup);
            }

            // Track pending ACK (will be resolved to BATCH_ACK by LegacyConnectionState)
            String pendingKey = clientId + ":legacy-batch:pending";
            pendingOffsets.put(pendingKey, System.currentTimeMillis());
            batchSendTimestamps.put(pendingKey, System.currentTimeMillis());

            // Schedule ACK timeout
            scheduleAckTimeout(clientId, "legacy-batch", consumerGroup, mergedBatch);

            log.warn("🔵 SUCCESS: Sent merged batch to legacy client {}: {} messages, {} bytes",
                    clientId, mergedBatch.getMessageCount(), mergedBatch.getTotalBytes());
            return true;

        } catch (Exception e) {
            log.error("Failed to deliver merged batch to legacy client: {}", clientId, e);

            // Record failure metrics for each topic
            for (String topic : topics) {
                metrics.recordConsumerFailure(clientId, topic, consumerGroup);
            }

            return false;
        }
    }

    /**
     * Convert MergedBatch to BrokerMessage for delivery.
     * Format: BATCH_HEADER with JSON payload containing all messages.
     */
    private BrokerMessage convertMergedBatchToBrokerMessage(MergedBatch batch) throws Exception {
        // For now, serialize as JSON
        // TODO: Optimize with binary format if needed
        String json = objectMapper.writeValueAsString(batch.getMessages());
        return new BrokerMessage(
                BrokerMessage.MessageType.BATCH_HEADER,
                System.nanoTime(),
                json.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Schedule ACK timeout for merged batch.
     */
    private void scheduleAckTimeout(String clientId, String topic, String group, MergedBatch batch) {
        String pendingKey = clientId + ":legacy-batch:pending";

        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            Long sendTime = batchSendTimestamps.remove(pendingKey);
            if (sendTime != null && pendingOffsets.remove(pendingKey) != null) {
                log.warn("ACK timeout for legacy batch: clientId={}, elapsed={}ms",
                        clientId, System.currentTimeMillis() - sendTime);

                // Record timeout metrics for ALL topics in the batch
                for (Map.Entry<String, Long> entry : batch.getMaxOffsetPerTopic().entrySet()) {
                    String topicName = entry.getKey();

                    // Record ACK timeout metric
                    metrics.recordAckTimeout(topicName, group);

                    // Clear pending ACK tracking (reset age gauge to 0)
                    metrics.completePendingAck(topicName, group);

                    // Offset NOT committed, will retry from current position
                    log.info("Legacy batch ACK timeout - offset NOT advanced for topic: {}", topicName);
                }
            }
        }, ackTimeoutMs, TimeUnit.MILLISECONDS);

        pendingTimeouts.put(pendingKey, timeoutTask);
    }

    /**
     * Remote consumer metadata
     */
    public static class RemoteConsumer {
        final String clientId;
        final String topic;
        final String group;
        final boolean isLegacy; // True if this consumer uses legacy Event protocol
        volatile long currentOffset;
        volatile Future<?> deliveryTask;
        volatile long lastDeliveryAttempt; // Rate limiting: timestamp of last delivery attempt
        volatile int consecutiveFailures; // Track consecutive failures for exponential backoff
        volatile long lastFailureTime; // Timestamp of last failure

        RemoteConsumer(String clientId, String topic, String group) {
            this(clientId, topic, group, false);
        }

        RemoteConsumer(String clientId, String topic, String group, boolean isLegacy) {
            this.clientId = clientId;
            this.topic = topic;
            this.group = group;
            this.isLegacy = isLegacy;
            this.currentOffset = 0;
            this.lastDeliveryAttempt = 0; // Allow immediate first delivery
            this.consecutiveFailures = 0;
            this.lastFailureTime = 0;
        }

        long getCurrentOffset() {
            return currentOffset;
        }

        void setCurrentOffset(long offset) {
            this.currentOffset = offset;
        }

        public String getGroup() {
            return group;
        }

        public String getTopic() {
            return topic;
        }

        public boolean isLegacy() {
            return isLegacy;
        }

        /**
         * Calculate backoff delay based on consecutive failures (exponential backoff)
         * @return delay in milliseconds before next retry
         */
        long getBackoffDelay() {
            if (consecutiveFailures == 0) {
                return 0; // No delay on first attempt
            }
            // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, max 5000ms
            long delay = Math.min(100L * (1L << (consecutiveFailures - 1)), 5000L);
            return delay;
        }

        void recordFailure() {
            consecutiveFailures++;
            lastFailureTime = System.currentTimeMillis();
        }

        void resetFailures() {
            consecutiveFailures = 0;
            lastFailureTime = 0;
        }
    }
}
