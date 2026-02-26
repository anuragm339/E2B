package com.messaging.broker.consumer;

import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.common.api.StorageEngine;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Adaptive polling-based batch delivery manager
 *
 * Replaces push-based delivery with watermark-based adaptive polling:
 * - Checks storage watermarks before reading (cheap in-memory offset check)
 * - Adaptive delay: 1ms when active, exponential backoff to 1s when idle
 * - Per-topic fairness via TopicFairScheduler
 * - Persistent delivery state for safe restarts
 *
 * Architecture:
 * - Each (consumer, topic) pair gets its own adaptive polling task
 * - Watermark check → storage read (only if new data) → TCP send → ACK wait
 * - Delay adapts: data found = 1ms, no data = backoff to 1s
 */
@Singleton
public class AdaptiveBatchDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(AdaptiveBatchDeliveryManager.class);

    private final RemoteConsumerRegistry consumerRegistry;
    private final StorageEngine storage;
    private final DeliveryStateStore deliveryStateStore;
    private final TopicFairScheduler fairScheduler;
    private final BrokerMetrics metrics;
    private final long batchSizeBytes;
    // B11-6a fix: Reference to DataRefreshManager to check if topic is in RESET_SENT state
    private com.messaging.broker.refresh.DataRefreshManager dataRefreshManager;

    // Adaptive polling config
    private static final long MIN_POLL_DELAY_MS = 1;     // Immediate on data found (near-push latency)
    private static final long MAX_POLL_DELAY_MS = 1000;  // Max 1s backoff when idle

    private volatile boolean running = false;

    @Inject
    public AdaptiveBatchDeliveryManager(
            RemoteConsumerRegistry consumerRegistry,
            StorageEngine storage,
            DeliveryStateStore deliveryStateStore,
            TopicFairScheduler fairScheduler,
            BrokerMetrics metrics,
            @Value("${broker.consumer.max-message-size-per-consumer:1048576}") long batchSizeBytes) {

        this.consumerRegistry = consumerRegistry;
        this.storage = storage;
        this.deliveryStateStore = deliveryStateStore;
        this.fairScheduler = fairScheduler;
        this.metrics = metrics;
        this.batchSizeBytes = batchSizeBytes;

        log.info("AdaptiveBatchDeliveryManager initialized: batchSize={}bytes, " +
                 "minDelay={}ms, maxDelay={}ms, per-topic current-offset watermark enabled",
                batchSizeBytes, MIN_POLL_DELAY_MS, MAX_POLL_DELAY_MS);
    }

    /**
     * Wire this manager to RemoteConsumerRegistry after injection
     */
    @PostConstruct
    public void init() {
        // Wire back-reference to enable consumer registration notifications
        consumerRegistry.setAdaptiveBatchDeliveryManager(this);
        log.info("AdaptiveBatchDeliveryManager wired to RemoteConsumerRegistry for consumer registration");
    }

    /**
     * B11-6a fix: Set DataRefreshManager reference (called by DataRefreshManager.init())
     */
    public void setDataRefreshManager(com.messaging.broker.refresh.DataRefreshManager dataRefreshManager) {
        this.dataRefreshManager = dataRefreshManager;
        log.info("AdaptiveBatchDeliveryManager wired to DataRefreshManager for refresh state checking");
    }

    /**
     * Start adaptive delivery for all consumers
     */
    public void start() {
        if (running) {
            log.warn("AdaptiveBatchDeliveryManager already running");
            return;
        }

        running = true;

        log.info("Starting adaptive batch delivery for existing consumers... {}",consumerRegistry.getAllConsumers());
        // Schedule adaptive polling for each existing consumer
        for (RemoteConsumerRegistry.RemoteConsumer consumer : consumerRegistry.getAllConsumers()) {
            scheduleAdaptiveDelivery(consumer, MIN_POLL_DELAY_MS);
        }

        log.info("Adaptive batch delivery started for {} consumers",
                consumerRegistry.getAllConsumers().size());
    }

    /**
     * Schedule adaptive delivery with exponential backoff
     *
     * This is the core adaptive polling loop:
     * 1. Try to deliver batch
     * 2. If data found → reschedule with MIN delay (1ms)
     * 3. If no data → reschedule with exponential backoff (up to 1s)
     *
     * @param consumer Consumer to deliver to
     * @param delayMs Current delay (will be adapted based on result)
     */
    private void scheduleAdaptiveDelivery(RemoteConsumerRegistry.RemoteConsumer consumer, long delayMs) {
        if (!running) {
            log.warn("DEBUG: scheduleAdaptiveDelivery called but not running for {}:{}", consumer.clientId, consumer.topic);
            return;
        }

        log.debug("Scheduling adaptive delivery for {}:{} with delay={}ms", consumer.clientId, consumer.topic, delayMs);
        // B1-2 fix: capture the ScheduledFuture and assign it to consumer.deliveryTask so that
        // unregisterConsumer() can cancel the task and stop delivery after disconnect.
        // B11-5 fix: use scheduleWithKey with unique deliveryKey to prevent unbounded retry buildup during DataRefresh
        String deliveryKey = consumer.clientId + ":" + consumer.topic;
        java.util.concurrent.ScheduledFuture<?> future = fairScheduler.scheduleWithKey(
            consumer.topic, deliveryKey, () -> {
            log.debug("Executing delivery task for {}:{}", consumer.clientId, consumer.topic);
            // Try delivery and get result (true = data found, false = no data/skipped)
            boolean dataFound = tryDeliverBatch(consumer);

            // Adaptive delay calculation
            long nextDelay;
            if (dataFound) {
                nextDelay = MIN_POLL_DELAY_MS;  // Immediate poll on success (near-push latency)
                log.trace("Data found for {}:{}, next poll in {}ms",
                        consumer.clientId, consumer.topic, nextDelay);
            } else {
                // Exponential backoff: no data → double delay up to 1s
                nextDelay = Math.min(delayMs * 2, MAX_POLL_DELAY_MS);
                log.trace("No data for {}:{}, backing off to {}ms",
                        consumer.clientId, consumer.topic, nextDelay);
            }

            // Reschedule adaptively
            scheduleAdaptiveDelivery(consumer, nextDelay);

        }, delayMs, TimeUnit.MILLISECONDS);
        consumer.deliveryTask = future;
    }

    /**
     * Try to deliver batch - returns true if data was found and delivered
     *
     * Flow:
     * 1. Get consumer's current offset
     * 2. Query storage for latest offset (in-memory write head)
     * 3. If latest > consumer offset → call consumerRegistry.deliverBatch()
     * 4. Return true if data delivered, false if skipped
     *
     * @param consumer Consumer to deliver to
     * @return true if data was found, false if no data or delivery skipped
     */
    private boolean tryDeliverBatch(RemoteConsumerRegistry.RemoteConsumer consumer) {
        String deliveryKey = consumer.clientId + ":" + consumer.topic;

        try {
            // B11-6a FIX: Skip delivery if topic is in DataRefresh RESET_SENT state
            // During RESET wait, consumer should be paused and not receiving messages
            // Continuing delivery creates polling storm with 187+ blocked attempts/minute
            // causing OOM due to accumulated ScheduledFuture + CompletableFuture objects
            if (dataRefreshManager != null) {
                com.messaging.broker.refresh.DataRefreshContext refreshContext =
                    dataRefreshManager.getRefreshStatus(consumer.topic);
                if (refreshContext != null &&
                    refreshContext.getState() == com.messaging.broker.refresh.DataRefreshState.RESET_SENT) {
                    log.trace("B11-6a: Skipping delivery for {}:{} - topic in RESET_SENT state (waiting for consumer ACK)",
                             consumer.clientId, consumer.topic);
                    metrics.recordAdaptivePollSkipped(consumer.topic);
                    return false;  // Return false to trigger exponential backoff to MAX_POLL_DELAY_MS (1s)
                }
            }

            // Get consumer's current offset from RemoteConsumer
            long consumerOffset = consumer.getCurrentOffset();

            // Query storage for latest offset (in-memory write head)
            long latestOffset = storage.getCurrentOffset(consumer.topic, 0);

            log.debug("tryDeliverBatch offset check: topic={}, latestOffset={}, consumerOffset={}",
                    consumer.topic, latestOffset, consumerOffset);

            // Check if new data available
            if (latestOffset <= consumerOffset) {
                log.debug("No new data available (latestOffset={} <= consumerOffset={})", latestOffset, consumerOffset);
                metrics.recordAdaptivePollSkipped(consumer.topic);
                return false;  // No data → trigger backoff
            }

            // New data available → proceed with delivery
            log.debug("New data available! Calling deliverBatch: topic={}, latest={}, consumer={}",
                     consumer.topic, latestOffset, consumerOffset);

            boolean success = consumerRegistry.deliverBatch(consumer, batchSizeBytes);

            if (success) {
                metrics.recordAdaptivePollSuccess(consumer.topic);
                return true;   // Data found → poll immediately
            } else {
                metrics.recordAdaptivePollSkipped(consumer.topic);
                return false;  // Delivery failed (in-flight/ACK pending) → backoff
            }

        } catch (Exception e) {
            log.error("Delivery attempt failed for {}", deliveryKey, e);
            metrics.recordAdaptivePollSkipped(consumer.topic);
            return false;  // Error → backoff
        }
    }

    /**
     * Register new consumer and start adaptive delivery for it
     *
     * @param consumer Consumer to register
     */
    public void registerConsumer(RemoteConsumerRegistry.RemoteConsumer consumer) {
        if (running) {
            scheduleAdaptiveDelivery(consumer, MIN_POLL_DELAY_MS);
            log.info("Started adaptive delivery for new consumer: {}:{}",
                    consumer.clientId, consumer.topic);
        }
    }

    /**
     * Stop adaptive delivery
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        fairScheduler.shutdown();
        log.info("Adaptive delivery manager stopped");
    }

    /**
     * Get current status for monitoring
     */
    public String getStatus() {
        int consumerCount = consumerRegistry.getAllConsumers().size();
        int trackedStates = deliveryStateStore.getTrackedCount();

        return String.format("AdaptiveBatchDeliveryManager[running=%s, consumers=%d, trackedStates=%d]",
                running, consumerCount, trackedStates);
    }
}
