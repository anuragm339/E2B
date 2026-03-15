package com.messaging.broker.consumer;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adaptive polling-based batch delivery manager.
 *
 * Thin coordinator that delegates to DeliveryScheduler for:
 * - Adaptive polling with watermark-based delivery
 * - Gate policies (data refresh, watermark checks)
 * - Retry policies (exponential backoff 1ms→1s)
 * - Per-topic fair scheduling
 *
 * This class now focuses solely on consumer lifecycle management.
 */
@Singleton
public class AdaptiveBatchDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(AdaptiveBatchDeliveryManager.class);

    private final ConsumerRegistry consumerRegistry;
    private final DeliveryScheduler deliveryScheduler;
    private final DeliveryStateStore deliveryStateStore;

    private volatile boolean running = false;

    @Inject
    public AdaptiveBatchDeliveryManager(
            ConsumerRegistry consumerRegistry,
            DeliveryScheduler deliveryScheduler,
            DeliveryStateStore deliveryStateStore) {

        this.consumerRegistry = consumerRegistry;
        this.deliveryScheduler = deliveryScheduler;
        this.deliveryStateStore = deliveryStateStore;

        log.info("AdaptiveBatchDeliveryManager initialized");
    }

    /**
     * Wire this manager to ConsumerRegistry after injection.
     */
    @PostConstruct
    public void init() {
        consumerRegistry.setAdaptiveBatchDeliveryManager(this);
        log.info("AdaptiveBatchDeliveryManager wired to ConsumerRegistry");
    }

    /**
     * Start adaptive delivery for all consumers.
     */
    public void start() {
        if (running) {
            log.warn("AdaptiveBatchDeliveryManager already running");
            return;
        }

        running = true;

        log.info("Starting adaptive batch delivery for {} existing consumers",
                consumerRegistry.getAllConsumers().size());

        // Schedule adaptive polling for each existing consumer
        long initialDelay = deliveryScheduler.getInitialDelay();
        for (RemoteConsumer consumer : consumerRegistry.getAllConsumers()) {
            deliveryScheduler.scheduleDelivery(consumer, initialDelay);
        }

        log.info("Adaptive batch delivery started");
    }

    /**
     * Register new consumer and start adaptive delivery for it.
     *
     * @param consumer Consumer to register
     */
    public void registerConsumer(RemoteConsumer consumer) {
        if (running) {
            long initialDelay = deliveryScheduler.getInitialDelay();
            deliveryScheduler.scheduleDelivery(consumer, initialDelay);
            log.info("Started adaptive delivery for new consumer: {}:{}",
                    consumer.getClientId(), consumer.getTopic());
        }
    }

    /**
     * Stop adaptive delivery.
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        deliveryScheduler.shutdown();
        log.info("Adaptive delivery manager stopped");
    }

    /**
     * Get current status for monitoring.
     */
    public String getStatus() {
        int consumerCount = consumerRegistry.getAllConsumers().size();
        int trackedStates = deliveryStateStore.getTrackedCount();

        return String.format("AdaptiveBatchDeliveryManager[running=%s, consumers=%d, trackedStates=%d]",
                running, consumerCount, trackedStates);
    }
}
