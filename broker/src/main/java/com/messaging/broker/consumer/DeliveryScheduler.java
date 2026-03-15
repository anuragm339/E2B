package com.messaging.broker.consumer;

import com.messaging.broker.consumer.DeliveryGatePolicy;
import com.messaging.broker.consumer.DeliveryRetryPolicy;
import com.messaging.broker.monitoring.BrokerMetrics;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates adaptive batch delivery scheduling.
 *
 * Responsibilities:
 * - Schedule delivery tasks with fair per-topic scheduling
 * - Apply gate policies before each delivery attempt
 * - Apply retry policy for adaptive backoff
 * - Coordinate with consumer registry for actual delivery
 */
@Singleton
public class DeliveryScheduler {
    private static final Logger log = LoggerFactory.getLogger(DeliveryScheduler.class);

    private final TopicFairScheduler fairScheduler;
    private final ConsumerRegistry consumerRegistry;
    private final DeliveryRetryPolicy retryPolicy;
    private final List<DeliveryGatePolicy> gatePolicies;
    private final BrokerMetrics metrics;
    private final long batchSizeBytes;

    @Inject
    public DeliveryScheduler(
            TopicFairScheduler fairScheduler,
            ConsumerRegistry consumerRegistry,
            DeliveryRetryPolicy retryPolicy,
            List<DeliveryGatePolicy> gatePolicies,
            BrokerMetrics metrics,
            @Value("${broker.consumer.max-message-size-per-consumer:1048576}") long batchSizeBytes) {
        this.fairScheduler = fairScheduler;
        this.consumerRegistry = consumerRegistry;
        this.retryPolicy = retryPolicy;
        this.gatePolicies = gatePolicies;
        this.metrics = metrics;
        this.batchSizeBytes = batchSizeBytes;

        log.info("DeliveryScheduler initialized with {} gate policies, batchSize={}bytes",
                gatePolicies.size(), batchSizeBytes);
    }

    /**
     * Schedule adaptive delivery for a consumer.
     *
     * @param consumer Consumer to schedule delivery for
     * @param delayMs Initial delay in milliseconds
     */
    public void scheduleDelivery(RemoteConsumer consumer, long delayMs) {
        String deliveryKey = consumer.getClientId() + ":" + consumer.getTopic();

        log.debug("Scheduling delivery for {}:{} with delay={}ms", consumer.getClientId(), consumer.getTopic(), delayMs);

        ScheduledFuture<?> future = fairScheduler.scheduleWithKey(
                consumer.getTopic(),
                deliveryKey,
                () -> executeDelivery(consumer, delayMs),
                delayMs,
                TimeUnit.MILLISECONDS
        );

        consumer.setDeliveryTask(future);
    }

    /**
     * Execute delivery attempt with gate checks and adaptive rescheduling.
     */
    private void executeDelivery(RemoteConsumer consumer, long currentDelayMs) {
        log.debug("Executing delivery task for {}:{}", consumer.getClientId(), consumer.getTopic());

        // Apply all gate policies
        for (DeliveryGatePolicy gate : gatePolicies) {
            DeliveryGatePolicy.GateResult result = gate.shouldDeliver(consumer);
            if (!result.isAllowed()) {
                log.trace("Delivery blocked by gate: {}:{} - {}",
                        consumer.getClientId(), consumer.getTopic(), result.getReason());
                metrics.recordAdaptivePollSkipped(consumer.getTopic());

                // Reschedule with backoff (no data found)
                long nextDelay = retryPolicy.calculateNextDelay(currentDelayMs, false);
                scheduleDelivery(consumer, nextDelay);
                return;
            }
        }

        // All gates passed - attempt delivery
        boolean success = consumerRegistry.deliverBatch(consumer, batchSizeBytes);

        if (success) {
            metrics.recordAdaptivePollSuccess(consumer.getTopic());
            log.trace("Data delivered for {}:{}", consumer.getClientId(), consumer.getTopic());
        } else {
            metrics.recordAdaptivePollSkipped(consumer.getTopic());
            log.trace("Delivery skipped for {}:{} (in-flight or no capacity)",
                    consumer.getClientId(), consumer.getTopic());
        }

        // Calculate next delay based on success
        long nextDelay = retryPolicy.calculateNextDelay(currentDelayMs, success);
        scheduleDelivery(consumer, nextDelay);
    }

    /**
     * Get initial delay for new delivery task.
     */
    public long getInitialDelay() {
        return retryPolicy.getInitialDelay();
    }

    /**
     * Shutdown scheduler.
     */
    public void shutdown() {
        fairScheduler.shutdown();
        log.info("DeliveryScheduler shutdown complete");
    }
}
