package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.ConsumerEventLogger;
import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.consumer.ConsumerRegistrationService;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.consumer.ConsumerSessionStore;
import com.messaging.common.api.StorageEngine;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

/**
 * Manages consumer registration lifecycle with offset restoration and validation.
 */
@Singleton
public class ConsumerRegistrationManager implements ConsumerRegistrationService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRegistrationManager.class);

    private final ConsumerSessionStore sessionStore;
    private final ConsumerOffsetTracker offsetTracker;
    private final StorageEngine storage;
    private final BrokerMetrics metrics;
    private final ConsumerEventLogger consumerLogger;

    @Inject
    public ConsumerRegistrationManager(
            ConsumerSessionStore sessionStore,
            ConsumerOffsetTracker offsetTracker,
            StorageEngine storage,
            BrokerMetrics metrics,
            ConsumerEventLogger consumerLogger) {
        this.sessionStore = sessionStore;
        this.offsetTracker = offsetTracker;
        this.storage = storage;
        this.metrics = metrics;
        this.consumerLogger = consumerLogger;
    }

    @Override
    public RegistrationResult registerConsumer(String clientId, String topic, String group, boolean isLegacy, String traceId) {
        ConsumerKey key = ConsumerKey.of(clientId, topic, group);

        // Check for duplicate registration
        if (sessionStore.contains(key)) {
            LogContext context = LogContext.builder()
                    .traceId(traceId)
                    .clientId(clientId)
                    .topic(topic)
                    .consumerGroup(group)
                    .build();
            consumerLogger.logConsumerRegistrationDuplicate(context);
            RemoteConsumer existing = sessionStore.get(key).orElseThrow();
            return RegistrationResult.duplicate(existing);
        }

        // Load and validate persisted offset
        DeliveryKey deliveryKey = DeliveryKey.of(group, topic);
        long restoredOffset = offsetTracker.getOffset(deliveryKey.toString());
        long validatedOffset = validateAndCorrectOffset(deliveryKey, restoredOffset, traceId);

        // Create consumer
        RemoteConsumer consumer = new RemoteConsumer(clientId, topic, group, isLegacy);
        consumer.setCurrentOffset(validatedOffset);

        // Store
        sessionStore.put(key, consumer);

        // Metrics
        metrics.updateConsumerOffset(clientId, topic, group, validatedOffset);
        metrics.updateConsumerLag(clientId, topic, group, 0);
        metrics.recordConsumerConnection();

        // Structured logging
        LogContext context = LogContext.builder()
                .traceId(traceId)
                .clientId(clientId)
                .topic(topic)
                .consumerGroup(group)
                .offset(validatedOffset)
                .custom("legacy", isLegacy)
                .build();
        consumerLogger.logConsumerRegistered(context);

        return RegistrationResult.newRegistration(consumer, validatedOffset);
    }

    private long validateAndCorrectOffset(DeliveryKey deliveryKey, long restoredOffset, String traceId) {
        try {
            long storageHead = storage.getCurrentOffset(deliveryKey.topic(), 0);

            if (restoredOffset > storageHead) {
                LogContext context = LogContext.builder()
                        .traceId(traceId)
                        .topic(deliveryKey.topic())
                        .consumerGroup(deliveryKey.group())
                        .offset(restoredOffset)
                        .custom("storageHead", storageHead)
                        .custom("reason", "exceeds storage head")
                        .build();
                consumerLogger.logConsumerOffsetClamped(context);
                offsetTracker.updateOffset(deliveryKey.toString(), storageHead);
                return storageHead;
            } else if (restoredOffset < 0) {
                long earliestOffset = storage.getEarliestOffset(deliveryKey.topic(), 0);
                LogContext context = LogContext.builder()
                        .traceId(traceId)
                        .topic(deliveryKey.topic())
                        .consumerGroup(deliveryKey.group())
                        .offset(restoredOffset)
                        .custom("reason", "negative offset")
                        .custom("startingFrom", earliestOffset)
                        .build();
                consumerLogger.logConsumerOffsetClamped(context);
                offsetTracker.updateOffset(deliveryKey.toString(), earliestOffset);
                return earliestOffset;
            }

        } catch (Exception e) {
            log.error("Failed to validate offset for {}: {}", deliveryKey, e.getMessage(), e);
            // Continue with restored offset to avoid blocking registration
        }

        return restoredOffset;
    }

    @Override
    public int unregisterConsumer(String clientId) {
        Collection<RemoteConsumer> consumers = sessionStore.getByClientId(clientId);
        int count = sessionStore.removeByClientId(clientId);

        for (RemoteConsumer consumer : consumers) {
            LogContext context = LogContext.builder()
                    .clientId(clientId)
                    .topic(consumer.getTopic())
                    .consumerGroup(consumer.getGroup())
                    .build();
            consumerLogger.logConsumerUnregistered(context);
        }

        return count;
    }

    @Override
    public Optional<RemoteConsumer> getConsumer(ConsumerKey key) {
        return sessionStore.get(key);
    }

    @Override
    public Collection<RemoteConsumer> getConsumersByClient(String clientId) {
        return sessionStore.getByClientId(clientId);
    }

    @Override
    public Collection<RemoteConsumer> getConsumersByTopic(String topic) {
        return sessionStore.getByTopic(topic);
    }

    @Override
    public Collection<RemoteConsumer> getAllConsumers() {
        return sessionStore.getAll();
    }
}
