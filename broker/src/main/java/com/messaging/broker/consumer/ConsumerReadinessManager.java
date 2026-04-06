package com.messaging.broker.consumer;

import com.messaging.broker.consumer.ConsumerReadinessService;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.ReadyStateStore;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.model.BrokerMessage;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages READY_ACK workflow with retry handling.
 */
@Singleton
public class ConsumerReadinessManager implements ConsumerReadinessService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerReadinessManager.class);
    private static final int MAX_READY_RETRIES = 3;
    private static final long READY_RETRY_DELAY_MS = 5000; // 5 seconds

    private final ReadyStateStore readyStateStore;
    private final NetworkServer server;
    private final ScheduledExecutorService scheduler;

    @Inject
    public ConsumerReadinessManager(
            ReadyStateStore readyStateStore,
            NetworkServer server,
            @Named("consumerScheduler") ScheduledExecutorService scheduler) {
        this.readyStateStore = readyStateStore;
        this.server = server;
        this.scheduler = scheduler;
    }

    @Override
    public void markLegacyConsumerReady(String clientId) {
        readyStateStore.markLegacyConsumerReady(clientId);
        readyStateStore.cancelReadyRetry(clientId);
        log.info("✅ Legacy consumer ready: {}", clientId);
    }

    @Override
    public void markModernConsumerTopicReady(String clientId, String topic, String group) {
        DeliveryKey deliveryKey = DeliveryKey.of(group, topic);
        readyStateStore.markModernConsumerTopicReady(clientId, deliveryKey);
        readyStateStore.cancelReadyRetry(retryKey(clientId, topic, group));
        log.info("✅ Modern consumer ready: {} -> {}", clientId, deliveryKey);
    }

    @Override
    public boolean isLegacyConsumerReady(String clientId) {
        return readyStateStore.isLegacyConsumerReady(clientId);
    }

    @Override
    public boolean isModernConsumerTopicReady(String clientId, String topic, String group) {
        return readyStateStore.isModernConsumerTopicReady(clientId, DeliveryKey.of(group, topic));
    }

    @Override
    public Set<DeliveryKey> getModernConsumerReadyTopics(String clientId) {
        return readyStateStore.getModernConsumerReadyTopics(clientId);
    }

    @Override
    public void scheduleReadyRetry(String clientId, String topic, String group, int retryCount) {
        if (retryCount >= MAX_READY_RETRIES) {
            log.warn("⚠️ Max READY retries reached for {}", retryKey(clientId, topic, group));
            return;
        }

        String retryKey = retryKey(clientId, topic, group);

        ScheduledFuture<?> retryTask = scheduler.schedule(() -> {
            try {
                byte[] payload = topic != null ? topic.getBytes(StandardCharsets.UTF_8) : new byte[0];

                // Send READY message again
                BrokerMessage readyMessage = new BrokerMessage(
                    BrokerMessage.MessageType.READY,
                    System.currentTimeMillis(),
                    payload
                );
                server.send(clientId, readyMessage).get();

                log.info("🔄 READY retry {}/{} sent to {}",
                    retryCount + 1, MAX_READY_RETRIES, retryKey);

                // Schedule next retry
                scheduleReadyRetry(clientId, topic, group, retryCount + 1);

            } catch (Exception e) {
                log.error("Failed to send READY retry to {}: {}", retryKey, e.getMessage());
            }
        }, READY_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);

        readyStateStore.scheduleReadyRetry(retryKey, retryTask);
    }

    @Override
    public void cancelReadyRetry(String clientId, String topic, String group) {
        readyStateStore.cancelReadyRetry(retryKey(clientId, topic, group));
    }

    @Override
    public void removeClient(String clientId) {
        readyStateStore.removeClient(clientId);
        log.debug("Removed readiness state for client: {}", clientId);
    }

    private String retryKey(String clientId, String topic, String group) {
        if (topic == null) {
            return clientId;
        }
        return clientId + ":" + group + ":" + topic;
    }
}
