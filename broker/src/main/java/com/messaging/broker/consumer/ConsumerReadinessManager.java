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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Manages READY_ACK workflow with retry handling.
 */
@Singleton
public class ConsumerReadinessManager implements ConsumerReadinessService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerReadinessManager.class);
    private static final int MAX_READY_RETRIES = 3;
    private static final long READY_RETRY_DELAY_MS = 5000; // 5 seconds
    private static final long SEND_TIMEOUT_SECONDS = 10;

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
                if (isReady(clientId, topic, group)) {
                    return;
                }
                byte[] payload = topic != null ? topic.getBytes(StandardCharsets.UTF_8) : new byte[0];

                // Send READY message again
                BrokerMessage readyMessage = new BrokerMessage(
                    BrokerMessage.MessageType.READY,
                    System.currentTimeMillis(),
                    payload
                );
                awaitSend(server.send(clientId, readyMessage));

                log.info("🔄 READY retry {}/{} sent to {}",
                    retryCount + 1, MAX_READY_RETRIES, retryKey);

                // Schedule next retry
                if (!isReady(clientId, topic, group)) {
                    scheduleReadyRetry(clientId, topic, group, retryCount + 1);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("READY retry interrupted for {}", retryKey);
            } catch (Exception e) {
                log.error("Failed to send READY retry to {}: {}", retryKey, e.getMessage());
                // #12: a failed send must NOT terminate the retry chain. Reschedule (bounded by
                // MAX_READY_RETRIES, checked at the top) so a transient send blip still recovers
                // — otherwise this consumer is stuck not-ready and never receives deliveries.
                if (!isReady(clientId, topic, group)) {
                    scheduleReadyRetry(clientId, topic, group, retryCount + 1);
                }
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

    private boolean isReady(String clientId, String topic, String group) {
        return topic == null
                ? readyStateStore.isLegacyConsumerReady(clientId)
                : readyStateStore.isModernConsumerTopicReady(
                        clientId, DeliveryKey.of(group, topic));
    }

    private void awaitSend(CompletableFuture<Void> future) throws Exception {
        try {
            future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        }
    }
}
