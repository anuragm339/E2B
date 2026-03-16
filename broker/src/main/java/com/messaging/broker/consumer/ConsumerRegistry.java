package com.messaging.broker.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.messaging.broker.consumer.*;
import com.messaging.broker.legacy.LegacyConsumerDeliveryManager;
import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Unified entry point for consumer registration, delivery, readiness, and ACK handling.
 */
@Singleton
public class ConsumerRegistry {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRegistry.class);

    private final ConsumerRegistrationService registrationService;
    private final ConsumerReadinessService readinessService;
    private final ConsumerDeliveryService deliveryService;
    private final ConsumerAckService ackService;
    private final ConsumerStateService stateService;
    private final LegacyConsumerDeliveryManager legacyDeliveryManager;
    private final PendingAckStore pendingAckStore;
    private final BrokerMetrics metrics;
    private final NetworkServer server;
    private final StorageEngine storage;
    private final ConsumerOffsetTracker offsetTracker;
    private final ScheduledExecutorService consumerScheduler;
    private final long legacyAckTimeoutMs;
    private final ObjectMapper objectMapper;

    private volatile AdaptiveBatchDeliveryManager adaptiveDeliveryManager; // Lazy injection

    @Inject
    public ConsumerRegistry(
            ConsumerRegistrationService registrationService,
            ConsumerReadinessService readinessService,
            ConsumerDeliveryService deliveryService,
            ConsumerAckService ackService,
            ConsumerStateService stateService,
            LegacyConsumerDeliveryManager legacyDeliveryManager,
            PendingAckStore pendingAckStore,
            BrokerMetrics metrics,
            NetworkServer server,
            StorageEngine storage,
            ConsumerOffsetTracker offsetTracker,
            @Named("consumerScheduler") ScheduledExecutorService consumerScheduler,
            @Value("${broker.consumer.ack-timeout:60000}") long legacyAckTimeoutMs) {
        this.registrationService = registrationService;
        this.readinessService = readinessService;
        this.deliveryService = deliveryService;
        this.ackService = ackService;
        this.stateService = stateService;
        this.legacyDeliveryManager = legacyDeliveryManager;
        this.pendingAckStore = pendingAckStore;
        this.metrics = metrics;
        this.server = server;
        this.storage = storage;
        this.offsetTracker = offsetTracker;
        this.consumerScheduler = consumerScheduler;
        this.legacyAckTimeoutMs = legacyAckTimeoutMs;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    /**
     * Set AdaptiveBatchDeliveryManager reference (called by AdaptiveBatchDeliveryManager.init()).
     */
    public void setAdaptiveBatchDeliveryManager(AdaptiveBatchDeliveryManager adaptiveDeliveryManager) {
        this.adaptiveDeliveryManager = adaptiveDeliveryManager;
        log.info("ConsumerRegistry wired to AdaptiveBatchDeliveryManager");
    }

    // ==================== CONSUMER REGISTRATION ====================

    /**
     * Register consumer (modern protocol).
     */
    public boolean registerConsumer(String clientId, String topic, String group) {
        return registerConsumer(clientId, topic, group, false, null);
    }

    /**
     * Register consumer with legacy flag.
     */
    public boolean registerConsumer(String clientId, String topic, String group, boolean isLegacy) {
        return registerConsumer(clientId, topic, group, isLegacy, null);
    }

    /**
     * Register consumer with explicit trace id for correlated logging.
     */
    public boolean registerConsumer(String clientId, String topic, String group, boolean isLegacy, String traceId) {
        ConsumerRegistrationService.RegistrationResult result =
                registrationService.registerConsumer(clientId, topic, group, isLegacy, traceId);

        // Notify adaptive delivery manager if this is a new consumer
        if (result.isNew() && adaptiveDeliveryManager != null) {
            adaptiveDeliveryManager.registerConsumer(result.consumer());
        }

        return result.isNew();
    }

    /**
     * Unregister all consumers for a client (on disconnect).
     */
    public int unregisterConsumer(String clientId) {
        Collection<RemoteConsumer> consumers = registrationService.getConsumersByClient(clientId);

        for (RemoteConsumer consumer : consumers) {
            stateService.removeDeliveryState(DeliveryKey.of(consumer.getGroup(), consumer.getTopic()));
            metrics.completePendingAck(consumer.getTopic(), consumer.getGroup());
        }

        readinessService.removeClient(clientId);
        ackService.clearPendingAcks(clientId);

        if (adaptiveDeliveryManager != null) {
            adaptiveDeliveryManager.removeConsumer(clientId);
        }

        return registrationService.unregisterConsumer(clientId);
    }

    /**
     * Get all registered consumers.
     */
    public List<RemoteConsumer> getAllConsumers() {
        return new ArrayList<>(registrationService.getAllConsumers());
    }

    /**
     * Get all consumer clientIds subscribed to a topic for refresh coordination.
     */
    public Set<String> getConsumersForTopic(String topic) {
        return registrationService.getConsumersByTopic(topic).stream()
                .map(c -> c.getClientId())
                .collect(Collectors.toSet());
    }

    // ==================== BATCH DELIVERY ====================

    /**
     * Deliver batch to consumer (called by AdaptiveBatchDeliveryManager).
     */
    public boolean deliverBatch(RemoteConsumer consumer, long batchSizeBytes) {
        if (consumer.isLegacy()) {
            return deliverMergedBatchToLegacy(consumer.getClientId(), consumer.getGroup(), batchSizeBytes);
        }
        ConsumerDeliveryService.DeliveryResult result =
                deliveryService.deliverBatch(consumer, batchSizeBytes);
        return result.delivered();
    }

    /**
     * Deliver a merged batch to a legacy consumer across multiple topics.
     */
    public synchronized boolean deliverMergedBatchToLegacy(String clientId, String consumerGroup, long maxBytes) {
        if (!readinessService.isLegacyConsumerReady(clientId)) {
            log.debug("Legacy delivery blocked until READY_ACK: clientId={}, group={}", clientId, consumerGroup);
            return false;
        }

        if (pendingAckStore.getPendingBatch(clientId) != null) {
            log.debug("Legacy delivery blocked by pending ACK: clientId={}, group={}", clientId, consumerGroup);
            return false;
        }

        List<String> topics = getLegacyConsumersForClient(clientId).stream()
                .map(RemoteConsumer::getTopic)
                .distinct()
                .toList();

        if (topics.isEmpty()) {
            log.debug("Legacy delivery skipped: no legacy topics registered for clientId={}, group={}",
                    clientId, consumerGroup);
            return false;
        }

        try {
            MergedBatch batch = legacyDeliveryManager.buildMergedBatch(topics, consumerGroup, maxBytes);
            if (batch.isEmpty()) {
                return false;
            }

            byte[] payload = objectMapper.writeValueAsBytes(batch.getMessages());
            BrokerMessage batchMessage = new BrokerMessage(
                    BrokerMessage.MessageType.BATCH_HEADER,
                    System.currentTimeMillis(),
                    payload
            );

            Timer.Sample deliverySample = metrics.startConsumerDeliveryTimer();

            // Store batch BEFORE sending to prevent race: consumer ACKs before putPendingBatch is called,
            // causing ACK handler to find no pending batch and the batch getting stuck in the store permanently.
            pendingAckStore.putPendingBatch(clientId, batch);
            pendingAckStore.recordSendTime(clientId, System.currentTimeMillis());
            pendingAckStore.startTimer(clientId, deliverySample);
            metrics.recordBatchSize(batch.getMessageCount());

            for (String topic : batch.getMaxOffsetPerTopic().keySet()) {
                metrics.startPendingAck(topic, consumerGroup);
            }

            try {
                server.send(clientId, batchMessage).get();
            } catch (Exception sendEx) {
                // Send failed — remove the pre-stored batch so delivery isn't permanently blocked
                pendingAckStore.removePendingBatch(clientId);
                pendingAckStore.removeTimer(clientId);
                pendingAckStore.removeClient(clientId);
                throw sendEx;
            }

            // Schedule ACK timeout: if the consumer never ACKs, unblock delivery after the timeout window.
            // Use send timestamp (not the batch object) as the identity check to avoid retaining the
            // ~1MB MergedBatch in the lambda closure for the full 60-second timeout window.
            final long batchSendTime = pendingAckStore.getSendTime(clientId);
            consumerScheduler.schedule(() -> {
                if (pendingAckStore.getSendTime(clientId) == batchSendTime) {
                    MergedBatch pending = pendingAckStore.getPendingBatch(clientId);
                    if (pending != null) {
                        log.warn("Legacy batch ACK timeout: clientId={}, group={} — clearing blocked pending batch",
                                clientId, consumerGroup);
                        pendingAckStore.removeClient(clientId);
                        for (String t : pending.getMaxOffsetPerTopic().keySet()) {
                            metrics.completePendingAck(t, consumerGroup);
                        }
                    }
                }
            }, legacyAckTimeoutMs, TimeUnit.MILLISECONDS);

            log.info("Sent legacy merged batch: clientId={}, group={}, messages={}, bytes={}, topics={}",
                    clientId, consumerGroup, batch.getMessageCount(), batch.getTotalBytes(),
                    batch.getMaxOffsetPerTopic().keySet());
            return true;
        } catch (Exception e) {
            log.error("Failed to send legacy merged batch: clientId={}, group={}", clientId, consumerGroup, e);
            metrics.recordConsumerFailure(clientId, topics.get(0), consumerGroup);
            return false;
        }
    }

    // ==================== ACK HANDLING ====================

    /**
     * Handle BATCH_ACK from modern consumer.
     */
    public void handleBatchAck(String clientId, String topic, String group) {
        ackService.handleModernBatchAck(clientId, topic, group);
    }

    /**
     * Handle BATCH_ACK from legacy consumer.
     */
    public void handleLegacyBatchAck(String clientId, String group) {
        ackService.handleLegacyBatchAck(clientId, group);
    }

    // ==================== CONSUMER READINESS (READY_ACK) ====================

    /**
     * Mark legacy consumer as ready (received READY_ACK).
     */
    public void markLegacyConsumerReady(String clientId) {
        readinessService.markLegacyConsumerReady(clientId);
    }

    /**
     * Mark modern consumer topic as ready (received READY_ACK for topic).
     */
    public void markModernConsumerTopicReady(String clientId, String topic, String group) {
        readinessService.markModernConsumerTopicReady(clientId, topic, group);
    }

    /**
     * Check if legacy consumer is ready.
     */
    public boolean isLegacyConsumerReady(String clientId) {
        return readinessService.isLegacyConsumerReady(clientId);
    }

    /**
     * Check if modern consumer topic is ready.
     */
    public boolean isModernConsumerTopicReady(String clientId, String topic, String group) {
        return readinessService.isModernConsumerTopicReady(clientId, topic, group);
    }

    /**
     * Send READY message to all consumers on startup.
     */
    public void sendStartupReadyToAllConsumers() {
        for (RemoteConsumer consumer : getAllConsumers()) {
            if (consumer.isLegacy()) {
                sendStartupReadyToLegacyConsumer(consumer.getClientId());
            } else {
                sendStartupReadyToModernConsumer(consumer.getClientId(), consumer.getTopic(), consumer.getGroup());
            }
        }
    }

    /**
     * Send READY message to legacy consumer.
     */
    public void sendStartupReadyToLegacyConsumer(String clientId) {
        try {
            BrokerMessage readyMessage = new BrokerMessage(
                    BrokerMessage.MessageType.READY,
                    System.currentTimeMillis(),
                    new byte[0]
            );
            server.send(clientId, readyMessage).get();
            log.info("Sent READY to legacy consumer: {}", clientId);

            // Schedule retry if no ACK received
            readinessService.scheduleReadyRetry(clientId, null, null, 0);
        } catch (Exception e) {
            log.error("Failed to send READY to legacy consumer {}", clientId, e);
        }
    }

    /**
     * Send READY message to modern consumer for specific topic.
     */
    public void sendStartupReadyToModernConsumer(String clientId, String topic, String group) {
        try {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            BrokerMessage readyMessage = new BrokerMessage(
                    BrokerMessage.MessageType.READY,
                    System.currentTimeMillis(),
                    topicBytes
            );
            server.send(clientId, readyMessage).get();
            log.info("Sent READY to modern consumer: {}:{}:{}", clientId, topic, group);

            // Schedule retry if no ACK received
            readinessService.scheduleReadyRetry(clientId, topic, group, 0);
        } catch (Exception e) {
            log.error("Failed to send READY to modern consumer {}:{}:{}", clientId, topic, group, e);
        }
    }

    /**
     * Broadcast RESET message to all consumers of a topic for refresh coordination.
     */
    public void broadcastResetToTopic(String topic) {
        Set<String> clientIds = getConsumersForTopic(topic);
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);

        BrokerMessage resetMsg = new BrokerMessage(
                BrokerMessage.MessageType.RESET,
                System.currentTimeMillis(),
                payload
        );

        for (String clientId : clientIds) {
            try {
                server.send(clientId, resetMsg).get();
                log.info("Sent RESET to consumer {} for topic {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send RESET to consumer {} for topic {}", clientId, topic, e);
            }
        }

        log.info("Broadcast RESET to {} consumers for topic: {}", clientIds.size(), topic);
    }

    /**
     * Broadcast READY message to all consumers of a topic for refresh coordination.
     */
    public void broadcastReadyToTopic(String topic) {
        Set<String> clientIds = getConsumersForTopic(topic);
        byte[] payload = topic.getBytes(StandardCharsets.UTF_8);

        BrokerMessage readyMsg = new BrokerMessage(
                BrokerMessage.MessageType.READY,
                System.currentTimeMillis(),
                payload
        );

        for (String clientId : clientIds) {
            try {
                server.send(clientId, readyMsg).get();
                log.info("Sent READY to consumer {} for topic {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send READY to consumer {} for topic {}", clientId, topic, e);
            }
        }

        log.info("Broadcast READY to {} consumers for topic: {}", clientIds.size(), topic);
    }

    /**
     * Send a refresh READY (with topic payload) to a specific consumer.
     * Used when a consumer connects after the refresh has already entered READY_SENT state.
     */
    public void sendRefreshReadyToConsumer(String clientId, String topic) {
        byte[] topicPayload = topic.getBytes(StandardCharsets.UTF_8);
        BrokerMessage readyMsg = new BrokerMessage(
                BrokerMessage.MessageType.READY,
                System.currentTimeMillis(),
                topicPayload
        );
        try {
            server.send(clientId, readyMsg).get();
            log.info("Sent refresh READY to late-connecting consumer {} for topic {}", clientId, topic);
        } catch (Exception e) {
            log.error("Failed to send refresh READY to consumer {} for topic {}", clientId, topic, e);
        }
    }

    /**
     * Send READY to consumers that have ACKed RESET during refresh.
     *
     * Sends READY with topic as payload so LegacyConnectionState can build the structured
     * READY_ACK payload ([topicLen:4][topic:var][groupLen=0:4]) that ReadyAckHandler requires
     * to route the ACK to the refresh coordinator instead of the startup path.
     */
    public void sendReadyToAckedConsumers(String topic, Set<String> ackedGroupTopics) {
        byte[] topicPayload = topic.getBytes(StandardCharsets.UTF_8);
        BrokerMessage readyMsg = new BrokerMessage(
                BrokerMessage.MessageType.READY,
                System.currentTimeMillis(),
                topicPayload
        );

        for (String clientId : getConsumersForTopic(topic)) {
            for (RemoteConsumer consumer : registrationService.getConsumersByClient(clientId)) {
                if (consumer.getTopic().equals(topic)) {
                    String groupTopic = consumer.getGroup() + ":" + consumer.getTopic();
                    if (ackedGroupTopics.contains(groupTopic)) {
                        try {
                            server.send(clientId, readyMsg).get();
                            log.info("Sent refresh READY to consumer {} for topic {}", clientId, topic);
                        } catch (Exception e) {
                            log.error("Failed to send refresh READY to consumer {} for topic {}", clientId, topic, e);
                        }
                    }
                }
            }
        }
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Check if consumer is legacy.
     */
    public boolean isLegacyConsumer(String consumerKey) {
        // Parse key: "clientId:topic:group"
        ConsumerKey key = ConsumerKey.parse(consumerKey);
        Optional<RemoteConsumer> consumer = registrationService.getConsumer(key);
        return consumer.map(c -> c.isLegacy()).orElse(false);
    }

    /**
     * Reset consumer offset (for manual intervention or data refresh).
     */
    public void resetConsumerOffset(String clientId, String topic, String group, long offset) {
        ConsumerKey key = ConsumerKey.of(clientId, topic, group);
        Optional<RemoteConsumer> consumerOpt = registrationService.getConsumer(key);

        if (consumerOpt.isPresent()) {
            RemoteConsumer consumer = consumerOpt.get();
            consumer.setCurrentOffset(offset);
            offsetTracker.updateOffset(group + ":" + topic, offset);
            log.info("Reset consumer offset: {}:{}:{} -> {}", clientId, topic, group, offset);
        } else {
            log.warn("Cannot reset offset for unregistered consumer: {}:{}:{}", clientId, topic, group);
        }
    }

    /**
     * Check if all consumers for a topic have caught up to storage head.
     */
    public boolean allConsumersCaughtUp(String topic, Set<String> consumerGroupTopics) {
        try {
            long storageHead = storage.getCurrentOffset(topic, 0);

            for (String groupTopic : consumerGroupTopics) {
                long consumerOffset = offsetTracker.getOffset(groupTopic);
                if (consumerOffset < storageHead) {
                    log.debug("Consumer {} not caught up: offset={}, head={}", groupTopic, consumerOffset, storageHead);
                    return false;
                }
            }

            log.debug("All {} consumers caught up for topic: {}", consumerGroupTopics.size(), topic);
            return true;
        } catch (Exception e) {
            log.error("Error checking if consumers caught up for topic {}", topic, e);
            return false;
        }
    }

    /**
     * Get consumer group:topic identifier for clientId+topic.
     *
     * @deprecated Use getConsumerGroupTopicPairs() for multi-group support
     */
    @Deprecated
    public String getConsumerGroupTopic(String clientId, String topic) {
        // Find first matching consumer (there may be multiple groups)
        for (RemoteConsumer consumer : registrationService.getConsumersByClient(clientId)) {
            if (consumer.getTopic().equals(topic)) {
                return consumer.getGroup() + ":" + consumer.getTopic();
            }
        }
        return null;
    }

    /**
     * Get list of consumer group:topic pairs for a topic (multi-group support).
     */
    public List<ConsumerGroupTopicPair> getConsumerGroupTopicPairs(String topic) {
        return registrationService.getConsumersByTopic(topic).stream()
                .map(c -> new ConsumerGroupTopicPair(c.getClientId(), c.getGroup() + ":" + c.getTopic()))
                .collect(Collectors.toList());
    }

    /**
     * Get set of group:topic identifiers for all consumers of a topic.
     *
     * @param topic Topic name
     * @return Set of "group:topic" strings for currently registered consumers
     */
    public Set<String> getGroupTopicIdentifiers(String topic) {
        return registrationService.getConsumersByTopic(topic).stream()
                .map(c -> c.getGroup() + ":" + c.getTopic())
                .collect(Collectors.toSet());
    }

    /**
     * Get all legacy consumers for a given clientId.
     *
     * Legacy consumers share the same clientId but subscribe to multiple topics.
     */
    public List<RemoteConsumer> getLegacyConsumersForClient(String clientId) {
        return registrationService.getConsumersByClient(clientId).stream()
                .filter(c -> c.isLegacy())
                .collect(Collectors.toList());
    }

    /**
     * Get formatted string of remote consumers for a topic (for debugging).
     */
    public String getRemoteConsumers(String topic) {
        List<RemoteConsumer> consumers = new ArrayList<>(registrationService.getConsumersByTopic(topic));
        if (consumers.isEmpty()) {
            return "No remote consumers for topic: " + topic;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Remote consumers for topic ").append(topic).append(": ").append(consumers.size()).append("\n");
        for (RemoteConsumer consumer : consumers) {
            sb.append("  - ").append(consumer.getClientId())
              .append(":").append(consumer.getTopic())
              .append(":").append(consumer.getGroup())
              .append(" (offset=").append(consumer.getCurrentOffset())
              .append(", legacy=").append(consumer.isLegacy())
              .append(")\n");
        }
        return sb.toString();
    }

    /**
     * Shutdown facade (cleanup resources).
     */
    public void shutdown() {
        log.info("Shutting down ConsumerRegistry...");
        // Services are managed by DI container, no manual cleanup needed
        log.info("ConsumerRegistry shutdown complete");
    }

    /**
     * Consumer group:topic pair for multi-group support.
     */
    public static class ConsumerGroupTopicPair {
        public final String clientId;
        public final String groupTopic;

        public ConsumerGroupTopicPair(String clientId, String groupTopic) {
            this.clientId = clientId;
            this.groupTopic = groupTopic;
        }

        @Override
        public String toString() {
            return clientId + " -> " + groupTopic;
        }
    }
}
