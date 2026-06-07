package com.messaging.broker.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.messaging.broker.consumer.*;
import com.messaging.broker.legacy.LegacyConsumerDeliveryManager;
import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.broker.monitoring.DataRefreshMetrics;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Unified entry point for consumer registration, delivery, readiness, and ACK handling.
 */
@Singleton
public class ConsumerRegistry {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRegistry.class);
    private static final long LEGACY_PENDING_ACK_WARN_THRESHOLD_MS = 5_000L;
    private static final long LEGACY_BLOCKED_WARN_INTERVAL_MS = 10_000L;
    private static final long NETWORK_SEND_TIMEOUT_SECONDS = 10L;

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
    private final ConcurrentHashMap<String, AtomicLong> legacyBlockedWarnTime = new ConcurrentHashMap<>();

    private volatile AdaptiveBatchDeliveryManager adaptiveDeliveryManager; // Lazy injection
    private volatile RefreshCoordinator refreshCoordinator; // Lazy injection (avoids circular dep)
    private final DataRefreshMetrics dataRefreshMetrics;

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
            DataRefreshMetrics dataRefreshMetrics,
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
        this.dataRefreshMetrics = dataRefreshMetrics;
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
        log.info("event=consumer_registry.adaptive_delivery_wired");
    }

    public void setRefreshCoordinator(RefreshCoordinator coordinator) {
        this.refreshCoordinator = coordinator;
        log.info("event=consumer_registry.refresh_coordinator_wired");
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
            java.util.concurrent.Future<?> deliveryTask = consumer.getDeliveryTask();
            if (deliveryTask != null) {
                deliveryTask.cancel(false);
                consumer.setDeliveryTask(null);
            }
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

    public boolean isRegistered(RemoteConsumer consumer) {
        return registrationService.getConsumer(
                ConsumerKey.of(consumer.getClientId(), consumer.getTopic(), consumer.getGroup()))
                .isPresent();
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
     *
     * Previously this method was {@code synchronized}, which blocked the entire ConsumerRegistry
     * instance (including registration and ACK handling) for the duration of a blocking network
     * send. It also serialized all legacy deliveries globally across all clients.
     *
     * Replaced with a lock-free approach:
     *   1. A non-atomic fast-path check (readiness, pending ACK) avoids building the batch needlessly.
     *   2. {@code putPendingBatchIfAbsent()} is the single atomic gate: only the thread that wins
     *      the CAS-like putIfAbsent proceeds to the network send. Any concurrent caller sees the
     *      slot occupied and returns false — no lock held during I/O.
     */
    public boolean deliverMergedBatchToLegacy(String clientId, String consumerGroup, long maxBytes) {
        if (!readinessService.isLegacyConsumerReady(clientId)) {
            metrics.recordLegacyDeliveryBlocked(consumerGroup, "not_ready");
            log.debug("Legacy delivery blocked until READY_ACK: clientId={}, group={}", clientId, consumerGroup);
            return false;
        }

        // Non-atomic fast-path: if a batch is already pending, skip building the next one.
        // The authoritative gate is putPendingBatchIfAbsent() below.
        MergedBatch pendingBatch = pendingAckStore.getPendingBatch(clientId);
        if (pendingBatch != null) {
            metrics.recordLegacyDeliveryBlocked(consumerGroup, "pending_ack");
            logLegacyPendingAckBlocked(clientId, consumerGroup, "pending_ack", pendingBatch);
            return false;
        }

        List<String> topics = getLegacyConsumersForClient(clientId).stream()
                .map(RemoteConsumer::getTopic)
                .distinct()
                .toList();

        if (topics.isEmpty()) {
            metrics.recordLegacyDeliveryBlocked(consumerGroup, "no_topics");
            log.debug("Legacy delivery skipped: no legacy topics registered for clientId={}, group={}",
                    clientId, consumerGroup);
            return false;
        }

        try {
            MergedBatch batch = legacyDeliveryManager.buildMergedBatch(topics, consumerGroup, maxBytes);
            if (batch.isEmpty()) {
                metrics.recordLegacyDeliveryBlocked(consumerGroup, "empty_batch");
                log.info("event=legacy_delivery.empty_merged_batch clientId={} group={} topicsRequested={} maxBytes={}",
                        clientId, consumerGroup, topics.size(), maxBytes);
                return false;
            }

            byte[] payload = objectMapper.writeValueAsBytes(batch.getMessages());
            BrokerMessage batchMessage = new BrokerMessage(
                    BrokerMessage.MessageType.BATCH_HEADER,
                    System.currentTimeMillis(),
                    payload
            );

            Timer.Sample deliverySample = metrics.startConsumerDeliveryTimer();

            // Reserve all ACK-visible state before the send so an immediate ACK can claim one
            // immutable generation without observing partially initialized maps.
            long sendTime = System.currentTimeMillis();
            long deliveryGeneration =
                    pendingAckStore.reservePendingBatch(clientId, batch, deliverySample, sendTime);
            if (deliveryGeneration < 0) {
                metrics.recordLegacyDeliveryBlocked(consumerGroup, "slot_taken");
                logLegacyPendingAckBlocked(clientId, consumerGroup, "slot_taken",
                        pendingAckStore.getPendingBatch(clientId));
                return false;
            }
            metrics.recordBatchSize(batch.getMessageCount());
            metrics.recordLegacyBatchSent(consumerGroup, batch.getMessageCount(), batch.getMaxOffsetPerTopic().size());

            for (String topic : batch.getMaxOffsetPerTopic().keySet()) {
                metrics.startPendingAck(topic, consumerGroup);
            }

            try {
                awaitSend(server.send(clientId, batchMessage));
            } catch (Exception sendEx) {
                pendingAckStore.claimPendingDelivery(clientId, deliveryGeneration);
                metrics.clearLegacyPendingBatch(consumerGroup);
                log.warn("event=legacy_batch.send_failed clientId={} group={} messageCount={} bytes={} topicOffsets={} error={}",
                        clientId, consumerGroup, batch.getMessageCount(), batch.getTotalBytes(),
                        batch.getMaxOffsetPerTopic(), sendEx.toString());
                throw sendEx;
            }

            // The timeout can only claim the generation it was created for. A stale timer can
            // never clear a newer batch, even when two sends share the same millisecond timestamp.
            final long batchSendTime = sendTime;
            consumerScheduler.schedule(() -> {
                PendingLegacyDelivery timedOut =
                        pendingAckStore.claimPendingDelivery(clientId, deliveryGeneration);
                if (timedOut != null) {
                    MergedBatch pending = timedOut.batch();
                    long pendingAgeMs = Math.max(0, System.currentTimeMillis() - timedOut.sendTime());
                    log.warn("event=legacy_batch.ack_timeout clientId={} group={} pendingAckAgeMs={} messageCount={} topicCount={} topicOffsets={} action=clear_pending_batch",
                            clientId, consumerGroup, pendingAgeMs, pending.getMessageCount(),
                            pending.getMaxOffsetPerTopic().size(), pending.getMaxOffsetPerTopic());
                    metrics.recordLegacyBatchTimeout(consumerGroup);
                    for (String t : pending.getMaxOffsetPerTopic().keySet()) {
                        metrics.completePendingAck(t, consumerGroup);
                    }
                }
            }, legacyAckTimeoutMs, TimeUnit.MILLISECONDS);

            log.debug("Sent legacy merged batch: clientId={}, group={}, messages={}, bytes={}, topics={}",
                    clientId, consumerGroup, batch.getMessageCount(), batch.getTotalBytes(),
                    batch.getMaxOffsetPerTopic().keySet());

            Map<String, Long> bytesPerTopic = batch.getBytesPerTopic();
            Map<String, Integer> msgCountPerTopic = batch.getMessageCountPerTopic();
            for (String topic : batch.getMaxOffsetPerTopic().keySet()) {
                metrics.recordConsumerBatchSent(
                        clientId,
                        topic,
                        consumerGroup,
                        msgCountPerTopic.getOrDefault(topic, 0),
                        bytesPerTopic.getOrDefault(topic, 0L)
                );
            }

            // Record bytes/messages transferred per topic for refresh metrics (during REPLAYING state only)
            if (refreshCoordinator != null) {
                for (String topic : batch.getMaxOffsetPerTopic().keySet()) {
                    RefreshContext refreshCtx = refreshCoordinator.getRefreshStatus(topic);
                    if (refreshCtx != null && refreshCtx.getState() == RefreshState.REPLAYING) {
                        long topicBytes = batch.getBytesPerTopic().getOrDefault(topic, 0L);
                        long topicMessages = batch.getMessageCountPerTopic().getOrDefault(topic, 0);
                        dataRefreshMetrics.recordDataTransferred(
                                topic,
                                consumerGroup,
                                topicBytes,
                                topicMessages,
                                refreshCtx.getRefreshId(),
                                refreshCtx.getRefreshType()
                        );
                    }
                }
            }

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

    private void logLegacyPendingAckBlocked(String clientId, String consumerGroup, String reason, MergedBatch pendingBatch) {
        if (pendingBatch == null) {
            log.debug("Legacy delivery blocked: clientId={}, group={}, reason={}, pendingBatch=null",
                    clientId, consumerGroup, reason);
            return;
        }

        long sendTime = pendingAckStore.getSendTime(clientId);
        long pendingAgeMs = sendTime > 0 ? Math.max(0, System.currentTimeMillis() - sendTime) : -1L;
        if (pendingAgeMs < LEGACY_PENDING_ACK_WARN_THRESHOLD_MS || !shouldWarnLegacyBlocked(consumerGroup)) {
            log.debug("Legacy delivery blocked: clientId={}, group={}, reason={}, pendingAckAgeMs={}, messageCount={}, topicOffsets={}",
                    clientId, consumerGroup, reason, pendingAgeMs,
                    pendingBatch.getMessageCount(), pendingBatch.getMaxOffsetPerTopic());
            return;
        }

        log.warn("event=legacy_delivery.blocked clientId={} group={} reason={} pendingAckAgeMs={} messageCount={} topicCount={} topicOffsets={}",
                clientId, consumerGroup, reason, pendingAgeMs, pendingBatch.getMessageCount(),
                pendingBatch.getMaxOffsetPerTopic().size(), pendingBatch.getMaxOffsetPerTopic());
    }

    private boolean shouldWarnLegacyBlocked(String consumerGroup) {
        String key = consumerGroup == null || consumerGroup.isBlank() ? "unknown" : consumerGroup;
        long now = System.currentTimeMillis();
        AtomicLong lastLogged = legacyBlockedWarnTime.computeIfAbsent(key, ignored -> new AtomicLong(0));
        long previous = lastLogged.get();
        return now - previous >= LEGACY_BLOCKED_WARN_INTERVAL_MS && lastLogged.compareAndSet(previous, now);
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
            awaitSend(server.send(clientId, readyMessage));
            log.debug("Sent READY to legacy consumer: {}", clientId);

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
            awaitSend(server.send(clientId, readyMessage));
            log.debug("Sent READY to modern consumer: {}:{}:{}", clientId, topic, group);

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
                awaitSend(server.send(clientId, resetMsg));
                log.debug("Sent RESET to consumer {} for topic {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send RESET to consumer {} for topic {}", clientId, topic, e);
            }
        }

        log.info("event=refresh.reset_broadcast topic={} consumerCount={}", topic, clientIds.size());
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
                awaitSend(server.send(clientId, readyMsg));
                log.debug("Sent READY to consumer {} for topic {}", clientId, topic);
            } catch (Exception e) {
                log.error("Failed to send READY to consumer {} for topic {}", clientId, topic, e);
            }
        }

        log.info("event=refresh.ready_broadcast topic={} consumerCount={}", topic, clientIds.size());
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
            awaitSend(server.send(clientId, readyMsg));
            log.info("event=refresh.ready_sent_late_joiner clientId={} topic={}", clientId, topic);
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
                            awaitSend(server.send(clientId, readyMsg));
                            log.info("event=refresh.ready_sent clientId={} topic={}", clientId, topic);
                        } catch (Exception e) {
                            log.error("Failed to send refresh READY to consumer {} for topic {}", clientId, topic, e);
                        }
                    }
                }
            }
        }
    }

    // ==================== UTILITY METHODS ====================

    private void awaitSend(CompletableFuture<Void> future) throws Exception {
        try {
            future.get(NETWORK_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        }
    }

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
            log.info("event=consumer.offset_reset clientId={} topic={} group={} offset={}", clientId, topic, group, offset);
        } else {
            log.warn("event=consumer.offset_reset_skipped clientId={} topic={} group={} reason=unregistered_consumer", clientId, topic, group);
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
     * Get committed offset for a group:topic pair.
     */
    public long getCommittedOffset(String consumerGroupTopic) {
        return offsetTracker.getOffset(consumerGroupTopic);
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

    public Collection<RemoteConsumer> getConsumersByClient(String clientId) {
        return registrationService.getConsumersByClient(clientId);
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
        log.info("event=consumer_registry.shutdown_started");
        // Services are managed by DI container, no manual cleanup needed
        log.info("event=consumer_registry.shutdown_complete");
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
