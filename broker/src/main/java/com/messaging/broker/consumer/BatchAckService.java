package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.ConsumerEventLogger;
import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.consumer.ConsumerAckService;
import com.messaging.broker.consumer.ConsumerRegistrationService;
import com.messaging.broker.consumer.ConsumerStateService;
import com.messaging.broker.legacy.LegacyConsumerDeliveryManager;
import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.PendingAckStore;
import com.messaging.common.api.StorageEngine;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles batch acknowledgment workflow for modern and legacy consumers.
 */
@Singleton
public class BatchAckService implements ConsumerAckService {

    private static final Logger log = LoggerFactory.getLogger(BatchAckService.class);

    private final ConsumerStateService stateService;
    private final PendingAckStore pendingAckStore;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final StorageEngine storage;
    private final ConsumerRegistrationService registrationService;
    private final LegacyConsumerDeliveryManager legacyDeliveryManager;
    private final ConsumerEventLogger consumerLogger;

    @Inject
    public BatchAckService(
            ConsumerStateService stateService,
            PendingAckStore pendingAckStore,
            ConsumerOffsetTracker offsetTracker,
            BrokerMetrics metrics,
            StorageEngine storage,
            ConsumerRegistrationService registrationService,
            LegacyConsumerDeliveryManager legacyDeliveryManager,
            ConsumerEventLogger consumerLogger) {
        this.stateService = stateService;
        this.pendingAckStore = pendingAckStore;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.storage = storage;
        this.registrationService = registrationService;
        this.legacyDeliveryManager = legacyDeliveryManager;
        this.consumerLogger = consumerLogger;
    }

    @Override
    public void handleModernBatchAck(String clientId, String topic, String group) {
        DeliveryKey deliveryKey = DeliveryKey.of(group, topic);
        String deliveryKeyStr = clientId + " -> " + deliveryKey;
        String traceId = stateService.getTraceId(deliveryKey);

        // Calculate ACK latency
        long ackReceiveTime = System.currentTimeMillis();
        Long sendTime = stateService.getBatchSendTime(deliveryKey);
        long ackLatencyMs = sendTime != null && sendTime > 0 ? (ackReceiveTime - sendTime) : -1;

        // Remove pending state
        Long committedOffset = stateService.getPendingOffset(deliveryKey);
        if (committedOffset != null) {
            stateService.clearPendingOffset(deliveryKey);
        }

        // Cancel timeout
        stateService.cancelTimeout(deliveryKey);

        if (committedOffset == null) {
            log.warn("⚠️ ACK with no pending offset: {} (likely a late ACK after timeout). ACK_LATENCY={}ms. Clearing inFlight to unblock delivery. traceId={}",
                     deliveryKeyStr, ackLatencyMs, traceId);
            // Always clear inFlight on any ACK, even a late one
            stateService.recordBatchSendTime(deliveryKey, 0);
            stateService.clearInFlight(deliveryKey);
            stateService.clearTraceId(deliveryKey);
            return;
        }

        // Structured logging for ACK received
        LogContext ackContext = LogContext.builder()
                .traceId(traceId)
                .clientId(clientId)
                .topic(topic)
                .consumerGroup(group)
                .offset(committedOffset)
                .custom("ackLatencyMs", ackLatencyMs)
                .build();
        consumerLogger.logBatchAckReceived(ackContext);

        // Update offset tracker
        ConsumerKey consumerKey = ConsumerKey.of(clientId, topic, group);
        Optional<RemoteConsumer> consumerOpt = registrationService.getConsumer(consumerKey);

        if (consumerOpt.isPresent()) {
            RemoteConsumer consumer = consumerOpt.get();
            long oldOffset = consumer.getCurrentOffset();
            offsetTracker.updateOffset(group + ":" + topic, committedOffset);

            // Update metrics
            metrics.updateConsumerOffset(clientId, topic, group, committedOffset);
            metrics.updateConsumerLastAckTime(clientId, topic, group);
            metrics.updateConsumerLastDeliveryTime(clientId, topic, group);
            metrics.recordConsumerAck(clientId, topic, group);
            metrics.completePendingAck(topic, group);

            // Structured logging for offset update
            LogContext offsetContext = LogContext.builder()
                    .traceId(traceId)
                    .clientId(clientId)
                    .topic(topic)
                    .consumerGroup(group)
                    .custom("oldOffset", oldOffset)
                    .custom("newOffset", committedOffset)
                    .build();
            consumerLogger.logConsumerOffsetUpdated(offsetContext);

            // Calculate and update consumer lag
            try {
                long storageHead = storage.getCurrentOffset(topic, 0);
                long lag = Math.max(0, storageHead - committedOffset);
                metrics.updateConsumerLag(clientId, topic, group, lag);
            } catch (Exception lagEx) {
                log.debug("Could not update consumer lag metric for {}: {}", deliveryKeyStr, lagEx.getMessage());
            }
        } else {
            // Consumer was unregistered, but we can still persist the offset
            log.warn("ACK for unregistered consumer {}, persisting offset {} for group {}",
                     deliveryKeyStr, committedOffset, group);
            offsetTracker.updateOffset(group + ":" + topic, committedOffset);
        }

        // Clear in-flight status
        stateService.recordBatchSendTime(deliveryKey, 0);
        stateService.clearInFlight(deliveryKey);
        stateService.clearTraceId(deliveryKey);

        log.debug("ACK committed for {} at offset {}, traceId={}", deliveryKeyStr, committedOffset, traceId);
    }

    @Override
    public void handleLegacyBatchAck(String clientId, String group) {
        long ackReceiveTime = System.currentTimeMillis();
        long sendTime = pendingAckStore.getSendTime(clientId);
        Timer.Sample deliverySample = pendingAckStore.removeTimer(clientId);
        MergedBatch batch = pendingAckStore.removePendingBatch(clientId);
        pendingAckStore.removeClient(clientId); // clean up send time and any remaining state

        if (batch == null) {
            log.warn("⚠️ Legacy batch ACK with no pending data: clientId={}, group={} (likely a late ACK after timeout).",
                    clientId, group);
            return;
        }

        // Calculate ACK latency from stored send timestamp
        long ackLatencyMs = sendTime > 0 ? (ackReceiveTime - sendTime) : -1;

        log.info("✅ Legacy batch ACK_RECEIVED for clientId={}, group={} at T={}ms",
                clientId, group, ackLatencyMs);

        try {
            // Use the handleMergedBatchAck method from LegacyConsumerDeliveryManager
            legacyDeliveryManager.handleMergedBatchAck(group, batch);

            log.info("Legacy batch ACK committed for clientId={}, group={}, topics={}",
                    clientId, group, batch.getMaxOffsetPerTopic());

            // Record metrics for messages and bytes sent (NOW that ACK is received)
            metrics.recordBatchMessagesSent(batch.getMessageCount(), batch.getTotalBytes());

            // Update metrics for each topic using actual per-topic counts
            Map<String, Long> bytesPerTopic = batch.getBytesPerTopic();
            Map<String, Integer> msgCountPerTopic = batch.getMessageCountPerTopic();

            for (Map.Entry<String, Long> entry : batch.getMaxOffsetPerTopic().entrySet()) {
                String topic = entry.getKey();
                long offset = entry.getValue();
                long topicBytes = bytesPerTopic.getOrDefault(topic, 0L);
                int topicMessages = msgCountPerTopic.getOrDefault(topic, 0);

                // Record per-consumer batch sent metrics with actual per-topic values
                metrics.recordConsumerBatchSent(clientId, topic, group,
                        topicMessages, topicBytes);

                // Record delivery latency for this topic
                if (deliverySample != null) {
                    metrics.stopConsumerDeliveryTimer(deliverySample, clientId, topic, group);
                }

                // Update last successful delivery timestamp for stuck detection
                metrics.updateConsumerLastDeliveryTime(clientId, topic, group);

                // Update offset, ACK time, and ACK count
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

    @Override
    public boolean isAckPending(String topic, String group) {
        DeliveryKey deliveryKey = DeliveryKey.of(group, topic);
        return stateService.getPendingOffset(deliveryKey) != null;
    }

    @Override
    public void clearPendingAcks(String clientId) {
        // Clear legacy consumer ACKs
        pendingAckStore.removeClient(clientId);

        log.debug("Cleared legacy pending ACKs for client: {}", clientId);
    }
}
