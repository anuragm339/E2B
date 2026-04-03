package com.messaging.broker.consumer;

import com.messaging.broker.monitoring.ConsumerEventLogger;
import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.monitoring.TraceIds;
import com.messaging.broker.consumer.ConsumerDeliveryService;
import com.messaging.broker.consumer.ConsumerRegistrationService;
import com.messaging.broker.consumer.ConsumerReadinessService;
import com.messaging.broker.consumer.ConsumerStateService;
import com.messaging.broker.consumer.DeliveryStateStore;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.broker.monitoring.DataRefreshMetrics;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.common.api.BatchReadableStorage;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.DeliveryBatch;
import com.messaging.common.model.BrokerMessage;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates batch delivery with flow control, zero-copy transfer, and error handling.
 */
@Singleton
public class BatchDeliveryService implements ConsumerDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(BatchDeliveryService.class);
    private static final int MAX_CONSECUTIVE_FAILURES = 10;

    private final NetworkServer server;
    private final StorageEngine storage;
    private final BatchReadableStorage batchStorage;
    private final ConsumerStateService stateService;
    private final ConsumerReadinessService readinessService;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final DataRefreshMetrics dataRefreshMetrics;
    private final ConsumerRegistrationService registrationService;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService storageExecutor;
    private final long ackTimeoutMs;
    private final long sendTimeoutBaseSeconds;
    private final long sendTimeoutPerMbSeconds;
    private final ConsumerEventLogger consumerLogger;

    private volatile RefreshCoordinator dataRefreshCoordinator; // Lazy injection to avoid circular dependency

    @Inject
    public BatchDeliveryService(
            NetworkServer server,
            StorageEngine storage,
            BatchReadableStorage batchStorage,
            ConsumerStateService stateService,
            ConsumerReadinessService readinessService,
            ConsumerOffsetTracker offsetTracker,
            BrokerMetrics metrics,
            DataRefreshMetrics dataRefreshMetrics,
            ConsumerRegistrationService registrationService,
            @Named("consumerScheduler") ScheduledExecutorService scheduler,
            @Named("storageExecutor") ExecutorService storageExecutor,
            @Value("${broker.consumer.ack-timeout}") long ackTimeoutMs,
            @Value("${broker.consumer.send-timeout-base-seconds:1}") long sendTimeoutBaseSeconds,
            @Value("${broker.consumer.send-timeout-per-mb-seconds:2}") long sendTimeoutPerMbSeconds,
            ConsumerEventLogger consumerLogger) {
        this.server = server;
        this.storage = storage;
        this.batchStorage = batchStorage;
        this.stateService = stateService;
        this.readinessService = readinessService;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.dataRefreshMetrics = dataRefreshMetrics;
        this.registrationService = registrationService;
        this.scheduler = scheduler;
        this.storageExecutor = storageExecutor;
        this.ackTimeoutMs = ackTimeoutMs;
        this.sendTimeoutBaseSeconds = sendTimeoutBaseSeconds;
        this.sendTimeoutPerMbSeconds = sendTimeoutPerMbSeconds;
        this.consumerLogger = consumerLogger;
    }

    /**
     * Set RefreshCoordinator reference.
     */
    public void setDataRefreshCoordinator(RefreshCoordinator dataRefreshCoordinator) {
        this.dataRefreshCoordinator = dataRefreshCoordinator;
        log.info("event=batch_delivery.refresh_coordinator_wired");
    }

    @Override
    public DeliveryResult deliverBatch(RemoteConsumer consumer, long batchSizeBytes) {
        DeliveryKey deliveryKey = DeliveryKey.of(consumer.getGroup(), consumer.getTopic());
        String deliveryKeyStr = consumer.getClientId() + " -> " + deliveryKey;

        if (!consumer.isLegacy() &&
                !readinessService.isModernConsumerTopicReady(consumer.getClientId(), consumer.getTopic(), consumer.getGroup())) {
            log.debug("deliverBatch: BLOCKED (waiting for READY_ACK) for {}", deliveryKeyStr);
            return DeliveryResult.blocked("not-ready");
        }

        // Gate 1: Check in-flight (per group:topic)
        AtomicBoolean inFlight = stateService.markInFlight(deliveryKey);
        if (!inFlight.compareAndSet(false, true)) {
            log.debug("deliverBatch: Gate 1 BLOCKED (in-flight) for {}", deliveryKeyStr);
            return DeliveryResult.blocked("in-flight");
        }

        // Gate 2: Check pending ACK
        if (stateService.getPendingOffset(deliveryKey) != null) {
            log.debug("deliverBatch: Gate 2 BLOCKED (pending ACK) for {}, pendingOffset={}",
                     deliveryKeyStr, stateService.getPendingOffset(deliveryKey));
            inFlight.set(false);
            return DeliveryResult.blocked("pending-ack");
        }

        // Gate 3: Check maximum consecutive failures
        if (consumer.getConsecutiveFailures() >= MAX_CONSECUTIVE_FAILURES) {
            log.error("Consumer {} has exceeded max consecutive failures ({}), unregistering",
                     deliveryKeyStr, MAX_CONSECUTIVE_FAILURES);
            inFlight.set(false);
            registrationService.unregisterConsumer(consumer.getClientId());
            return DeliveryResult.failure("max-failures-exceeded");
        }

        // Gate 4: Check exponential backoff after failures
        long backoffDelay = consumer.getBackoffDelay();
        if (backoffDelay > 0) {
            long timeSinceLastFailure = System.currentTimeMillis() - consumer.getLastFailureTime();
            if (timeSinceLastFailure < backoffDelay) {
                long remainingDelay = backoffDelay - timeSinceLastFailure;
                log.debug("deliverBatch: Gate 4 BLOCKED (backoff) for {}, consecutiveFailures={}, remainingDelay={}ms",
                         deliveryKeyStr, consumer.getConsecutiveFailures(), remainingDelay);
                inFlight.set(false);
                return DeliveryResult.blocked("backoff");
            }
        }

        log.debug("deliverBatch: Gates passed, proceeding with batch read for {}", deliveryKeyStr);

        // Record retry metric when a previously-failed consumer is being retried
        if (consumer.getConsecutiveFailures() > 0) {
            metrics.recordConsumerRetry(consumer.getClientId(), consumer.getTopic(), consumer.getGroup());
        }

        long startOffset = consumer.getCurrentOffset();
        Timer.Sample readSample = null;
        Timer.Sample deliverySample = null;
        DeliveryBatch batch = null;
        String traceId = null;

        try {
            // ================= STORAGE READ METRICS =================
            readSample = metrics.startStorageReadTimer();

            // Read batch from storage (using storage executor to prevent deadlock)
            final long capturedOffset = startOffset;
            batch = storageExecutor.submit(() ->
                batchStorage.getBatch(consumer.getTopic(), 0, capturedOffset, batchSizeBytes)
            ).get(10, TimeUnit.MINUTES);

            metrics.stopStorageReadTimer(readSample);
            metrics.recordStorageRead();

            log.debug("deliverBatch: Batch read complete for {}, recordCount={}",
                     deliveryKeyStr, batch.getRecordCount());

            if (batch.isEmpty()) {
                log.debug("deliverBatch: EMPTY BATCH for {}, startOffset={}", deliveryKeyStr, startOffset);
                try { batch.close(); } catch (IOException ignored) {}
                inFlight.set(false);
                return DeliveryResult.blocked("no-data");
            }

            traceId = TraceIds.newTraceId();

            // ================= BATCH VISIBILITY =================
            metrics.recordBatchSize(batch.getRecordCount());

            // ================= OFFSET RESERVATION =================
            long originalOffset = startOffset;
            long nextOffset = batch.getLastOffset() + 1;
            consumer.setCurrentOffset(nextOffset);
            stateService.setPendingOffset(deliveryKey, nextOffset);
            stateService.setFromOffset(deliveryKey, startOffset);  // persisted for ACK-time msgKey lookup

            LogContext startedContext = LogContext.builder()
                    .traceId(traceId)
                    .clientId(consumer.getClientId())
                    .topic(consumer.getTopic())
                    .consumerGroup(consumer.getGroup())
                    .offset(startOffset)
                    .custom("messageCount", batch.getRecordCount())
                    .custom("bytes", batch.getTotalBytes())
                    .custom("deliveryKey", deliveryKey)
                    .build();
            consumerLogger.logBatchDeliveryStarted(startedContext);

            // ================= DELIVERY METRICS =================
            deliverySample = metrics.startConsumerDeliveryTimer();

            sendBatchToConsumer(consumer, batch, startOffset);

            stateService.recordTraceId(deliveryKey, traceId);

            // Start tracking pending ACK age for monitoring
            metrics.startPendingAck(consumer.getTopic(), consumer.getGroup());

            // ================= ACK TIMEOUT SETUP =================
            long pendingStartTime = System.currentTimeMillis();
            stateService.recordBatchSendTime(deliveryKey, pendingStartTime);

            log.debug("BATCH_SENT to {} at startOffset={}, recordCount={}, bytes={}, ackTimeoutConfigured={}ms, traceId={}",
                     deliveryKeyStr, startOffset, batch.getRecordCount(), batch.getTotalBytes(), ackTimeoutMs, traceId);

            ScheduledFuture<?> timeoutFuture = scheduler.schedule(() -> {
                String timeoutTraceId = stateService.getTraceId(deliveryKey);
                if (stateService.getPendingOffset(deliveryKey) != null) {
                    stateService.clearPendingOffset(deliveryKey);
                    stateService.clearFromOffset(deliveryKey);
                    stateService.recordBatchSendTime(deliveryKey, 0); // Clear timestamp

                    long pendingDuration = System.currentTimeMillis() - pendingStartTime;
                    log.warn("event=batch_delivery.ack_timeout deliveryKey={} pendingMs={} revertFrom={} revertTo={} traceId={}",
                             deliveryKeyStr, pendingDuration, nextOffset, originalOffset, timeoutTraceId);

                    // REVERT consumer offset to prevent delivery gap
                    consumer.setCurrentOffset(originalOffset);
                    inFlight.set(false);

                    metrics.recordAckTimeout(consumer.getTopic(), consumer.getGroup());
                    metrics.completePendingAck(consumer.getTopic(), consumer.getGroup());
                }
                stateService.clearTraceId(deliveryKey);
                stateService.cancelTimeout(deliveryKey);
            }, ackTimeoutMs, TimeUnit.MILLISECONDS);

            stateService.scheduleTimeout(deliveryKey, timeoutFuture);

            metrics.stopConsumerDeliveryTimer(
                    deliverySample,
                    consumer.getClientId(),
                    consumer.getTopic(),
                    consumer.getGroup()
            );

            // ================= DATA REFRESH METRICS (if in refresh mode) =================
            // Guard: only record during active REPLAYING state, not during the 60-second
            // post-completion cleanup window where getRefreshIdForTopic() still returns
            // non-null. Without this guard, normal post-refresh deliveries inflate the
            // bytes-transferred metric for the completed refresh.
            if (dataRefreshCoordinator != null) {
                RefreshContext refreshCtx = dataRefreshCoordinator.getRefreshStatus(consumer.getTopic());
                if (refreshCtx != null && refreshCtx.getState() == RefreshState.REPLAYING) {
                    dataRefreshMetrics.recordDataTransferred(
                            consumer.getTopic(),
                            consumer.getGroup(),
                            batch.getTotalBytes(),
                            batch.getRecordCount(),
                            refreshCtx.getRefreshId(),
                            refreshCtx.getRefreshType()
                    );
                }
            }

            // Reset failure counter on successful delivery
            consumer.resetFailures();

            // Structured logging for successful delivery
            LogContext successContext = LogContext.builder()
                    .traceId(traceId)
                    .clientId(consumer.getClientId())
                    .topic(consumer.getTopic())
                    .consumerGroup(consumer.getGroup())
                    .offset(startOffset)
                    .custom("messageCount", batch.getRecordCount())
                    .custom("bytes", batch.getTotalBytes())
                    .custom("deliveryKey", deliveryKey)
                    .build();
            consumerLogger.logBatchDeliverySucceeded(successContext);

            // Update consumer lag metric after each successful delivery
            try {
                long storageHead = storage.getCurrentOffset(consumer.getTopic(), 0);
                long lag = Math.max(0, storageHead - consumer.getCurrentOffset());
                metrics.updateConsumerLag(consumer.getClientId(), consumer.getTopic(), consumer.getGroup(), lag);

                LogContext lagContext = LogContext.builder()
                        .traceId(traceId)
                        .clientId(consumer.getClientId())
                        .topic(consumer.getTopic())
                        .consumerGroup(consumer.getGroup())
                        .custom("consumerOffset", consumer.getCurrentOffset())
                        .custom("storageOffset", storageHead)
                        .custom("lag", lag)
                        .build();
                consumerLogger.logConsumerLag(lagContext);
            } catch (Exception lagEx) {
                log.debug("Could not update consumer lag metric for {}: {}", deliveryKeyStr, lagEx.getMessage());
            }

            return DeliveryResult.success();

        } catch (Exception e) {
            String deliveryTraceId = traceId != null ? traceId : stateService.getTraceId(deliveryKey);
            // Structured logging for delivery failure
            LogContext failureContext = LogContext.builder()
                    .traceId(deliveryTraceId)
                    .clientId(consumer.getClientId())
                    .topic(consumer.getTopic())
                    .consumerGroup(consumer.getGroup())
                    .offset(startOffset)
                    .custom("error", e.getMessage())
                    .custom("consecutiveFailures", consumer.getConsecutiveFailures())
                    .custom("deliveryKey", deliveryKey)
                    .build();
            consumerLogger.logBatchDeliveryFailed(failureContext);

            metrics.recordConsumerFailure(consumer.getClientId(), consumer.getTopic(), consumer.getGroup());
            inFlight.set(false);
            stateService.clearFromOffset(deliveryKey);  // clear regardless of permanent/transient failure

            // Revert offset reservation on error
            consumer.setCurrentOffset(startOffset);

            // Record failure for exponential backoff
            consumer.recordFailure();

            // Only remove pending offset for PERMANENT failures
            boolean isPermanentFailure = consumer.getConsecutiveFailures() >= MAX_CONSECUTIVE_FAILURES;

            if (isPermanentFailure) {
                stateService.clearPendingOffset(deliveryKey);
                stateService.recordBatchSendTime(deliveryKey, 0);
                stateService.clearTraceId(deliveryKey);
                log.error("Permanent failure for {} (consecutiveFailures={}), removing pending offset and unregistering",
                         deliveryKeyStr, MAX_CONSECUTIVE_FAILURES);
                registrationService.unregisterConsumer(consumer.getClientId());
            } else {
                log.warn("event=batch_delivery.transient_failure deliveryKey={} pendingOffsetRetained=true ackTimeoutMs={} consecutiveFailures={}",
                        deliveryKeyStr, ackTimeoutMs, consumer.getConsecutiveFailures());
            }

            // Record failed transfer metrics
            if (batch != null) {
                metrics.recordConsumerTransferFailed(
                    consumer.getClientId(),
                    consumer.getTopic(),
                    consumer.getGroup(),
                    batch.getRecordCount(),
                    batch.getTotalBytes()
                );
            }

            return DeliveryResult.failure(e.getMessage());
        }
    }

    /**
     * Send batch to consumer. Transport (NettyTcpServer) takes ownership of the payload:
     * it encodes the BATCH_HEADER, transfers the payload bytes, and closes the payload
     * on success, failure, or cancellation.
     */
    private void sendBatchToConsumer(RemoteConsumer consumer, DeliveryBatch batch, long startOffset)
            throws Exception {

        long batchMb = batch.getTotalBytes() / (1024 * 1024);
        long timeoutSeconds = sendTimeoutBaseSeconds + (batchMb * sendTimeoutPerMbSeconds);

        log.debug("Sending batch to consumer {}: topic={}, group={}, recordCount={}, totalBytes={}, startOffset={}",
                 consumer.getClientId(), consumer.getTopic(), consumer.getGroup(),
                 batch.getRecordCount(), batch.getTotalBytes(), startOffset);

        // Transport owns batch from this point — NettyTcpServer closes it via deallocate()
        server.sendBatch(consumer.getClientId(), consumer.getGroup(), batch)
              .get(timeoutSeconds, TimeUnit.SECONDS);

        log.debug("Sent batch to consumer {}: recordCount={}, bytes={}, startOffset={}, lastOffset={}",
                 consumer.getClientId(), batch.getRecordCount(), batch.getTotalBytes(),
                 startOffset, batch.getLastOffset());

        metrics.recordConsumerBatchSent(consumer.getClientId(), consumer.getTopic(), consumer.getGroup(),
                batch.getRecordCount(), batch.getTotalBytes());
    }

    @Override
    public boolean canDeliver(RemoteConsumer consumer) {
        DeliveryKey deliveryKey = DeliveryKey.of(consumer.getGroup(), consumer.getTopic());

        if (!consumer.isLegacy() &&
                !readinessService.isModernConsumerTopicReady(consumer.getClientId(), consumer.getTopic(), consumer.getGroup())) {
            return false;
        }

        // Check all flow control gates
        if (stateService.isInFlight(deliveryKey)) {
            return false;
        }
        if (stateService.getPendingOffset(deliveryKey) != null) {
            return false;
        }
        if (consumer.getConsecutiveFailures() >= MAX_CONSECUTIVE_FAILURES) {
            return false;
        }

        long backoffDelay = consumer.getBackoffDelay();
        if (backoffDelay > 0) {
            long timeSinceLastFailure = System.currentTimeMillis() - consumer.getLastFailureTime();
            if (timeSinceLastFailure < backoffDelay) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void resetDeliveryState(RemoteConsumer consumer) {
        DeliveryKey deliveryKey = DeliveryKey.of(consumer.getGroup(), consumer.getTopic());
        stateService.removeDeliveryState(deliveryKey);
        consumer.resetFailures();
        log.debug("Reset delivery state for {}", deliveryKey);
    }

    @Override
    public void handleDeliverySuccess(RemoteConsumer consumer) {
        DeliveryKey deliveryKey = DeliveryKey.of(consumer.getGroup(), consumer.getTopic());
        stateService.clearInFlight(deliveryKey);
        consumer.resetFailures();
        log.debug("Delivery success for {}", deliveryKey);
    }

    @Override
    public void handleDeliveryFailure(RemoteConsumer consumer, Throwable error) {
        DeliveryKey deliveryKey = DeliveryKey.of(consumer.getGroup(), consumer.getTopic());
        consumer.recordFailure();
        metrics.recordConsumerFailure(consumer.getClientId(), consumer.getTopic(), consumer.getGroup());
        log.error("Delivery failure for {}: consecutiveFailures={}, nextBackoff={}ms",
                 deliveryKey, consumer.getConsecutiveFailures(), consumer.getBackoffDelay(), error);
    }
}
