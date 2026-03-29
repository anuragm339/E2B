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
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.BrokerMessage;
import com.messaging.storage.segment.Segment;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    private final ConsumerStateService stateService;
    private final ConsumerReadinessService readinessService;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final DataRefreshMetrics dataRefreshMetrics;
    private final ConsumerRegistrationService registrationService;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService storageExecutor;
    private final long ackTimeoutMs;
    private final ConsumerEventLogger consumerLogger;

    private volatile RefreshCoordinator dataRefreshCoordinator; // Lazy injection to avoid circular dependency

    @Inject
    public BatchDeliveryService(
            NetworkServer server,
            StorageEngine storage,
            ConsumerStateService stateService,
            ConsumerReadinessService readinessService,
            ConsumerOffsetTracker offsetTracker,
            BrokerMetrics metrics,
            DataRefreshMetrics dataRefreshMetrics,
            ConsumerRegistrationService registrationService,
            @Named("consumerScheduler") ScheduledExecutorService scheduler,
            @Named("storageExecutor") ExecutorService storageExecutor,
            @Value("${broker.consumer.ack-timeout}") long ackTimeoutMs,
            ConsumerEventLogger consumerLogger) {
        this.server = server;
        this.storage = storage;
        this.stateService = stateService;
        this.readinessService = readinessService;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.dataRefreshMetrics = dataRefreshMetrics;
        this.registrationService = registrationService;
        this.scheduler = scheduler;
        this.storageExecutor = storageExecutor;
        this.ackTimeoutMs = ackTimeoutMs;
        this.consumerLogger = consumerLogger;
    }

    /**
     * Set RefreshCoordinator reference.
     */
    public void setDataRefreshCoordinator(RefreshCoordinator dataRefreshCoordinator) {
        this.dataRefreshCoordinator = dataRefreshCoordinator;
        log.info("BatchDeliveryService wired to RefreshCoordinator");
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
        Segment.BatchFileRegion batch = null;
        String traceId = null;

        try {
            // ================= STORAGE READ METRICS =================
            readSample = metrics.startStorageReadTimer();

            // Read batch from storage (using storage executor to prevent deadlock)
            final long capturedOffset = startOffset;
            batch = storageExecutor.submit(() -> {
                if (storage instanceof com.messaging.storage.filechannel.FileChannelStorageEngine) {
                    return ((com.messaging.storage.filechannel.FileChannelStorageEngine) storage)
                            .getZeroCopyBatch(consumer.getTopic(), 0, capturedOffset, batchSizeBytes);
                } else if (storage instanceof com.messaging.storage.mmap.MMapStorageEngine) {
                    return ((com.messaging.storage.mmap.MMapStorageEngine) storage)
                            .getZeroCopyBatch(consumer.getTopic(), 0, capturedOffset, batchSizeBytes);
                } else {
                    throw new IllegalStateException("StorageEngine implementation does not support " +
                            "getZeroCopyBatch(): " + storage.getClass().getName());
                }
            }).get(10, TimeUnit.MINUTES);

            metrics.stopStorageReadTimer(readSample);
            metrics.recordStorageRead();

            log.debug("deliverBatch: Batch read complete for {}, recordCount={}, fileRegion={}",
                     deliveryKeyStr, batch.recordCount, (batch.fileRegion != null ? "present" : "NULL"));

            if (batch.recordCount == 0 || batch.fileRegion == null) {
                log.debug("deliverBatch: EMPTY BATCH (recordCount={}, fileRegion={}) for {}, startOffset={}",
                         batch.recordCount, (batch.fileRegion != null ? "present" : "NULL"), deliveryKeyStr, startOffset);
                inFlight.set(false);
                return DeliveryResult.blocked("no-data");
            }

            traceId = TraceIds.newTraceId();

            // ================= BATCH VISIBILITY =================
            metrics.recordBatchSize(batch.recordCount);

            // ================= OFFSET RESERVATION =================
            long originalOffset = startOffset;
            long nextOffset = batch.lastOffset + 1;
            consumer.setCurrentOffset(nextOffset);
            stateService.setPendingOffset(deliveryKey, nextOffset);
            stateService.setFromOffset(deliveryKey, startOffset);  // persisted for ACK-time msgKey lookup

            LogContext startedContext = LogContext.builder()
                    .traceId(traceId)
                    .clientId(consumer.getClientId())
                    .topic(consumer.getTopic())
                    .consumerGroup(consumer.getGroup())
                    .offset(startOffset)
                    .custom("messageCount", batch.recordCount)
                    .custom("bytes", batch.totalBytes)
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

            log.warn("⏱️ BATCH_SENT to {} at T=0ms, startOffset={}, recordCount={}, bytes={}, ackTimeoutConfigured={}ms, traceId={}",
                     deliveryKeyStr, startOffset, batch.recordCount, batch.totalBytes, ackTimeoutMs, traceId);

            ScheduledFuture<?> timeoutFuture = scheduler.schedule(() -> {
                String timeoutTraceId = stateService.getTraceId(deliveryKey);
                if (stateService.getPendingOffset(deliveryKey) != null) {
                    stateService.clearPendingOffset(deliveryKey);
                    stateService.clearFromOffset(deliveryKey);
                    stateService.recordBatchSendTime(deliveryKey, 0); // Clear timestamp

                    long pendingDuration = System.currentTimeMillis() - pendingStartTime;
                    log.warn("⏰ ACK_TIMEOUT for {} after {}ms, reverting offset from {} to {}, traceId={}",
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
                            batch.totalBytes,
                            batch.recordCount,
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
                    .custom("messageCount", batch.recordCount)
                    .custom("bytes", batch.totalBytes)
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
                log.warn("Transient failure for {}, KEEPING pending offset to prevent rapid retries. " +
                        "Will wait for ACK or timeout ({}ms). consecutiveFailures={}",
                        deliveryKeyStr, ackTimeoutMs, consumer.getConsecutiveFailures());
            }

            // Record failed transfer metrics
            if (batch != null) {
                metrics.recordConsumerTransferFailed(
                    consumer.getClientId(),
                    consumer.getTopic(),
                    consumer.getGroup(),
                    batch.recordCount,
                    batch.totalBytes
                );
            }

            return DeliveryResult.failure(e.getMessage());
        }
    }

    /**
     * Send batch to consumer using zero-copy transfer.
     */
    private void sendBatchToConsumer(RemoteConsumer consumer, Segment.BatchFileRegion batchRegion, long startOffset)
            throws Exception {

        if (batchRegion.fileRegion == null) {
            log.debug("No data to send for consumer {}", consumer.getClientId());
            return;
        }

        // Step 1: Create and send header message with batch metadata
        byte[] topicBytes = consumer.getTopic().getBytes(StandardCharsets.UTF_8);
        int topicLen = topicBytes.length;
        byte[] groupBytes = consumer.getGroup().getBytes(StandardCharsets.UTF_8);
        int groupLen = groupBytes.length;

        // Header format: [recordCount:4][totalBytes:8][topicLen:4][topic:var][groupLen:4][group:var]
        ByteBuffer headerBuffer = ByteBuffer.allocate(12 + 4 + topicLen + 4 + groupLen);
        headerBuffer.putInt(batchRegion.recordCount);
        headerBuffer.putLong(batchRegion.totalBytes);
        headerBuffer.putInt(topicLen);
        headerBuffer.put(topicBytes);
        headerBuffer.putInt(groupLen);
        headerBuffer.put(groupBytes);
        headerBuffer.flip();

        byte[] header = new byte[12 + 4 + topicLen + 4 + groupLen];
        headerBuffer.get(header);

        // Send header using BATCH_HEADER message type
        BrokerMessage headerMsg = new BrokerMessage(
            BrokerMessage.MessageType.BATCH_HEADER,
            System.currentTimeMillis(),
            header
        );

        log.debug("Sending BATCH_HEADER to consumer {}: topic={}, group={}, recordCount={}, totalBytes={}",
                 consumer.getClientId(), consumer.getTopic(), consumer.getGroup(), batchRegion.recordCount, batchRegion.totalBytes);

        long timeoutSeconds = 1 + (batchRegion.totalBytes / (1024 * 1024) * 10);
        server.send(consumer.getClientId(), headerMsg).get(timeoutSeconds, TimeUnit.SECONDS);

        // Step 2: Send FileRegion for true zero-copy transfer
        log.debug("Sending zero-copy FileRegion: position={}, count={}, records={}, bytes={}, consumer={}, topic={}, startOffset={}",
                 batchRegion.fileRegion.position(), batchRegion.fileRegion.count(),
                 batchRegion.recordCount, batchRegion.totalBytes, consumer.getClientId(), consumer.getTopic(), startOffset);

        try {
            server.sendFileRegion(consumer.getClientId(), batchRegion.fileRegion)
                  .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            // CRITICAL: BATCH_HEADER was already sent — consumer decoder has transitioned to
            // READING_ZERO_COPY_BATCH and is waiting for expectedTotalBytes that will never arrive.
            // Close the connection so the consumer reconnects and decoder resets to initial state.
            log.error("FileRegion send failed after BATCH_HEADER was already sent for consumer {} topic {}. " +
                      "Closing connection to reset consumer decoder state.",
                      consumer.getClientId(), consumer.getTopic(), e);
            server.closeConnection(consumer.getClientId());
            throw e;
        }

        log.debug("Sent zero-copy batch to consumer {}: recordCount={}, bytes={}, startOffset={}, lastOffset={}",
                 consumer.getClientId(), batchRegion.recordCount, batchRegion.totalBytes, startOffset, batchRegion.lastOffset);

        // Record metrics for messages and bytes sent
        metrics.recordConsumerBatchSent(consumer.getClientId(), consumer.getTopic(), consumer.getGroup(), batchRegion.recordCount, batchRegion.totalBytes);
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
