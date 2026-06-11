package com.messaging.broker.consumer;

import com.messaging.broker.compaction.CompactionIndex;
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
import com.messaging.broker.monitoring.LogMdc;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.common.api.BatchReadableStorage;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.ByteArrayDeliveryBatch;
import com.messaging.common.model.DeliveryBatch;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.MessageRecord;
import io.micrometer.core.instrument.Timer;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Orchestrates batch delivery with flow control, zero-copy transfer, and error handling.
 */
@Singleton
public class BatchDeliveryService implements ConsumerDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(BatchDeliveryService.class);
    private static final int MAX_CONSECUTIVE_FAILURES = 10;
    private static final long MODERN_PENDING_ACK_WARN_THRESHOLD_MS = 5_000L;
    private static final long MODERN_BLOCKED_WARN_INTERVAL_MS = 10_000L;
    private static final long SLOW_STORAGE_READ_MS = 250L;

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
    private final long storageReadTimeoutSeconds;
    private final ConsumerEventLogger consumerLogger;
    private final CompactionIndex compactionIndex;
    private final ConcurrentHashMap<String, AtomicLong> blockedWarnTime = new ConcurrentHashMap<>();

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
            @Value("${broker.consumer.storage-read-timeout-seconds:30}") long storageReadTimeoutSeconds,
            ConsumerEventLogger consumerLogger,
            CompactionIndex compactionIndex) {
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
        this.storageReadTimeoutSeconds = Math.max(1L, storageReadTimeoutSeconds);
        this.consumerLogger = consumerLogger;
        this.compactionIndex = compactionIndex;
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
            metrics.recordConsumerDeliveryBlocked(consumer.getTopic(), consumer.getGroup(), "not-ready");
            log.debug("deliverBatch: BLOCKED (waiting for READY_ACK) for {}", deliveryKeyStr);
            return DeliveryResult.blocked("not-ready");
        }

        // Gate 1: Check in-flight (per group:topic)
        AtomicBoolean inFlight = stateService.markInFlight(deliveryKey);
        if (!inFlight.compareAndSet(false, true)) {
            metrics.recordConsumerDeliveryBlocked(consumer.getTopic(), consumer.getGroup(), "in-flight");
            log.debug("deliverBatch: Gate 1 BLOCKED (in-flight) for {}", deliveryKeyStr);
            return DeliveryResult.blocked("in-flight");
        }

        // Gate 2: Check pending ACK
        Long pendingOffset = stateService.getPendingOffset(deliveryKey);
        if (pendingOffset != null) {
            metrics.recordConsumerDeliveryBlocked(consumer.getTopic(), consumer.getGroup(), "pending-ack");
            maybeLogModernPendingAckBlocked(consumer, deliveryKey, pendingOffset);
            log.debug("deliverBatch: Gate 2 BLOCKED (pending ACK) for {}, pendingOffset={}",
                     deliveryKeyStr, pendingOffset);
            inFlight.set(false);
            return DeliveryResult.blocked("pending-ack");
        }

        // Gate 3: Check maximum consecutive failures
        if (consumer.getConsecutiveFailures() >= MAX_CONSECUTIVE_FAILURES) {
            metrics.recordConsumerDeliveryBlocked(consumer.getTopic(), consumer.getGroup(), "max-failures");
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
                metrics.recordConsumerDeliveryBlocked(consumer.getTopic(), consumer.getGroup(), "backoff");
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
        boolean timeoutScheduled = false;
        Timer.Sample readSample = null;
        Timer.Sample deliverySample = null;
        DeliveryBatch batch = null;
        String traceId = null;
        long deliveryGeneration = -1L;

        try {
            deliveryGeneration = stateService.beginDelivery(deliveryKey);

            // ================= STORAGE READ METRICS =================
            readSample = metrics.startStorageReadTimer();
            long storageReadStartMs = System.currentTimeMillis();

            // Read batch from storage (using storage executor to prevent deadlock)
            final long capturedOffset = startOffset;
            Future<DeliveryBatch> storageRead = storageExecutor.submit(() ->
                batchStorage.getBatch(consumer.getTopic(), 0, capturedOffset, batchSizeBytes)
            );
            try {
                batch = storageRead.get(storageReadTimeoutSeconds, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                storageRead.cancel(true);
                throw e;
            } catch (InterruptedException e) {
                storageRead.cancel(true);
                Thread.currentThread().interrupt();
                throw e;
            }
            long storageReadDurationMs = System.currentTimeMillis() - storageReadStartMs;

            metrics.stopStorageReadTimer(readSample);
            metrics.recordStorageRead();

            if (storageReadDurationMs >= SLOW_STORAGE_READ_MS) {
                log.warn("event=batch_delivery.storage_read_slow topic={} group={} clientId={} startOffset={} batchSizeBytes={} durationMs={} storageExecutor={}",
                        consumer.getTopic(), consumer.getGroup(), consumer.getClientId(), capturedOffset,
                        batchSizeBytes, storageReadDurationMs, describeExecutor(storageExecutor));
            }

            log.debug("deliverBatch: Batch read complete for {}, recordCount={}",
                     deliveryKeyStr, batch.getRecordCount());

            if (batch.isEmpty()) {
                metrics.recordConsumerDeliveryBlocked(consumer.getTopic(), consumer.getGroup(), "no-data");
                log.debug("deliverBatch: EMPTY BATCH for {}, startOffset={}", deliveryKeyStr, startOffset);
                try { batch.close(); } catch (IOException ignored) {}
                stateService.completeDelivery(deliveryKey, deliveryGeneration);
                return DeliveryResult.blocked("no-data");
            }

            // Apply compaction delivery filter: drop superseded records so consumers never see
            // stale versions. Physical compaction (segment rewrite) will eventually remove them,
            // but until then we filter here on every delivery. Filtering only happens when the
            // index has entries for this topic; otherwise the zero-copy path is used as-is.
            long originalLastOffset = batch.getLastOffset();
            long originalFirstOffset = batch.getFirstOffset(); // capture before filter (used for ACK store)
            batch = applyCompactionFilter(consumer.getTopic(), batch, capturedOffset);
            if (batch.isEmpty()) {
                // All records in the batch were superseded — advance offset without sending.
                // Offsets are persisted per group:topic (DeliveryKey), never per clientId:
                // clientId is the remote socket address and changes on every reconnect.
                try { batch.close(); } catch (IOException ignored) {}
                consumer.setCurrentOffset(originalLastOffset + 1);
                offsetTracker.updateOffset(deliveryKey.toString(), originalLastOffset + 1);
                stateService.completeDelivery(deliveryKey, deliveryGeneration);
                return DeliveryResult.success();
            }

            traceId = TraceIds.newTraceId();
            try (LogMdc.Scope ignored = LogMdc.with(traceId, consumer.getTopic(), consumer.getGroup(), consumer.getClientId())) {

            // ================= BATCH VISIBILITY =================
            metrics.recordBatchSize(batch.getRecordCount());

            // ================= OFFSET RESERVATION =================
            // Advance past ALL records in the original batch (including compaction-filtered ones)
            // so that superseded records are never re-read on the next delivery cycle.
            long originalOffset = startOffset;
            long nextOffset = originalLastOffset + 1;
            consumer.setCurrentOffset(nextOffset);
            stateService.setOriginalOffset(deliveryKey, deliveryGeneration, originalOffset);
            stateService.setPendingOffset(deliveryKey, nextOffset);
            stateService.setFromOffset(deliveryKey, originalFirstOffset);  // original batch first offset covers all records (incl. compaction-filtered) for ACK-store write
            stateService.recordTraceId(deliveryKey, traceId);

            // Record all generation state before sending. An ACK can arrive as soon as the
            // transport publishes the batch, so no state may be initialized after the send.
            long pendingStartTime = System.currentTimeMillis();
            stateService.recordBatchSendTime(deliveryKey, pendingStartTime);

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

            // Start tracking pending ACK age for monitoring
            metrics.startPendingAck(consumer.getTopic(), consumer.getGroup());

            // ================= ACK TIMEOUT SETUP =================
            log.debug("BATCH_SENT to {} at startOffset={}, recordCount={}, bytes={}, ackTimeoutConfigured={}ms, traceId={}",
                     deliveryKeyStr, startOffset, batch.getRecordCount(), batch.getTotalBytes(), ackTimeoutMs, traceId);

            long timeoutGeneration = deliveryGeneration;
            ScheduledFuture<?> timeoutFuture = scheduler.schedule(() -> {
                PendingDelivery claimed =
                        stateService.claimPendingDelivery(deliveryKey, timeoutGeneration);
                if (claimed != null) {
                    try {
                    long pendingDuration = claimed.sendTime() == null
                            ? -1L
                            : System.currentTimeMillis() - claimed.sendTime();
                    log.warn("event=batch_delivery.ack_timeout deliveryKey={} pendingMs={} revertFrom={} revertTo={} traceId={}",
                             deliveryKeyStr, pendingDuration, claimed.pendingOffset(),
                             claimed.originalOffset(), claimed.traceId());

                    // REVERT consumer offset to prevent delivery gap
                    consumer.setCurrentOffset(claimed.originalOffset());

                    metrics.recordAckTimeout(consumer.getTopic(), consumer.getGroup());
                    metrics.completePendingAck(consumer.getTopic(), consumer.getGroup());
                    } finally {
                        stateService.completeDelivery(deliveryKey, timeoutGeneration);
                    }
                }
            }, ackTimeoutMs, TimeUnit.MILLISECONDS);

            timeoutScheduled =
                    stateService.scheduleTimeout(deliveryKey, timeoutGeneration, timeoutFuture);

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
            }

        } catch (Exception e) {
            String deliveryTraceId = traceId != null ? traceId : stateService.getTraceId(deliveryKey);
            try (LogMdc.Scope ignored = LogMdc.with(deliveryTraceId, consumer.getTopic(), consumer.getGroup(), consumer.getClientId())) {
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

                // Record failure for exponential backoff
                consumer.recordFailure();

                boolean isPermanentFailure = consumer.getConsecutiveFailures() >= MAX_CONSECUTIVE_FAILURES;

                if (isPermanentFailure) {
                // Permanent failure: clear this generation and unregister.
                if (deliveryGeneration >= 0) {
                    if (stateService.completeDelivery(deliveryKey, deliveryGeneration)) {
                        consumer.setCurrentOffset(startOffset);
                    }
                } else {
                    inFlight.set(false);
                    consumer.setCurrentOffset(startOffset);
                }
                log.error("Permanent failure for {} (consecutiveFailures={}), removing pending offset and unregistering",
                         deliveryKeyStr, MAX_CONSECUTIVE_FAILURES);
                registrationService.unregisterConsumer(consumer.getClientId());
                } else if (!timeoutScheduled) {
                // sendBatchToConsumer() threw before the ACK timeout was scheduled.
                // No ACK is coming and nothing will ever clear pendingOffset — clear it now
                // so Gate 2 does not permanently block all future delivery attempts.
                if (deliveryGeneration >= 0) {
                    if (stateService.completeDelivery(deliveryKey, deliveryGeneration)) {
                        consumer.setCurrentOffset(startOffset);
                    }
                } else {
                    inFlight.set(false);
                    consumer.setCurrentOffset(startOffset);
                }
                log.warn("event=batch_delivery.transient_failure deliveryKey={} pendingOffsetCleared=true ackTimeoutMs={} consecutiveFailures={}",
                        deliveryKeyStr, ackTimeoutMs, consumer.getConsecutiveFailures());
                } else {
                // Exception thrown after the ACK timeout was already scheduled (rare: post-send
                // metrics/logging path). The timeout will fire and clear pendingOffset on its own.
                log.warn("event=batch_delivery.transient_failure deliveryKey={} pendingOffsetRetained=true timeoutScheduled=true ackTimeoutMs={} consecutiveFailures={}",
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
        CompletableFuture<Void> sendFuture =
                server.sendBatch(consumer.getClientId(), consumer.getGroup(), batch);
        try {
            sendFuture.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            sendFuture.cancel(true);
            throw e;
        } catch (InterruptedException e) {
            sendFuture.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        }

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

    /**
     * Apply compaction delivery filter to {@code batch}.
     *
     * <p>If the batch starts beyond the topic's highest known stale offset, the original batch is
     * returned unchanged and the zero-copy path is preserved. Otherwise the batch is decoded,
     * superseded records are dropped, and a heap-backed {@link ByteArrayDeliveryBatch} carrying only
     * the deliverable records is returned. The original batch is closed before this method returns.
     *
     * <p>If all records in the batch are superseded an empty (isEmpty()) batch is returned.
     */
    private DeliveryBatch applyCompactionFilter(String topic, DeliveryBatch batch, long fromOffset) {
        // Capture offsets before any close() call; ByteArrayDeliveryBatch constructors need them
        // and the original batch must not be touched after close().
        long batchFirstOffset = batch.getFirstOffset();
        long batchLastOffset  = batch.getLastOffset();
        int  batchRecordCount = batch.getRecordCount();

        // Fast-path: only filter batches whose offsets can still contain superseded records.
        if (!compactionIndex.shouldFilterDelivery(topic, batchFirstOffset)) {
            return batch;
        }

        try {
            List<MessageRecord> decoded = storage.read(topic, 0, fromOffset, batchRecordCount);

            // Partial-decode fail-open: if the storage read returned fewer records than the batch
            // contains (e.g. 1MB size cap truncated the result), we cannot safely determine which
            // records are superseded — return the original batch unchanged.
            if (decoded.size() < batchRecordCount) {
                return batch;
            }

            List<MessageRecord> deliverable = decoded.stream()
                    .filter(r -> !compactionIndex.isSuperseded(topic, r.getMsgKey(), r.getOffset()))
                    .collect(Collectors.toList());

            if (deliverable.size() == decoded.size()) {
                return batch; // nothing filtered — keep zero-copy batch
            }

            // Build the replacement BEFORE closing the original so that encoding errors
            // never leave us with a closed-but-returned batch (which causes ClosedChannelException
            // in Netty's zero-copy path).
            DeliveryBatch replacement;
            if (deliverable.isEmpty()) {
                replacement = new ByteArrayDeliveryBatch(topic, new byte[0], 0, batchFirstOffset, batchLastOffset);
            } else {
                byte[] encoded = encodeToBinaryFormat(deliverable);
                long firstOff = deliverable.get(0).getOffset();
                long lastOff  = deliverable.get(deliverable.size() - 1).getOffset();
                replacement = new ByteArrayDeliveryBatch(topic, encoded, deliverable.size(), firstOff, lastOff);
            }

            // Replacement is ready — safe to close the original file-backed batch.
            try { batch.close(); } catch (IOException ignored) {}
            return replacement;

        } catch (Exception e) {
            log.warn("Compaction filter failed for topic={}, delivering unfiltered batch: {}", topic, e.getMessage());
            // batch is never closed in this path — caller can still use it safely.
            return batch;
        }
    }

    /**
     * Encode a list of records into the unified segment log binary format:
     * {@code [keyLen:4][key:var][eventType:1][dataLen:4][data:var][timestamp:8]}
     */
    private static byte[] encodeToBinaryFormat(List<MessageRecord> records) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (MessageRecord r : records) {
            byte[] keyBytes  = r.getMsgKey() != null ? r.getMsgKey().getBytes(StandardCharsets.UTF_8) : new byte[0];
            byte[] dataBytes = r.getData() != null ? r.getData().getBytes(StandardCharsets.UTF_8) : new byte[0];
            ByteBuffer buf = ByteBuffer.allocate(4 + keyBytes.length + 1 + 4 + dataBytes.length + 8);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            buf.put((byte) r.getEventType().getCode());
            buf.putInt(dataBytes.length);
            if (dataBytes.length > 0) buf.put(dataBytes);
            buf.putLong(r.getCreatedAt().toEpochMilli());
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            baos.write(bytes);
        }
        return baos.toByteArray();
    }

    private void maybeLogModernPendingAckBlocked(RemoteConsumer consumer, DeliveryKey deliveryKey, long pendingOffset) {
        Long sendTime = stateService.getBatchSendTime(deliveryKey);
        long pendingAgeMs = sendTime != null && sendTime > 0
                ? Math.max(0, System.currentTimeMillis() - sendTime)
                : -1L;
        if (pendingAgeMs < MODERN_PENDING_ACK_WARN_THRESHOLD_MS || !shouldWarnBlocked(consumer.getGroup(), consumer.getTopic())) {
            return;
        }

        log.warn("event=batch_delivery.blocked topic={} group={} clientId={} reason=pending_ack pendingAckAgeMs={} pendingOffset={} consumerOffset={}",
                consumer.getTopic(), consumer.getGroup(), consumer.getClientId(),
                pendingAgeMs, pendingOffset, consumer.getCurrentOffset());
    }

    private String describeExecutor(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor pool) {
            return String.format("poolSize=%d active=%d queued=%d completed=%d",
                    pool.getPoolSize(),
                    pool.getActiveCount(),
                    pool.getQueue().size(),
                    pool.getCompletedTaskCount());
        }
        return executor.getClass().getSimpleName();
    }

    private boolean shouldWarnBlocked(String group, String topic) {
        String key = (group == null || group.isBlank() ? "unknown" : group) + ":" +
                (topic == null || topic.isBlank() ? "unknown" : topic);
        long now = System.currentTimeMillis();
        AtomicLong lastLogged = blockedWarnTime.computeIfAbsent(key, ignored -> new AtomicLong(0));
        long previous = lastLogged.get();
        return now - previous >= MODERN_BLOCKED_WARN_INTERVAL_MS && lastLogged.compareAndSet(previous, now);
    }
}
