package com.messaging.broker.consumer;

import com.messaging.broker.ack.AckRecord;
import com.messaging.broker.ack.RocksDbAckStore;
import com.messaging.broker.monitoring.ConsumerEventLogger;
import com.messaging.broker.monitoring.LogContext;
import com.messaging.broker.consumer.ConsumerAckService;
import com.messaging.broker.consumer.ConsumerRegistrationService;
import com.messaging.broker.consumer.ConsumerStateService;
import com.messaging.broker.legacy.LegacyConsumerDeliveryManager;
import com.messaging.broker.legacy.MergedBatch;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.broker.monitoring.LogMdc;
import com.messaging.broker.model.ConsumerKey;
import com.messaging.broker.model.DeliveryKey;
import com.messaging.broker.consumer.PendingAckStore;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import io.micronaut.context.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles batch acknowledgment workflow for modern and legacy consumers.
 */
@Singleton
public class BatchAckService implements ConsumerAckService {

    private static final Logger log = LoggerFactory.getLogger(BatchAckService.class);
    private static final long MODERN_ACK_SLOW_MS = 200L;
    private static final long MODERN_ACK_REPLAY_SLOW_MS = 500L;
    private static final long LEGACY_ACK_SLOW_MS = 500L;

    private final ConsumerStateService stateService;
    private final PendingAckStore pendingAckStore;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final StorageEngine storage;
    private final ConsumerRegistrationService registrationService;
    private final LegacyConsumerDeliveryManager legacyDeliveryManager;
    private final ConsumerEventLogger consumerLogger;
    private final RocksDbAckStore ackStore;
    private final ExecutorService ackStorageExecutor;
    private final boolean liveReplayEnabled;

    @Inject
    public BatchAckService(
            ConsumerStateService stateService,
            PendingAckStore pendingAckStore,
            ConsumerOffsetTracker offsetTracker,
            BrokerMetrics metrics,
            StorageEngine storage,
            ConsumerRegistrationService registrationService,
            LegacyConsumerDeliveryManager legacyDeliveryManager,
            ConsumerEventLogger consumerLogger,
            RocksDbAckStore ackStore,
            @Named("ackStorageExecutor") ExecutorService ackStorageExecutor,
            @Value("${ack-store.live-replay.enabled:true}") boolean liveReplayEnabled) {
        this.stateService = stateService;
        this.pendingAckStore = pendingAckStore;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.storage = storage;
        this.registrationService = registrationService;
        this.legacyDeliveryManager = legacyDeliveryManager;
        this.consumerLogger = consumerLogger;
        this.ackStore = ackStore;
        this.ackStorageExecutor = ackStorageExecutor;
        this.liveReplayEnabled = liveReplayEnabled;
    }

    @Override
    public void handleModernBatchAck(String clientId, String topic, String group) {
        long handlerStartMs = System.currentTimeMillis();
        DeliveryKey deliveryKey = DeliveryKey.of(group, topic);
        String deliveryKeyStr = clientId + " -> " + deliveryKey;
        String traceId = stateService.getTraceId(deliveryKey);
        try (LogMdc.Scope ignored = LogMdc.with(traceId, topic, group, clientId)) {

        // Calculate ACK latency
        long ackReceiveTime = System.currentTimeMillis();
        Long sendTime = stateService.getBatchSendTime(deliveryKey);
        long ackLatencyMs = sendTime != null && sendTime > 0 ? (ackReceiveTime - sendTime) : -1;

        // Atomically claim ownership of the pending offset.
        // removePendingOffset() is a single ConcurrentHashMap.remove() call — whichever thread
        // (this ACK handler or the scheduled timeout) calls it first gets the non-null value.
        // The other gets null and short-circuits, preventing the double-revert race.
        Long committedOffset = stateService.removePendingOffset(deliveryKey);

        // Cancel timeout
        stateService.cancelTimeout(deliveryKey);

        if (committedOffset == null) {
            log.warn("event=batch_ack.late_ack deliveryKey={} ackLatencyMs={} action=clear_inflight traceId={}",
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
            log.warn("event=batch_ack.unregistered_consumer deliveryKey={} committedOffset={} group={} action=persist_offset",
                     deliveryKeyStr, committedOffset, group);
            offsetTracker.updateOffset(group + ":" + topic, committedOffset);
        }

        // Capture fromOffset BEFORE clearing in-flight to prevent race: once clearInFlight() fires,
        // the delivery scheduler can immediately start the next batch and overwrite fromOffset via
        // setFromOffset(). Capturing first ensures we read this batch's window, not the next one's.
        Long capturedFromOffset = stateService.getFromOffset(deliveryKey);
        stateService.clearFromOffset(deliveryKey);  // clear before async task to prevent leak

        // Clear in-flight status (opens the delivery race window — fromOffset already captured above)
        stateService.recordBatchSendTime(deliveryKey, 0);
        stateService.clearInFlight(deliveryKey);
        stateService.clearTraceId(deliveryKey);

        // Async RocksDB write: re-read batch from storage to extract msgKeys.
        // Uses a loop in chunks of 500 because storage.read() has a 1MB internal size limit —
        // a single read call may return fewer records than the batch contains when records are large.
        if (liveReplayEnabled && capturedFromOffset != null) {
            final long fromOffsetSnap = capturedFromOffset;
            final long toOffsetSnap = committedOffset;
            final String topicSnap = topic;
            final String groupSnap = group;
            final long ackReceiveTimeSnap = ackReceiveTime;
            // Use a dedicated ACK storage pool so replay/persistence work cannot occupy
            // the delivery read pool and stall all TopicFairScheduler threads.
            ackStorageExecutor.execute(() -> {
                if (toOffsetSnap <= fromOffsetSnap) return;
                long replayStartMs = System.currentTimeMillis();
                int writtenAckRecords = 0;
                int chunksRead = 0;
                try {
                    // Flush per storage chunk rather than accumulating all records first.
                    // A post-refresh replay ACK can span 1M+ offsets — building a single
                    // ArrayList of all AckRecords before writing filled the heap (OOM).
                    String[] topicArr = new String[]{topicSnap};
                    String[] groupArr = new String[]{groupSnap};

                    long currentOffset = fromOffsetSnap;
                    while (currentOffset < toOffsetSnap) {
                        int chunkSize = (int) Math.min(toOffsetSnap - currentOffset, 500);
                        List<MessageRecord> chunk = storage.read(topicSnap, 0, currentOffset, chunkSize);
                        if (chunk.isEmpty()) break;
                        chunksRead++;

                        List<AckRecord> ackChunk = new ArrayList<>(chunk.size());
                        for (MessageRecord r : chunk) {
                            if (r.getOffset() >= toOffsetSnap) break;
                            ackChunk.add(new AckRecord(r.getOffset(), ackReceiveTimeSnap));
                        }

                        if (!ackChunk.isEmpty()) {
                            String[] topics = new String[ackChunk.size()];
                            String[] groups = new String[ackChunk.size()];
                            java.util.Arrays.fill(topics, topicSnap);
                            java.util.Arrays.fill(groups, groupSnap);
                            ackStore.putBatch(topics, groups, ackChunk.toArray(new AckRecord[0]));
                            writtenAckRecords += ackChunk.size();
                        }

                        currentOffset = chunk.get(chunk.size() - 1).getOffset() + 1;
                    }
                    long replayDurationMs = System.currentTimeMillis() - replayStartMs;
                    if (replayDurationMs >= MODERN_ACK_REPLAY_SLOW_MS) {
                        log.warn("event=batch_ack.replay_slow topic={} group={} fromOffset={} toOffset={} writtenAckRecords={} chunksRead={} durationMs={} ackStorageExecutor={}",
                                topicSnap, groupSnap, fromOffsetSnap, toOffsetSnap,
                                writtenAckRecords, chunksRead, replayDurationMs,
                                describeExecutor(ackStorageExecutor));
                    }
                } catch (Exception ex) {
                    log.warn("event=batch_ack.rocksdb_write_failed topic={} group={} fromOffset={}",
                            topicSnap, groupSnap, fromOffsetSnap, ex);
                }
            });
        } else if (!liveReplayEnabled && capturedFromOffset != null) {
            log.debug("event=batch_ack.replay_skipped topic={} group={} fromOffset={} toOffset={} reason=disabled",
                    topic, group, capturedFromOffset, committedOffset);
        }

        long handlerDurationMs = System.currentTimeMillis() - handlerStartMs;
        if (handlerDurationMs >= MODERN_ACK_SLOW_MS) {
            log.warn("event=batch_ack.slow deliveryKey={} topic={} group={} committedOffset={} ackLatencyMs={} handlerDurationMs={} ackStorageExecutor={}",
                    deliveryKeyStr, topic, group, committedOffset, ackLatencyMs, handlerDurationMs,
                    describeExecutor(ackStorageExecutor));
        }
        log.debug("ACK committed for {} at offset {}", deliveryKeyStr, committedOffset);
        }
    }

    @Override
    public void handleLegacyBatchAck(String clientId, String group) {
        long handlerStartMs = System.currentTimeMillis();
        long ackReceiveTime = System.currentTimeMillis();
        long sendTime = pendingAckStore.getSendTime(clientId);
        Timer.Sample deliverySample = pendingAckStore.removeTimer(clientId);
        MergedBatch batch = pendingAckStore.removePendingBatch(clientId);
        pendingAckStore.removeClient(clientId); // clean up send time and any remaining state

        if (batch == null) {
            metrics.recordLegacyDeliveryBlocked(group, "late_ack_or_timeout");
            log.warn("event=legacy_batch_ack.no_pending_data clientId={} group={} cause=late_ack_or_timeout",
                    clientId, group);
            return;
        }

        // Calculate ACK latency from stored send timestamp
        long ackLatencyMs = sendTime > 0 ? (ackReceiveTime - sendTime) : -1;
        metrics.recordLegacyBatchAck(group);

        if (ackLatencyMs >= 5_000) {
            log.warn("event=legacy_batch.ack_received_slow clientId={} group={} ackLatencyMs={} messageCount={} topicOffsets={}",
                    clientId, group, ackLatencyMs, batch.getMessageCount(), batch.getMaxOffsetPerTopic());
        } else {
            log.debug("Legacy batch ACK_RECEIVED for clientId={}, group={} at T={}ms",
                    clientId, group, ackLatencyMs);
        }

        try {
            // Use the handleMergedBatchAck method from LegacyConsumerDeliveryManager
            legacyDeliveryManager.handleMergedBatchAck(group, batch);

            log.debug("Legacy batch ACK committed for clientId={}, group={}, topics={}",
                    clientId, group, batch.getMaxOffsetPerTopic());

            // Write per-offset ACK records to RocksDB (no async needed — already on ackExecutor)
            List<MessageRecord> messages = batch.getMessages();
            if (!messages.isEmpty()) {
                long rocksStartMs = System.currentTimeMillis();
                List<String> topicList = new ArrayList<>(messages.size());
                List<String> groupList = new ArrayList<>(messages.size());
                List<AckRecord> ackList = new ArrayList<>(messages.size());
                for (MessageRecord r : messages) {
                    topicList.add(r.getTopic());
                    groupList.add(group);
                    ackList.add(new AckRecord(r.getOffset(), ackReceiveTime));
                }
                ackStore.putBatch(
                        topicList.toArray(new String[0]),
                        groupList.toArray(new String[0]),
                        ackList.toArray(new AckRecord[0]));
                long rocksDurationMs = System.currentTimeMillis() - rocksStartMs;
                if (rocksDurationMs >= LEGACY_ACK_SLOW_MS) {
                    log.warn("event=legacy_batch_ack.rocksdb_write_slow clientId={} group={} messageCount={} topicCount={} durationMs={}",
                            clientId, group, messages.size(), batch.getMaxOffsetPerTopic().size(), rocksDurationMs);
                }
            }

            // Update metrics for each topic using actual per-topic counts
            Map<String, Long> bytesPerTopic = batch.getBytesPerTopic();
            Map<String, Integer> msgCountPerTopic = batch.getMessageCountPerTopic();

            for (Map.Entry<String, Long> entry : batch.getMaxOffsetPerTopic().entrySet()) {
                String topic = entry.getKey();
                long offset = offsetTracker.getOffset(group + ":" + topic);
                long topicBytes = bytesPerTopic.getOrDefault(topic, 0L);
                int topicMessages = msgCountPerTopic.getOrDefault(topic, 0);

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
        } finally {
            long handlerDurationMs = System.currentTimeMillis() - handlerStartMs;
            if (handlerDurationMs >= LEGACY_ACK_SLOW_MS) {
                log.warn("event=legacy_batch_ack.slow clientId={} group={} handlerDurationMs={} ackLatencyMs={}",
                        clientId, group, handlerDurationMs, sendTime > 0 ? (ackReceiveTime - sendTime) : -1);
            }
        }
    }

    @Override
    public boolean isAckPending(String topic, String group) {
        DeliveryKey deliveryKey = DeliveryKey.of(group, topic);
        return stateService.getPendingOffset(deliveryKey) != null;
    }

    @Override
    public void clearPendingAcks(String clientId) {
        MergedBatch pendingBatch = pendingAckStore.getPendingBatch(clientId);
        String legacyGroup = registrationService.getConsumersByClient(clientId).stream()
                .filter(RemoteConsumer::isLegacy)
                .map(RemoteConsumer::getGroup)
                .findFirst()
                .orElse(null);

        // Clear legacy consumer ACKs
        pendingAckStore.removeClient(clientId);
        if (legacyGroup != null) {
            metrics.clearLegacyPendingBatch(legacyGroup);
        }

        if (pendingBatch != null) {
            log.info("event=legacy_batch.pending_cleared_on_disconnect clientId={} group={} messageCount={} topicOffsets={}",
                    clientId, legacyGroup, pendingBatch.getMessageCount(), pendingBatch.getMaxOffsetPerTopic());
        }

        log.debug("Cleared legacy pending ACKs for client: {}", clientId);
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
}
