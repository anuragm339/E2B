package com.messaging.broker.refresh;

import com.messaging.broker.consumer.RemoteConsumerRegistry;
import com.messaging.broker.metrics.DataRefreshMetrics;
import com.messaging.common.api.PipeConnector;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Central orchestrator for DataRefresh workflow with immediate replay logic
 *
 * Key Features:
 * - Immediate replay per consumer (don't wait for all)
 * - State persistence for restart recovery
 * - Health check integration
 * - Pipe pause/resume control
 */
@Singleton
public class DataRefreshManager {
    private static final Logger log = LoggerFactory.getLogger(DataRefreshManager.class);
    private static final long READY_ACK_TIMEOUT_MS = 10000;
    private static final long REPLAY_CHECK_INTERVAL_MS = 1000;
    private static final long RESET_RETRY_INTERVAL_MS = 5000;  // Retry RESET every 5 seconds

    private final RemoteConsumerRegistry remoteConsumers;
    private final PipeConnector pipeConnector;
    private final DataRefreshConfiguration config;
    private final DataRefreshStateStore stateStore;
    private final DataRefreshMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final Map<String, DataRefreshContext> activeRefreshes;
    private final Map<String, ScheduledFuture<?>> replayCheckTasks;
    private final Map<String, ScheduledFuture<?>> resetRetryTasks;

    // Track current refresh batch ID - shared by all topics in the same batch
    private volatile String currentRefreshId = null;

    public DataRefreshManager(
            RemoteConsumerRegistry remoteConsumers,
            PipeConnector pipeConnector,
            DataRefreshConfiguration config,
            DataRefreshStateStore stateStore,
            DataRefreshMetrics metrics) {
        this.remoteConsumers = remoteConsumers;
        this.pipeConnector = pipeConnector;
        this.config = config;
        this.stateStore = stateStore;
        this.metrics = metrics;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setName("DataRefreshManager");
            return t;
        });
        this.activeRefreshes = new ConcurrentHashMap<>();
        this.replayCheckTasks = new ConcurrentHashMap<>();
        this.resetRetryTasks = new ConcurrentHashMap<>();
    }

    /**
     * Resume any in-progress refreshes from properties file on startup
     */
    @PostConstruct
    public void init() {
        // Wire this manager to RemoteConsumerRegistry for metrics tracking
        remoteConsumers.setDataRefreshManager(this);
        log.info("DataRefreshManager wired to RemoteConsumerRegistry for metrics tracking");

        Map<String, DataRefreshContext> savedRefreshes = stateStore.loadAllRefreshes();

        if (savedRefreshes.isEmpty()) {
            log.info("No saved refresh state found, starting fresh");
            return;
        }

        log.info("Resuming {} refresh(es) from saved state", savedRefreshes.size());

        // IMPORTANT: Pause pipes BEFORE resuming any refresh
        // This ensures no new messages flow during resume process
        pipeConnector.pausePipeCalls();
        log.info("Pipe calls PAUSED before resuming refreshes");

        // Record startup time for all resumed refreshes
        Instant startupTime = Instant.now();

        // Group topics by refresh_id to handle batches properly
        Map<String, List<DataRefreshContext>> batchGroups = new HashMap<>();
        for (Map.Entry<String, DataRefreshContext> entry : savedRefreshes.entrySet()) {
            String topic = entry.getKey();
            DataRefreshContext context = entry.getValue();

            String refreshId = context.getRefreshId();
            if (refreshId == null) {
                // Backward compatibility: generate refresh_id if missing
                refreshId = String.valueOf(System.currentTimeMillis());
                context.setRefreshId(refreshId);
                log.warn("No refresh_id found for topic {}, generating new one: {}", topic, refreshId);
            }

            batchGroups.computeIfAbsent(refreshId, k -> new ArrayList<>()).add(context);
        }

        log.info("Found {} batch(es) to resume", batchGroups.size());

        // Process batches - set currentRefreshId to the most recent batch
        // (batches are timestamp-based, so latest = highest value)
        String latestRefreshId = batchGroups.keySet().stream()
                .max(String::compareTo)
                .orElse(null);
        currentRefreshId = latestRefreshId;
        log.info("Set currentRefreshId to most recent batch: {}", currentRefreshId);

        // Resume all topics, grouped by batch
        for (Map.Entry<String, List<DataRefreshContext>> batchEntry : batchGroups.entrySet()) {
            String batchId = batchEntry.getKey();
            List<DataRefreshContext> topicsInBatch = batchEntry.getValue();

            log.info("Resuming batch {} with {} topic(s): {}",
                    batchId,
                    topicsInBatch.size(),
                    topicsInBatch.stream().map(DataRefreshContext::getTopic).collect(Collectors.toList()));

            for (DataRefreshContext context : topicsInBatch) {
                String topic = context.getTopic();
                log.info("Resuming topic: {} (batch: {}, state: {})", topic, batchId, context.getState());

                // Record startup to close any open downtime period
                context.recordStartup(startupTime);
                long totalDowntime = context.getTotalDowntimeSeconds();
                log.info("Recorded startup for topic: {} at {} (total downtime so far: {}s)",
                         topic, startupTime, totalDowntime);

                activeRefreshes.put(topic, context);
                stateStore.saveState(context);  // Persist the updated context with startup time
                resumeRefresh(context);
            }
        }

        log.info("Successfully resumed {} active refresh(es) across {} batch(es)",
                savedRefreshes.size(), batchGroups.size());
    }

    /**
     * Start refresh workflow: pause pipes, broadcast RESET, persist state
     * Uses topic name as the expected consumer identifier (one consumer per topic)
     */
    public CompletableFuture<RefreshResult> startRefresh(String topic) {

        // For per-topic refresh, we expect ONE consumer per topic
        // The expected consumer identifier is the topic name itself
        // This matches how consumers are registered in application.yml
        Set<String> expectedConsumers = Set.of(topic);

        log.info("Starting refresh for topic: {} with {} expected consumer(s)",
                topic, expectedConsumers.size());

        // Thread-safe initialization of currentRefreshId and metrics reset
        // Reset all metrics ONLY if this is the first refresh (no active refreshes)
        // This ensures dashboard shows only current refresh batch data
        // Prometheus historical data is preserved for time-series queries
        synchronized (this) {
            if (activeRefreshes.isEmpty()) {
                // Generate new refresh_id for this batch (timestamp-based)
                currentRefreshId = String.valueOf(System.currentTimeMillis());
                metrics.resetMetricsForNewRefresh();
                log.info("Starting new refresh batch with refresh_id: {}", currentRefreshId);
            } else {
                log.info("Adding topic to existing refresh batch: {}", currentRefreshId);
            }

            // Ensure currentRefreshId is never null (defensive programming)
            if (currentRefreshId == null) {
                currentRefreshId = String.valueOf(System.currentTimeMillis());
                log.warn("currentRefreshId was null, initializing to: {}", currentRefreshId);
            }
        }

        DataRefreshContext context = new DataRefreshContext(
                topic,
                expectedConsumers
        );
        activeRefreshes.put(topic, context);

        context.setState(DataRefreshState.RESET_SENT);
        context.setResetSentTime(Instant.now());
        context.setRefreshId(currentRefreshId);  // Store refresh_id for metrics tracking

        // Record metrics: refresh started (with refresh_id)
        metrics.recordRefreshStarted(topic, "LOCAL", currentRefreshId);

        // Pause pipe calls before starting refresh
        pipeConnector.pausePipeCalls();
        log.info("Pipe calls PAUSED for refresh of topic: {}", topic);

        // Broadcast RESET to all consumers
        remoteConsumers.broadcastResetToTopic(topic);
        log.info("RESET sent to all consumers for topic: {}", topic);

        // Record metrics: RESET sent to each expected consumer
        for (String consumer : expectedConsumers) {
            metrics.recordResetSent(topic, consumer, currentRefreshId);
        }

        // Persist state immediately
        stateStore.saveState(context);

        // Schedule periodic RESET retry every 5 seconds until all ACKs received
        ScheduledFuture<?> resetTask = scheduler.scheduleWithFixedDelay(
                () -> retryResetBroadcast(topic),
                RESET_RETRY_INTERVAL_MS,
                RESET_RETRY_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        resetRetryTasks.put(topic, resetTask);
        log.info("Scheduled RESET retry task for topic {} (interval: {}ms)", topic, RESET_RETRY_INTERVAL_MS);

        return CompletableFuture.completedFuture(
            RefreshResult.success(topic, context.getState(), expectedConsumers.size())
        );
    }

    /**
     * Handle RESET ACK from consumer - START REPLAY IMMEDIATELY for this consumer
     * Don't wait for all consumers to ACK
     *
     * @param consumerGroupTopic Stable consumer identifier ("group:topic" format) for tracking
     * @param clientId Dynamic client socket ID for triggering replay
     * @param topic Topic being refreshed
     */
    public void handleResetAck(String consumerGroupTopic, String clientId, String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        if (context == null) {
            log.warn("Received RESET ACK from {} (client: {}) for topic {} but no active refresh",
                    consumerGroupTopic, clientId, topic);
            return;
        }

        if (!context.getExpectedConsumers().contains(consumerGroupTopic)) {
            log.warn("Received RESET ACK from unexpected consumer: {} (expected: {})",
                    consumerGroupTopic, context.getExpectedConsumers());
            return;
        }

        if (context.getReceivedResetAcks().contains(consumerGroupTopic)) {
            log.debug("Duplicate RESET ACK from {} for topic {}, ignoring", consumerGroupTopic, topic);
            return;
        }

        context.recordResetAck(consumerGroupTopic);
        log.info("RESET ACK received from {} (client: {}) for topic {} ({}/{})",
                consumerGroupTopic, clientId, topic,
                context.getReceivedResetAcks().size(),
                context.getExpectedConsumers().size());

        // Record metrics: RESET ACK received
        metrics.recordResetAckReceived(topic, consumerGroupTopic, context.getRefreshId());

        // CRITICAL FIX: Reset offset to 0 for THIS consumer (do this for EVERY RESET_ACK)
        // Previously this was only done for the first RESET_ACK, causing incomplete replays
        remoteConsumers.resetConsumerOffset(clientId, topic, 0);
        log.info("Reset offset to 0 for consumer: {} (group:topic={}) on topic: {}",
                 clientId, consumerGroupTopic, topic);

        // Persist state after each ACK
        stateStore.saveState(context);

        // Transition to REPLAYING state on first ACK
        if (context.getState() == DataRefreshState.RESET_SENT) {
            context.setState(DataRefreshState.REPLAYING);
            stateStore.saveState(context);

            // Cancel RESET retry task (no longer need to retry)
            ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
            if (resetTask != null) {
                resetTask.cancel(false);
                log.info("Cancelled RESET retry task for topic {} - transitioning to REPLAYING", topic);
            }

            // Start periodic replay progress check
            ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                    () -> checkReplayProgress(topic),
                    REPLAY_CHECK_INTERVAL_MS,
                    REPLAY_CHECK_INTERVAL_MS,
                    TimeUnit.MILLISECONDS
            );
            replayCheckTasks.put(topic, task);
        }

    }

    /**
     * Start immediate replay for individual consumer
     */
    private void startReplayForConsumer(String clientId, String topic) {
        log.info("Starting IMMEDIATE replay for consumer: {} on topic: {}", clientId, topic);

        try {
            DataRefreshContext context = activeRefreshes.get(topic);
            if (context != null) {
                // Record metrics: replay started
                metrics.recordReplayStarted(topic, topic, context.getRefreshId());  // Use topic as consumer ID
            }

            // Note: Adaptive delivery manager will automatically discover and deliver messages
            // No explicit trigger needed - watermark-based polling handles replay

            log.info("Replay ready for consumer: {} starting from offset 0 (adaptive delivery will poll)", clientId);

        } catch (Exception e) {
            DataRefreshException ex = new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                "Failed to start replay for consumer", e);
            ex.withContext("clientId", clientId);
            ex.withContext("topic", topic);
            ExceptionLogger.logError(log, ex);
            // Don't rethrow - replay will be retried automatically
        }
    }

    /**
     * Retry RESET broadcast until all expected consumers ACK
     * Called periodically by scheduler when in RESET_SENT state
     */
    private void retryResetBroadcast(String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        if (context == null || context.getState() != DataRefreshState.RESET_SENT) {
            // State changed, stop retrying
            ScheduledFuture<?> task = resetRetryTasks.remove(topic);
            if (task != null) {
                task.cancel(false);
            }
            return;
        }

        Set<String> missingAcks = getMissingResetAcks(context);
        if (missingAcks.isEmpty()) {
            // All ACKs received, stop retrying (will transition to REPLAYING in handleResetAck)
            log.info("All RESET ACKs received for topic {}, stopping retry scheduler", topic);
            ScheduledFuture<?> task = resetRetryTasks.remove(topic);
            if (task != null) {
                task.cancel(false);
            }
            return;
        }

        // Re-broadcast RESET to all consumers (safe to send multiple times)
        log.info("Retrying RESET broadcast for topic {} - still waiting for {} consumer(s): {}",
                topic, missingAcks.size(), missingAcks);
        remoteConsumers.broadcastResetToTopic(topic);
    }

    /**
     * Check if ALL consumers have caught up to latest offset
     * If yes, send READY to all consumers
     */
    private void checkReplayProgress(String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        if (context == null || context.getState() != DataRefreshState.REPLAYING) {
            return;
        }

        // Only check consumers that have ACKed RESET
        Set<String> ackedConsumers = context.getReceivedResetAcks();
        if (ackedConsumers.isEmpty()) {
            return;
        }

        boolean allCaughtUp = remoteConsumers.allConsumersCaughtUp(topic, ackedConsumers);

        if (allCaughtUp) {
            log.info("All consumers caught up for topic {}, sending READY", topic);
            sendReady(topic, context);

            // Cancel replay check task
            ScheduledFuture<?> task = replayCheckTasks.remove(topic);
            if (task != null) {
                task.cancel(false);
            }
        } else {
            // Trigger replay for each consumer (InFlight check in notifyNewMessageForConsumer prevents duplicates)
            // This polling is necessary to keep driving deliveries during refresh
            List<String> allConsumerIds = remoteConsumers.getAllConsumerIds(topic);
            if (allConsumerIds == null || allConsumerIds.isEmpty()) {
                log.debug("No remote consumers found for topic {}, cannot trigger replay", topic);
            } else {
                log.debug("Checking replay progress for {} consumers on topic {}", allConsumerIds.size(), topic);
                for (String clientId : allConsumerIds) {
                    // The InFlight check in notifyNewMessageForConsumer will prevent duplicate scheduling
                    startReplayForConsumer(clientId, topic);
                }
            }
        }
    }

    /**
     * Send READY message to all consumers
     */
    private void sendReady(String topic, DataRefreshContext context) {
        log.info("All consumers caught up for topic {}, sending READY...", topic);

        context.setState(DataRefreshState.READY_SENT);
        context.setReadySentTime(Instant.now());

        // Record READY sent metrics for each expected consumer
        for (String consumer : context.getExpectedConsumers()) {
            metrics.recordReadySent(topic, consumer, context.getRefreshId());
        }

        remoteConsumers.broadcastReadyToTopic(topic);
        log.info("READY sent to all consumers for topic: {}", topic);

        // Persist state
        stateStore.saveState(context);

        // Schedule timeout check
        scheduler.schedule(
                () -> checkReadyAckTimeout(topic),
                READY_ACK_TIMEOUT_MS,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Handle READY ACK from consumer - complete when ALL expected consumers ACKed
     *
     * @param consumerGroupTopic Stable consumer identifier ("group:topic" format) for tracking
     * @param topic Topic being refreshed
     */
    public void handleReadyAck(String consumerGroupTopic, String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        if (context == null || context.getState() != DataRefreshState.READY_SENT) {
            log.warn("Received unexpected READY ACK from {} for topic {} (state: {})",
                    consumerGroupTopic, topic, context != null ? context.getState() : "NO_CONTEXT");
            return;
        }

        if (!context.getExpectedConsumers().contains(consumerGroupTopic)) {
            log.warn("Received READY ACK from unexpected consumer: {} (expected: {})",
                    consumerGroupTopic, context.getExpectedConsumers());
            return;
        }

//        if (context.getReceivedReadyAcks().contains(consumerGroupTopic)) {
//            log.debug("Duplicate READY ACK from {} for topic {}, ignoring", consumerGroupTopic, topic);
//            return;
//        }

        context.recordReadyAck(consumerGroupTopic);
        log.info("READY ACK received from {} for topic {} ({}/{})",
                consumerGroupTopic, topic,
                context.getReceivedReadyAcks().size(),
                context.getExpectedConsumers().size());

        // Record metrics: READY ACK received
        metrics.recordReadyAckReceived(topic, consumerGroupTopic, context.getRefreshId());

        // Persist state after each ACK
        stateStore.saveState(context);

        // Complete only when ALL expected consumers ACKed
        if (context.allReadyAcksReceived()) {
            completeRefresh(topic, context);
        } else {
            Set<String> missing = getMissingReadyAcks(context);
            log.info("Waiting for READY ACKs from: {}", missing);
        }
    }

    /**
     * Complete refresh: resume pipes, clear state, cleanup
     */
    private void completeRefresh(String topic, DataRefreshContext context) {
        log.info("All READY ACKs received for topic {}, refresh COMPLETE", topic);

        context.setState(DataRefreshState.COMPLETED);
        stateStore.saveState(context);

        // Record metrics: refresh completed successfully (with refresh_id AND context for downtime)
        metrics.recordRefreshCompleted(topic, "LOCAL", "SUCCESS", currentRefreshId, context);

        // Resume pipe calls only if NO other refreshes IN THE SAME BATCH are in progress
        // Check only topics with the same refresh_id (same batch)
        String batchId = context.getRefreshId();
        boolean otherRefreshesInBatchActive = activeRefreshes.values().stream()
                .anyMatch(ctx -> !ctx.getTopic().equals(topic) &&
                                 ctx.getRefreshId().equals(batchId) &&
                                 ctx.getState() != DataRefreshState.COMPLETED);

        if (!otherRefreshesInBatchActive) {
            pipeConnector.resumePipeCalls();
            log.info("Pipe calls RESUMED after refresh of topic: {} (all topics in batch {} completed)", topic, batchId);
            // Clear refresh_id when batch completes
            log.info("Refresh batch {} completed", currentRefreshId);
            currentRefreshId = null;
        } else {
            // Count how many topics in this batch are still active
            long activeTopicsInBatch = activeRefreshes.values().stream()
                    .filter(ctx -> ctx.getRefreshId().equals(batchId) &&
                                   ctx.getState() != DataRefreshState.COMPLETED)
                    .count();
            log.info("Pipe calls remain PAUSED ({} other topic(s) in batch {} still in progress)",
                    activeTopicsInBatch, batchId);
        }

        // Clear state for this topic only (keeps other topics' state)
        stateStore.clearState(topic);
        log.info("Refresh state cleared for topic: {}", topic);

        // Cleanup after delay (keep context for status queries)
        // Store refresh ID to ensure we only remove THIS refresh, not a newer one
        final String completedRefreshId = context.getRefreshId();
        scheduler.schedule(() -> {
            // Only remove if the context still belongs to this refresh
            // This prevents removing a newer refresh that started within 60 seconds
            DataRefreshContext currentContext = activeRefreshes.get(topic);
            if (currentContext != null && completedRefreshId.equals(currentContext.getRefreshId())) {
                activeRefreshes.remove(topic);
                log.info("Refresh context removed for topic: {} (refresh_id: {})", topic, completedRefreshId);
            } else if (currentContext != null) {
                log.info("Skipping context removal for topic: {} - newer refresh is active (current: {}, completed: {})",
                        topic, currentContext.getRefreshId(), completedRefreshId);
            } else {
                log.debug("Context already removed for topic: {} (refresh_id: {})", topic, completedRefreshId);
            }
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Check for READY ACK timeout
     */
    private void checkReadyAckTimeout(String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        if (context.getState() == DataRefreshState.READY_SENT &&
            !context.allReadyAcksReceived()) {
            Set<String> missing = getMissingReadyAcks(context);
            log.warn("READY ACK timeout for topic {} - missing ACKs from: {}", topic, missing);
            sendReady(topic, context);
        }
    }

    /**
     * Get consumers that haven't sent READY ACK yet
     */
    private Set<String> getMissingReadyAcks(DataRefreshContext context) {
        Set<String> missing = new HashSet<>(context.getExpectedConsumers());
        missing.removeAll(context.getReceivedReadyAcks());
        return missing;
    }

    /**
     * Resume refresh from saved state after broker restart
     */
    private void resumeRefresh(DataRefreshContext context) {
        String topic = context.getTopic();
        DataRefreshState state = context.getState();

        log.info("Resuming refresh for topic {} from state: {}", topic, state);

        switch (state) {
            case RESET_SENT:
                // Still waiting for RESET ACKs - start periodic retry
                Set<String> missingResetAcks = getMissingResetAcks(context);
                log.info("Resuming from RESET_SENT - starting periodic RESET retry for {} consumer(s): {}",
                        missingResetAcks.size(), missingResetAcks);

                // Re-record RESET sent metrics with current refresh_id (in-memory map was cleared on restart)
                String resumeRefreshId = context.getRefreshId();
                for (String consumer : context.getExpectedConsumers()) {
                    metrics.recordResetSent(topic, consumer, resumeRefreshId);
                }
                log.info("Re-recorded RESET sent metrics for topic {} with refresh_id: {}", topic, resumeRefreshId);

                // Immediately send RESET once
                remoteConsumers.broadcastResetToTopic(topic);
                log.info("Initial RESET sent to all consumers for topic: {}", topic);

                // Schedule periodic retry every 5 seconds until all ACKs received
                ScheduledFuture<?> resetTask = scheduler.scheduleWithFixedDelay(
                        () -> retryResetBroadcast(topic),
                        RESET_RETRY_INTERVAL_MS,
                        RESET_RETRY_INTERVAL_MS,
                        TimeUnit.MILLISECONDS
                );
                resetRetryTasks.put(topic, resetTask);
                log.info("Scheduled RESET retry task for topic {} (interval: {}ms)", topic, RESET_RETRY_INTERVAL_MS);
                break;

            case REPLAYING:
                // Continue replay progress monitoring
                log.info("Resuming from REPLAYING - {} consumers ACKed, checking progress",
                        context.getReceivedResetAcks().size());

                // Re-record RESET sent metrics with current refresh_id (Timer was cleared on restart)
                String replayingRefreshId = context.getRefreshId();
                for (String consumer : context.getExpectedConsumers()) {
                    metrics.recordResetSent(topic, consumer, replayingRefreshId);

                    // If RESET ACK was already received before restart, record the historical duration
                    if (context.getReceivedResetAcks().contains(consumer)) {
                        Instant resetSentTime = context.getResetSentTime();
                        Instant resetAckTime = context.getResetAckTimes().get(consumer);
                        if (resetSentTime != null && resetAckTime != null) {
                            long durationMs = java.time.Duration.between(resetSentTime, resetAckTime).toMillis();
                            metrics.recordResetAckDuration(topic, consumer, replayingRefreshId, durationMs);
                            log.info("Recorded historical RESET ACK duration for consumer {}: {}ms", consumer, durationMs);
                        }
                    }
                }
                log.info("Re-recorded RESET sent metrics for topic {} with refresh_id: {}", topic, replayingRefreshId);

                ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                        () -> checkReplayProgress(topic),
                        REPLAY_CHECK_INTERVAL_MS,
                        REPLAY_CHECK_INTERVAL_MS,
                        TimeUnit.MILLISECONDS
                );
                replayCheckTasks.put(topic, task);
                break;

            case READY_SENT:
                // Continue waiting for READY ACKs
                log.info("Resuming from READY_SENT - waiting for ACKs from: {}", getMissingReadyAcks(context));

                // Re-record both RESET and READY sent metrics (Timers were cleared on restart)
                String readySentRefreshId = context.getRefreshId();
                for (String consumer : context.getExpectedConsumers()) {
                    metrics.recordResetSent(topic, consumer, readySentRefreshId);
                    metrics.recordReadySent(topic, consumer, readySentRefreshId);

                    // Record historical RESET ACK duration if available
                    if (context.getReceivedResetAcks().contains(consumer)) {
                        Instant resetSentTime = context.getResetSentTime();
                        Instant resetAckTime = context.getResetAckTimes().get(consumer);
                        if (resetSentTime != null && resetAckTime != null) {
                            long durationMs = java.time.Duration.between(resetSentTime, resetAckTime).toMillis();
                            metrics.recordResetAckDuration(topic, consumer, readySentRefreshId, durationMs);
                            log.info("Recorded historical RESET ACK duration for consumer {}: {}ms", consumer, durationMs);
                        }
                    }

                    // Record historical READY ACK duration if already received before restart
                    if (context.getReceivedReadyAcks().contains(consumer)) {
                        Instant readySentTime = context.getReadySentTime();
                        Instant readyAckTime = context.getReadyAckTimes().get(consumer);
                        if (readySentTime != null && readyAckTime != null) {
                            long durationMs = java.time.Duration.between(readySentTime, readyAckTime).toMillis();
                            metrics.recordReadyAckDuration(topic, consumer, readySentRefreshId, durationMs);
                            log.info("Recorded historical READY ACK duration for consumer {}: {}ms", consumer, durationMs);
                        }
                    }
                }
                log.info("Re-recorded RESET and READY sent metrics for topic {} with refresh_id: {}", topic, readySentRefreshId);

                scheduler.schedule(
                        () -> checkReadyAckTimeout(topic),
                        READY_ACK_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS
                );
                break;

            case COMPLETED:
                // Already done, cleanup
                log.info("Resuming from COMPLETED - cleaning up");
                activeRefreshes.remove(topic);
                stateStore.clearState();
                break;

            case ABORTED:
                log.warn("Resuming from ABORTED state - manual intervention needed");
                break;

            default:
                log.warn("Unknown state during resume: {}", state);
        }
    }

    /**
     * Get consumers that haven't sent RESET ACK yet
     */
    private Set<String> getMissingResetAcks(DataRefreshContext context) {
        Set<String> missing = new HashSet<>(context.getExpectedConsumers());
        missing.removeAll(context.getReceivedResetAcks());
        return missing;
    }

    /**
     * For health check indicator
     */
    public DataRefreshContext getCurrentRefreshContext() {
        return activeRefreshes.values().stream().findFirst().orElse(null);
    }

    /**
     * Get refresh status for specific topic
     */
    public DataRefreshContext getRefreshStatus(String topic) {
        return activeRefreshes.get(topic);
    }

    /**
     * Check if any refresh is in progress
     */
    public boolean isRefreshInProgress() {
        return !activeRefreshes.isEmpty();
    }

    /**
     * Get current refresh ID for metrics tracking
     */
    public String getCurrentRefreshId() {
        return currentRefreshId;
    }

    /**
     * Get refresh ID for a specific topic
     * More robust than getCurrentRefreshId() for multi-batch scenarios
     */
    public String getRefreshIdForTopic(String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        String refreshId = context != null ? context.getRefreshId() : null;
        log.info("getRefreshIdForTopic({}): context={}, refreshId={}", topic, (context != null), refreshId);
        return refreshId;
    }

    /**
     * Get refresh type for a topic currently undergoing refresh
     * @param topic Topic name
     * @return Refresh type (e.g., "LOCAL", "GLOBAL") or null if not in refresh
     */
    public String getRefreshTypeForTopic(String topic) {
        DataRefreshContext context = activeRefreshes.get(topic);
        return context != null ? context.getRefreshType() : null;
    }

    /**
     * Get current refresh topic (if any)
     */
    public String getCurrentRefreshTopic() {
        DataRefreshContext context = getCurrentRefreshContext();
        return context != null ? context.getTopic() : null;
    }

    /**
     * Shutdown executor on application shutdown
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down DataRefreshManager");

        // Cancel all scheduled tasks
        int resetTaskCount = resetRetryTasks.size();
        for (ScheduledFuture<?> task : resetRetryTasks.values()) {
            task.cancel(false);
        }
        resetRetryTasks.clear();
        log.info("Cancelled {} RESET retry task(s)", resetTaskCount);

        int replayTaskCount = replayCheckTasks.size();
        for (ScheduledFuture<?> task : replayCheckTasks.values()) {
            task.cancel(false);
        }
        replayCheckTasks.clear();
        log.info("Cancelled {} replay check task(s)", replayTaskCount);

        // Record shutdown time for all active refreshes
        if (!activeRefreshes.isEmpty()) {
            Instant shutdownTime = Instant.now();
            log.info("Recording shutdown time for {} active refresh(es)", activeRefreshes.size());

            for (DataRefreshContext context : activeRefreshes.values()) {
                context.recordShutdown(shutdownTime);
                stateStore.saveState(context);
                log.info("Recorded shutdown for topic: {} at {}", context.getTopic(), shutdownTime);
            }
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
