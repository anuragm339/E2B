package com.messaging.broker.refresh;

import com.messaging.broker.consumer.RemoteConsumerRegistry;
import com.messaging.broker.metrics.DataRefreshMetrics;
import com.messaging.common.api.PipeConnector;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

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

    private final RemoteConsumerRegistry remoteConsumers;
    private final PipeConnector pipeConnector;
    private final DataRefreshConfiguration config;
    private final DataRefreshStateStore stateStore;
    private final DataRefreshMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final Map<String, DataRefreshContext> activeRefreshes;
    private final Map<String, ScheduledFuture<?>> replayCheckTasks;

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

        for (Map.Entry<String, DataRefreshContext> entry : savedRefreshes.entrySet()) {
            String topic = entry.getKey();
            DataRefreshContext context = entry.getValue();

            log.info("Resuming refresh for topic: {}, state: {}", topic, context.getState());
            activeRefreshes.put(topic, context);
            resumeRefresh(context);
        }

        log.info("Successfully resumed {} active refresh(es)", savedRefreshes.size());
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

        // Reset all metrics ONLY if this is the first refresh (no active refreshes)
        // This ensures dashboard shows only current refresh batch data
        // Prometheus historical data is preserved for time-series queries
        if (activeRefreshes.isEmpty()) {
            metrics.resetMetricsForNewRefresh();
            log.info("Metrics reset for new refresh batch");
        } else {
            log.info("Metrics NOT reset - adding to existing refresh batch");
        }

        DataRefreshContext context = new DataRefreshContext(
                topic,
                expectedConsumers
        );
        activeRefreshes.put(topic, context);

        context.setState(DataRefreshState.RESET_SENT);
        context.setResetSentTime(Instant.now());

        // Record metrics: refresh started
        metrics.recordRefreshStarted(topic, "LOCAL");

        // Pause pipe calls before starting refresh
        pipeConnector.pausePipeCalls();
        log.info("Pipe calls PAUSED for refresh of topic: {}", topic);

        // Broadcast RESET to all consumers
        remoteConsumers.broadcastResetToTopic(topic);
        log.info("RESET sent to all consumers for topic: {}", topic);

        // Record metrics: RESET sent to each expected consumer
        for (String consumer : expectedConsumers) {
            metrics.recordResetSent(topic, consumer);
        }

        // Persist state immediately
        stateStore.saveState(context);

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
        metrics.recordResetAckReceived(topic, consumerGroupTopic);

        // Persist state after each ACK
        stateStore.saveState(context);

        // Transition to REPLAYING state on first ACK
        if (context.getState() == DataRefreshState.RESET_SENT) {
            context.setState(DataRefreshState.REPLAYING);
            stateStore.saveState(context);

            remoteConsumers.resetConsumerOffset(clientId, topic, 0);
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
            // Record metrics: replay started
            metrics.recordReplayStarted(topic, topic);  // Use topic as consumer ID

            // Trigger delivery from offset 0
            remoteConsumers.notifyNewMessageForConsumer(clientId, topic, 0);

            log.info("Replay triggered for consumer: {} starting from offset 0", clientId);

        } catch (Exception e) {
            log.error("Failed to start replay for consumer: {}", clientId, e);
        }
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
            // Get ALL consumers for this topic and trigger replay for each
            List<String> allConsumerIds = remoteConsumers.getAllConsumerIds(topic);
            if (allConsumerIds == null || allConsumerIds.isEmpty()) {
                log.info("No remote consumers found for topic {}, cannot start replay", topic);
            } else {
                log.debug("Triggering replay for {} consumers on topic {}", allConsumerIds.size(), topic);
                for (String clientId : allConsumerIds) {
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
        metrics.recordReadyAckReceived(topic, consumerGroupTopic);

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

        // Record metrics: refresh completed successfully
        metrics.recordRefreshCompleted(topic, "LOCAL", "SUCCESS");

        // Resume pipe calls only if NO other refreshes are in progress
        boolean otherRefreshesActive = activeRefreshes.values().stream()
                .anyMatch(ctx -> !ctx.getTopic().equals(topic) &&
                                 ctx.getState() != DataRefreshState.COMPLETED);

        if (!otherRefreshesActive) {
            pipeConnector.resumePipeCalls();
            log.info("Pipe calls RESUMED after refresh of topic: {} (no other active refreshes)", topic);
        } else {
            log.info("Pipe calls remain PAUSED (other topic refreshes still in progress)");
        }

        // Clear state for this topic only (keeps other topics' state)
        stateStore.clearState(topic);
        log.info("Refresh state cleared for topic: {}", topic);

        // Cleanup after delay (keep context for status queries)
        scheduler.schedule(() -> {
            activeRefreshes.remove(topic);
            log.info("Refresh context removed for topic: {}", topic);
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
                // Still waiting for RESET ACKs
                log.info("Resuming from RESET_SENT - waiting for ACKs from: {}", getMissingResetAcks(context));
                // Normal handleResetAck() will handle incoming ACKs
                break;

            case REPLAYING:
                // Continue replay progress monitoring
                log.info("Resuming from REPLAYING - {} consumers ACKed, checking progress",
                        context.getReceivedResetAcks().size());
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
     * Get current refresh topic (if any)
     */
    public String getCurrentRefreshTopic() {
        DataRefreshContext context = getCurrentRefreshContext();
        return context != null ? context.getTopic() : null;
    }

    /**
     * Shutdown executor on application shutdown
     */
    public void shutdown() {
        log.info("Shutting down DataRefreshManager");
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
