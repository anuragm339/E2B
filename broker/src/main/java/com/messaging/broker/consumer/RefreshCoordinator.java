package com.messaging.broker.consumer;
import com.messaging.broker.consumer.BatchDeliveryService;
import com.messaging.broker.consumer.RefreshGatePolicy;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Central coordinator for the refresh workflow.
 *
 * Orchestrates startup, reset, replay, ready, and recovery phases.
 */
@Singleton
public class RefreshCoordinator {
    private static final Logger log = LoggerFactory.getLogger(RefreshCoordinator.class);
    private static final long READY_ACK_TIMEOUT_MS = 10000;
    private static final long REPLAY_CHECK_INTERVAL_MS = 1000;
    private static final long RESET_RETRY_INTERVAL_MS = 5000;
    private static final long REFRESH_ABORT_TIMEOUT_MS = 600000; // 10 minutes

    // Services
    private final RefreshStarter initiationService;
    private final ResetPhase resetService;
    private final ReplayPhase replayService;
    private final ReadyPhase readyService;
    private final RefreshRecovery recoveryService;
    private final RefreshWorkflow stateMachine;
    private final RefreshGatePolicy dataRefreshGatePolicy;
    private final BatchDeliveryService batchDeliveryService;
    private final ConsumerRegistry remoteConsumers;

    // Shared state
    private final Map<String, RefreshContext> activeRefreshes;
    private final Map<String, ScheduledFuture<?>> replayCheckTasks;
    private final Map<String, ScheduledFuture<?>> resetRetryTasks;
    private final ScheduledExecutorService scheduler;

    public RefreshCoordinator(
            RefreshStarter initiationService,
            ResetPhase resetService,
            ReplayPhase replayService,
            ReadyPhase readyService,
            RefreshRecovery recoveryService,
            RefreshWorkflow stateMachine,
            RefreshGatePolicy dataRefreshGatePolicy,
            BatchDeliveryService batchDeliveryService,
            ConsumerRegistry remoteConsumers) {
        this.initiationService = initiationService;
        this.resetService = resetService;
        this.replayService = replayService;
        this.readyService = readyService;
        this.recoveryService = recoveryService;
        this.stateMachine = stateMachine;
        this.dataRefreshGatePolicy = dataRefreshGatePolicy;
        this.batchDeliveryService = batchDeliveryService;
        this.remoteConsumers = remoteConsumers;

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setName("RefreshCoordinator");
            return t;
        });
        this.activeRefreshes = new ConcurrentHashMap<>();
        this.replayCheckTasks = new ConcurrentHashMap<>();
        this.resetRetryTasks = new ConcurrentHashMap<>();

        // Inject shared state into services
        wireServices();
    }

    /**
     * Inject shared state into all services.
     */
    private void wireServices() {
        // Initiation service needs task maps for cancellation
        if (initiationService instanceof RefreshInitiator) {
            ((RefreshInitiator) initiationService).setSharedState(
                    activeRefreshes, resetRetryTasks, replayCheckTasks);
        }

        // Ready service needs activeRefreshes for batch completion check
        if (readyService instanceof RefreshReadyService) {
            ((RefreshReadyService) readyService).setSharedState(
                    activeRefreshes, null);
        }

        // Recovery service needs task maps and scheduling callbacks
        if (recoveryService instanceof RefreshRecoveryService) {
            RefreshRecoveryService impl = (RefreshRecoveryService) recoveryService;
            impl.setSharedState(activeRefreshes, resetRetryTasks, replayCheckTasks);
            impl.setSchedulingCallbacks(
                    this::scheduleResetRetry,
                    this::scheduleReplayCheck,
                    this::scheduleReadyTimeout
            );
        }
    }

    @PostConstruct
    public void init() {
        dataRefreshGatePolicy.setDataRefreshCoordinator(this);
        batchDeliveryService.setDataRefreshCoordinator(this);
        remoteConsumers.setRefreshCoordinator(this);
        log.info("RefreshCoordinator initialized");

        // Recover and resume in-progress refreshes
        recoveryService.recoverAndResumeRefreshes();
    }

    /**
     * Start a refresh for a topic.
     */
    public CompletableFuture<RefreshResult> startRefresh(String topic) {
        CompletableFuture<RefreshResult> result = initiationService.startRefresh(topic);

        // Send RESET after initiation
        RefreshContext context = activeRefreshes.get(topic);
        if (context != null && context.getState() == RefreshState.RESET_SENT) {
            resetService.sendReset(topic, context);

            // Schedule periodic RESET retry
            scheduleResetRetry(topic);
        }

        // Schedule abort watchdog: if the refresh is still in a non-terminal state after 10 minutes, abort it.
        scheduler.schedule(() -> abortRefreshIfStuck(topic), REFRESH_ABORT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        return result;
    }

    /**
     * Abort a refresh that has not completed within the timeout window.
     */
    private void abortRefreshIfStuck(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        RefreshState state = context.getState();
        if (stateMachine.isTerminalState(state)) return;

        log.error("Refresh timeout after {}ms for topic={}, state={}, refreshId={} — aborting",
                REFRESH_ABORT_TIMEOUT_MS, topic, state, context.getRefreshId());

        RefreshWorkflow.StateTransitionResult result = stateMachine.transition(state, RefreshState.ABORTED);
        if (result.isSuccess()) {
            context.setState(RefreshState.ABORTED);

            ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
            if (resetTask != null) resetTask.cancel(false);

            ScheduledFuture<?> replayTask = replayCheckTasks.remove(topic);
            if (replayTask != null) replayTask.cancel(false);

            activeRefreshes.remove(topic);
            log.info("Refresh aborted and cleaned up for topic: {}", topic);
        }
    }

    /**
     * Handle RESET ACK from consumer.
     */
    public void handleResetAck(String consumerGroupTopic, String clientId, String topic, String traceId) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) {
            log.warn("Received RESET ACK from {} for topic {} but no active refresh, traceId={}",
                    consumerGroupTopic, topic, traceId);
            return;
        }

        boolean isFirstAck = resetService.handleResetAck(consumerGroupTopic, clientId, topic, context, traceId);

        if (isFirstAck) {
            // Transition to REPLAYING state
            RefreshWorkflow.StateTransitionResult transition =
                    stateMachine.transition(context.getState(), RefreshState.REPLAYING);

            if (transition.isSuccess()) {
                context.setState(RefreshState.REPLAYING);

                // Cancel RESET retry task
                ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
                if (resetTask != null) {
                    resetTask.cancel(false);
                    log.info("Cancelled RESET retry task for topic {}", topic);
                }

                // Start replay progress monitoring
                scheduleReplayCheck(topic);
            }
        }
    }

    /**
     * Handle READY ACK from consumer.
     */
    public void handleReadyAck(String consumerGroupTopic, String topic, String traceId) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) {
            log.warn("Received READY ACK from {} for topic {} but no active refresh, traceId={}",
                    consumerGroupTopic, topic, traceId);
            return;
        }

        boolean allReceived = readyService.handleReadyAck(consumerGroupTopic, topic, context, traceId);

        if (allReceived) {
            // Transition to COMPLETED state
            RefreshWorkflow.StateTransitionResult transition =
                    stateMachine.transition(context.getState(), RefreshState.COMPLETED);

            if (transition.isSuccess()) {
                readyService.completeRefresh(topic, context);

                // Cleanup after delay
                final String completedRefreshId = context.getRefreshId();
                scheduler.schedule(() -> {
                    RefreshContext currentContext = activeRefreshes.get(topic);
                    if (currentContext != null && completedRefreshId.equals(currentContext.getRefreshId())) {
                        activeRefreshes.remove(topic);
                        log.info("Refresh context removed for topic: {}", topic);
                    }
                }, 60, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Schedule periodic RESET retry task.
     */
    private void scheduleResetRetry(String topic) {
        ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                () -> retryResetBroadcast(topic),
                RESET_RETRY_INTERVAL_MS,
                RESET_RETRY_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        resetRetryTasks.put(topic, task);
        log.info("Scheduled RESET retry task for topic {}", topic);
    }

    /**
     * Schedule periodic replay check task.
     */
    private void scheduleReplayCheck(String topic) {
        ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(
                () -> checkReplayProgress(topic),
                REPLAY_CHECK_INTERVAL_MS,
                REPLAY_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        replayCheckTasks.put(topic, task);
        log.info("Scheduled replay check task for topic {}", topic);
    }

    /**
     * Schedule READY ACK timeout check.
     */
    private void scheduleReadyTimeout(String topic) {
        scheduler.schedule(
                () -> checkReadyAckTimeout(topic),
                READY_ACK_TIMEOUT_MS,
                TimeUnit.MILLISECONDS
        );
        log.debug("Scheduled READY timeout check for topic {}", topic);
    }

    /**
     * Retry RESET broadcast (called by scheduler).
     */
    private void retryResetBroadcast(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        resetService.retryResetBroadcast(topic, context);
    }

    /**
     * Check replay progress (called by scheduler).
     */
    private void checkReplayProgress(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        boolean readyToSend = replayService.checkReplayProgress(topic, context);

        if (readyToSend) {
            // Transition to READY_SENT state
            RefreshWorkflow.StateTransitionResult transition =
                    stateMachine.transition(context.getState(), RefreshState.READY_SENT);

            if (transition.isSuccess()) {
                readyService.sendReady(topic, context);

                // Cancel replay check task
                ScheduledFuture<?> task = replayCheckTasks.remove(topic);
                if (task != null) {
                    task.cancel(false);
                    log.info("Cancelled replay check task for topic {}", topic);
                }

                // Schedule READY timeout check
                scheduleReadyTimeout(topic);
            }
        }
    }

    /**
     * Check READY ACK timeout (called by scheduler).
     * Re-schedules itself if ACKs are still missing, so late-connecting consumers get READY.
     */
    private void checkReadyAckTimeout(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        readyService.checkReadyAckTimeout(topic, context);

        // If still waiting for ACKs, schedule another check so newly connected consumers get READY
        if (context.getState() == RefreshState.READY_SENT && !context.allReadyAcksReceived()) {
            scheduleReadyTimeout(topic);
        }
    }

    /**
     * Get refresh status for a topic.
     */
    public RefreshContext getRefreshStatus(String topic) {
        return activeRefreshes.get(topic);
    }

    /**
     * Check if any refresh is in progress.
     */
    public boolean isRefreshInProgress() {
        return !activeRefreshes.isEmpty();
    }

    /**
     * Check if a refresh is active for a specific topic.
     */
    public boolean isRefreshActive(String topic) {
        return activeRefreshes.containsKey(topic);
    }

    /**
     * Get current refresh ID.
     */
    public String getCurrentRefreshId() {
        if (initiationService instanceof RefreshInitiator) {
            return ((RefreshInitiator) initiationService).getCurrentRefreshId();
        }
        return null;
    }

    /**
     * Get refresh ID for a specific topic.
     */
    public String getRefreshIdForTopic(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        return context != null ? context.getRefreshId() : null;
    }

    /**
     * Get refresh type for a topic.
     */
    public String getRefreshTypeForTopic(String topic) {
        RefreshContext context = activeRefreshes.get(topic);
        return context != null ? context.getRefreshType() : null;
    }

    /**
     * Get current refresh context (for health check).
     */
    public RefreshContext getCurrentRefreshContext() {
        return activeRefreshes.values().stream().findFirst().orElse(null);
    }

    /**
     * Get current refresh topic.
     */
    public String getCurrentRefreshTopic() {
        RefreshContext context = getCurrentRefreshContext();
        return context != null ? context.getTopic() : null;
    }

    /**
     * Register a consumer that connected after the refresh started (late joiner).
     *
     * When a consumer subscribes during RESET_SENT or REPLAYING, it missed the RESET broadcast.
     * Treat it as having ACKed RESET so it is included when sendReady() fires at the end of replay.
     * No-op if the refresh is already in READY_SENT or COMPLETED state (handled separately).
     */
    public void registerLateJoiningConsumer(String topic, String groupTopic) {
        RefreshContext context = activeRefreshes.get(topic);
        if (context == null) return;

        RefreshState state = context.getState();
        if (state == RefreshState.RESET_SENT || state == RefreshState.REPLAYING) {
            context.recordResetAck(groupTopic);
            log.info("Late-joining consumer {} registered for topic {} in state {} - will receive refresh READY",
                    groupTopic, topic, state);

            // If this ACK completes the set and we are still in RESET_SENT, drive the transition
            // to REPLAYING ourselves — handleResetAck() was never called for this consumer so the
            // normal transition path was bypassed.
            if (state == RefreshState.RESET_SENT && context.allResetAcksReceived()) {
                RefreshWorkflow.StateTransitionResult transition =
                        stateMachine.transition(context.getState(), RefreshState.REPLAYING);
                if (transition.isSuccess()) {
                    context.setState(RefreshState.REPLAYING);

                    ScheduledFuture<?> resetTask = resetRetryTasks.remove(topic);
                    if (resetTask != null) {
                        resetTask.cancel(false);
                        log.info("Cancelled RESET retry task for topic {} (all ACKs via late-joiner)", topic);
                    }

                    scheduleReplayCheck(topic);
                    log.info("All RESET ACKs received via late-joining consumer — transitioning {} RESET_SENT → REPLAYING",
                            topic);
                }
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down RefreshCoordinator");

        // Cancel all scheduled tasks
        resetRetryTasks.values().forEach(task -> task.cancel(false));
        resetRetryTasks.clear();
        replayCheckTasks.values().forEach(task -> task.cancel(false));
        replayCheckTasks.clear();

        // Record shutdown time for all active refreshes
        if (!activeRefreshes.isEmpty()) {
            Instant shutdownTime = Instant.now();
            log.info("Recording shutdown time for {} active refresh(es)", activeRefreshes.size());

            for (RefreshContext context : activeRefreshes.values()) {
                context.recordShutdown(shutdownTime);
                log.info("Recorded shutdown for topic: {}", context.getTopic());
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
