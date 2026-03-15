package com.messaging.broker.core;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates graceful shutdown of managed executor services.
 */
@Singleton
public class ShutdownCoordinator {
    private static final Logger log = LoggerFactory.getLogger(ShutdownCoordinator.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<ScheduledExecutorService> schedulers = new ArrayList<>();

    @Inject
    public ShutdownCoordinator(
            @Named("ackExecutor") ExecutorService ackExecutor,
            @Named("storageExecutor") ExecutorService storageExecutor,
            @Named("consumerScheduler") ScheduledExecutorService consumerScheduler,
            @Named("dataRefreshScheduler") ScheduledExecutorService dataRefreshScheduler,
            @Named("flushScheduler") ScheduledExecutorService flushScheduler) {

        // Register executors
        this.executors.add(ackExecutor);
        this.executors.add(storageExecutor);

        // Register schedulers (shutdown before executors)
        this.schedulers.add(consumerScheduler);
        this.schedulers.add(dataRefreshScheduler);
        this.schedulers.add(flushScheduler);

        log.info("ShutdownCoordinator initialized with {} schedulers and {} executors",
                schedulers.size(), executors.size());
    }

    /**
     * Graceful shutdown of all managed executors.
     * Called automatically by Micronaut on application shutdown.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Starting graceful shutdown of {} schedulers and {} executors...",
                schedulers.size(), executors.size());

        long startTime = System.currentTimeMillis();

        // Shutdown schedulers first (they may be scheduling work on executors)
        for (ScheduledExecutorService scheduler : schedulers) {
            shutdownExecutor(scheduler, "Scheduler");
        }

        // Then shutdown executors
        for (ExecutorService executor : executors) {
            shutdownExecutor(executor, "Executor");
        }

        long elapsedMs = System.currentTimeMillis() - startTime;
        log.info("Shutdown completed in {}ms", elapsedMs);
    }

    /**
     * Shutdown a single executor with timeout.
     *
     * @param executor Executor to shutdown
     * @param type Type description for logging
     */
    private void shutdownExecutor(ExecutorService executor, String type) {
        try {
            log.debug("{} shutdown initiated", type);
            executor.shutdown();

            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("{} did not terminate within {} seconds, forcing shutdown",
                        type, SHUTDOWN_TIMEOUT_SECONDS);
                List<Runnable> droppedTasks = executor.shutdownNow();
                if (droppedTasks != null && !droppedTasks.isEmpty()) {
                    log.warn("{} dropped {} tasks during forced shutdown",
                            type, droppedTasks.size());
                }

                // Wait a bit longer for forced shutdown
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.error("{} did not terminate even after forced shutdown", type);
                }
            } else {
                log.debug("{} terminated successfully", type);
            }
        } catch (InterruptedException e) {
            log.warn("{} shutdown interrupted, forcing shutdown", type);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
