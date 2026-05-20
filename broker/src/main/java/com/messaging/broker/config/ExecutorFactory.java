package com.messaging.broker.config;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Factory for creating executor and scheduler beans.
 */
@Factory
public class ExecutorFactory {
    private static final Logger log = LoggerFactory.getLogger(ExecutorFactory.class);

    /**
     * Create dedicated executor for ACK processing.
     * This prevents ACKs from being blocked by heavy delivery operations on Netty event loop.
     *
     * @return ExecutorService for ACK processing
     */
    @Singleton
    @Named("ackExecutor")
    public ExecutorService ackExecutor(
            @Value("${executor.ack.threads:4}") int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads,
                createThreadFactory("ACK-Processor"));
        log.info("Created ACK executor with {} threads", threads);
        return executor;
    }

    /**
     * Create scheduled executor for consumer READY retries and ACK timeouts.
     *
     * @return ScheduledExecutorService for consumer workflow scheduling
     */
    @Singleton
    @Named("consumerScheduler")
    public ScheduledExecutorService consumerScheduler(
            @Value("${executor.consumer-scheduler.threads:4}") int threads) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(threads,
                createThreadFactory("Consumer-Scheduler"));
        log.info("Created consumer scheduler with {} threads", threads);
        return scheduler;
    }

    /**
     * Create executor for storage-backed batch reads.
     *
     * @return ExecutorService for blocking storage read tasks
     */
    @Singleton
    @Named("storageExecutor")
    public ExecutorService storageExecutor(
            @Value("${executor.storage.threads:4}") int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads,
                createThreadFactory("Storage-Executor"));
        log.info("Created storage executor with {} threads", threads);
        return executor;
    }

    /**
     * Create dedicated executor for async ACK replay / RocksDB persistence work.
     *
     * Keeping this separate from storageExecutor prevents post-ACK replay tasks from
     * starving delivery reads. When both pools were shared, the delivery scheduler
     * could block on Future.get() while all storage workers were busy replaying ACKs,
     * freezing every consumer even though no pending ACK gate was active.
     *
     * @return ExecutorService for async ACK persistence tasks
     */
    @Singleton
    @Named("ackStorageExecutor")
    public ExecutorService ackStorageExecutor(
            @Value("${executor.ack-storage.threads:2}") int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads,
                createThreadFactory("Ack-Storage-Executor"));
        log.info("Created ACK storage executor with {} threads", threads);
        return executor;
    }

    /**
     * Create scheduled executor for the refresh coordinator.
     *
     * @return ScheduledExecutorService for data refresh tasks
     */
    @Singleton
    @Named("dataRefreshScheduler")
    public ScheduledExecutorService dataRefreshScheduler() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2,
                createThreadFactory("DataRefresh-Scheduler"));
        log.info("Created data refresh scheduler with 2 threads");
        return scheduler;
    }

    /**
     * Create scheduled executor for periodic flush operations.
     *
     * @return ScheduledExecutorService for flushing
     */
    @Singleton
    @Named("flushScheduler")
    public ScheduledExecutorService flushScheduler() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
                createThreadFactory("Flush-Scheduler"));
        log.info("Created flush scheduler with 1 thread");
        return scheduler;
    }

    /**
     * Create a thread factory with consistent naming and daemon configuration.
     *
     * @param namePrefix Thread name prefix
     * @return ThreadFactory
     */
    private ThreadFactory createThreadFactory(String namePrefix) {
        return runnable -> {
            Thread t = new Thread(runnable);
            t.setName(namePrefix + "-" + t.getId());
            t.setDaemon(true);
            return t;
        };
    }
}
