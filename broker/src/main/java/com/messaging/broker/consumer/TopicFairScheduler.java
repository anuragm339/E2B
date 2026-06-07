package com.messaging.broker.consumer;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fair scheduler with per-topic work queue bounds.
 *
 * Prevents hot topics from starving quieter ones. Each topic gets its own
 * semaphore limiting max in-flight tasks. If a topic's quota is exhausted,
 * tasks for that topic are skipped instead of queued, allowing other topics
 * to proceed.
 *
 * Tracks one pending retry per delivery key to prevent unbounded recursive
 * scheduling during refresh saturation.
 */
@Singleton
public class TopicFairScheduler {
    private static final Logger log = LoggerFactory.getLogger(TopicFairScheduler.class);

    private final ScheduledExecutorService scheduler;
    private final Map<String, Semaphore> topicSemaphores;
    // Track pending retry tasks to prevent unbounded recursive scheduling.
    private final Map<String, RetrySlot> pendingRetries;
    private final int maxInFlightPerTopic;
    private final AtomicLong retryGeneration = new AtomicLong();
    private final AtomicLong threadCounter = new AtomicLong();

    private static final class RetrySlot {
        private final long generation;
        private volatile ScheduledFuture<?> future;

        private RetrySlot(long generation) {
            this.generation = generation;
        }
    }

    public TopicFairScheduler(
            @Value("${broker.consumer.fairness.threads:2}") int threads,
            @Value("${broker.consumer.fairness.max-in-flight-per-topic:1}") int maxInFlightPerTopic) {
        this.scheduler = Executors.newScheduledThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setName("TopicFairScheduler-" + threadCounter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        this.topicSemaphores = new ConcurrentHashMap<>();
        this.pendingRetries = new ConcurrentHashMap<>(); // B11-5 fix
        this.maxInFlightPerTopic = Math.max(1, maxInFlightPerTopic);

        log.info("TopicFairScheduler initialized: threads={}, maxInFlightPerTopic={}",
                threads, this.maxInFlightPerTopic);
    }

    /**
     * Schedule task with per-topic fairness
     *
     * @param topic Topic name for fairness tracking
     * @param task Task to execute
     * @param delay Delay before execution
     * @param unit Time unit for delay
     */
    public ScheduledFuture<?> schedule(String topic, Runnable task, long delay, TimeUnit unit) {
        return scheduleWithKey(topic, topic, task, delay, unit);
    }

    /**
     * Schedule a task with per-topic fairness and a unique delivery key.
     *
     * During refresh-driven backpressure, consumers can saturate the semaphore.
     * Without retry tracking, each failed task can recursively reschedule and
     * create unbounded task buildup. This method allows only one pending retry
     * per delivery key.
     *
     * @param topic Topic name for fairness tracking (semaphore key)
     * @param deliveryKey Unique key for the scheduled delivery task
     * @param task Task to execute
     * @param delay Delay before execution
     * @param unit Time unit for delay
     */
    public ScheduledFuture<?> scheduleWithKey(String topic, String deliveryKey, Runnable task, long delay, TimeUnit unit) {
        // Get or create semaphore for this topic
        Semaphore semaphore = topicSemaphores.computeIfAbsent(
            topic, k -> new Semaphore(maxInFlightPerTopic)
        );

        return scheduler.schedule(
                () -> runAttempt(topic, deliveryKey, task, delay, unit, semaphore),
                delay,
                unit);
    }

    private void runAttempt(
            String topic,
            String deliveryKey,
            Runnable task,
            long retryDelay,
            TimeUnit retryUnit,
            Semaphore semaphore) {
        if (!semaphore.tryAcquire()) {
            scheduleSingleRetry(topic, deliveryKey, task, retryDelay, retryUnit, semaphore);
            return;
        }

        try {
            task.run();
        } catch (RuntimeException e) {
            log.error("Task execution failed for topic={}, deliveryKey={}", topic, deliveryKey, e);
            throw e;
        } finally {
            semaphore.release();
        }
    }

    private void scheduleSingleRetry(
            String topic,
            String deliveryKey,
            Runnable task,
            long delay,
            TimeUnit unit,
            Semaphore semaphore) {
        RetrySlot slot = new RetrySlot(retryGeneration.incrementAndGet());
        RetrySlot existing = pendingRetries.putIfAbsent(deliveryKey, slot);
        if (existing != null) {
            log.trace("Skipping task for topic={}, deliveryKey={} - retry generation={} already pending",
                    topic, deliveryKey, existing.generation);
            return;
        }

        try {
            long retryDelayMs = Math.max(1L, unit.toMillis(delay));
            slot.future = scheduler.schedule(() -> {
                pendingRetries.remove(deliveryKey, slot);
                runAttempt(topic, deliveryKey, task, delay, unit, semaphore);
            }, retryDelayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            pendingRetries.remove(deliveryKey, slot);
            throw e;
        }
    }

    /**
     * Execute task immediately with fairness constraints
     *
     * @param topic Topic name for fairness tracking
     * @param task Task to execute
     */
    public void execute(String topic, Runnable task) {
        schedule(topic, task, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Get current in-flight count for a topic
     *
     * @param topic Topic name
     * @return Number of permits currently held (tasks in flight)
     */
    public int getInFlightCount(String topic) {
        Semaphore semaphore = topicSemaphores.get(topic);
        if (semaphore == null) {
            return 0;
        }
        return maxInFlightPerTopic - semaphore.availablePermits();
    }

    /**
     * Get available permits for a topic
     *
     * @param topic Topic name
     * @return Number of available permits
     */
    public int getAvailablePermits(String topic) {
        Semaphore semaphore = topicSemaphores.get(topic);
        if (semaphore == null) {
            return maxInFlightPerTopic;
        }
        return semaphore.availablePermits();
    }

    /**
     * Shutdown scheduler gracefully
     */
    public void shutdown() {
        log.info("Shutting down TopicFairScheduler...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("TopicFairScheduler stopped");
    }
}
