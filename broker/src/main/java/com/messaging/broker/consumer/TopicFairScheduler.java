package com.messaging.broker.consumer;

import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Fair scheduler with per-topic work queue bounds
 * Prevents hot topics from starving quieter ones
 *
 * Each topic gets its own semaphore limiting max in-flight tasks.
 * If a topic's quota is exhausted, tasks for that topic are skipped
 * (not queued), allowing other topics to proceed.
 */
@Singleton
public class TopicFairScheduler {
    private static final Logger log = LoggerFactory.getLogger(TopicFairScheduler.class);

    private final ScheduledExecutorService scheduler;
    private final Map<String, Semaphore> topicSemaphores;
    private final int maxInFlightPerTopic;

    public TopicFairScheduler() {
        this(Runtime.getRuntime().availableProcessors() * 2, 4);
    }

    public TopicFairScheduler(int threads, int maxInFlightPerTopic) {
        this.scheduler = Executors.newScheduledThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setName("TopicFairScheduler-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        this.topicSemaphores = new ConcurrentHashMap<>();
        this.maxInFlightPerTopic = maxInFlightPerTopic;

        log.info("TopicFairScheduler initialized: threads={}, maxInFlightPerTopic={}",
                threads, maxInFlightPerTopic);
    }

    /**
     * Schedule task with per-topic fairness
     *
     * @param topic Topic name for fairness tracking
     * @param task Task to execute
     * @param delay Delay before execution
     * @param unit Time unit for delay
     */
    public void schedule(String topic, Runnable task, long delay, TimeUnit unit) {
        // Get or create semaphore for this topic
        Semaphore semaphore = topicSemaphores.computeIfAbsent(
            topic, k -> new Semaphore(maxInFlightPerTopic)
        );

        scheduler.schedule(() -> {
            // Try to acquire permit (non-blocking)
            if (semaphore.tryAcquire()) {
                try {
                    task.run();
                } catch (Exception e) {
                    log.error("Task execution failed for topic={}", topic, e);
                } finally {
                    semaphore.release();
                }
            } else {
                // No permit available — reschedule the task so the delivery loop
                // does not die. The task itself is responsible for rescheduling
                // on success/failure, but it never runs here so we must reschedule.
                log.trace("Skipping task for topic={} - max in-flight reached, rescheduling", topic);
                schedule(topic, task, delay, unit);
            }
        }, delay, unit);
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
