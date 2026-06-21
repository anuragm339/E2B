package com.messaging.broker.consumer;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Properties repository with automatic periodic flushing.
 *
 * Wraps a PropertiesFileStore and adds background flush capability.
 */
public class FlushingPropertiesStore implements PropertiesStore {
    private static final Logger log = LoggerFactory.getLogger(FlushingPropertiesStore.class);

    private final PropertiesFileStore delegate;
    private final ScheduledExecutorService flusher;
    private final long flushIntervalMs;
    private final String description;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private volatile ScheduledFuture<?> flushTask;
    // True while a destructive download-refresh wipe is in progress. The periodic flush is cancelled
    // and all mutating/flush calls become no-ops so a stray write (in-flight ACK or timer) cannot
    // re-create the on-disk file after clearState has deleted it.
    private volatile boolean paused = false;

    /**
     * Create periodic flush repository.
     *
     * @param dataDir Data directory
     * @param fileName Properties file name
     * @param description Description for logging
     * @param flushIntervalMs Flush interval in milliseconds
     */
    public FlushingPropertiesStore(
            String dataDir,
            String fileName,
            String description,
            long flushIntervalMs) {
        this(
                new PropertiesFileStore(dataDir, fileName, description),
                createFlusher(description),
                description,
                flushIntervalMs);
    }

    FlushingPropertiesStore(
            PropertiesFileStore delegate,
            ScheduledExecutorService flusher,
            String description,
            long flushIntervalMs) {
        this.delegate = delegate;
        this.flusher = flusher;
        this.flushIntervalMs = flushIntervalMs;
        this.description = description;
    }

    private static ScheduledExecutorService createFlusher(String description) {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable);
            t.setName(description + "-Flusher");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start periodic flushing.
     */
    public void start() {
        if (stopped.get()) {
            throw new MessagingException(ErrorCode.BROKER_INVALID_STATE,
                    description + " store has already been stopped");
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        flushTask = flusher.scheduleWithFixedDelay(
                this::flushSafely,
                flushIntervalMs,
                flushIntervalMs,
                TimeUnit.MILLISECONDS
        );
        log.info("Started periodic flush for {} (interval={}ms)", description, flushIntervalMs);
    }

    /**
     * Stop periodic flushing and perform final flush.
     */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        log.info("Stopping periodic flush for {}...", description);

        ScheduledFuture<?> task = flushTask;
        if (task != null) {
            task.cancel(false);
        }
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(5, TimeUnit.SECONDS)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            flusher.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final flush
        delegate.persistToDisk();

        log.info("Stopped periodic flush for {}", description);
    }

    /**
     * Quiesce the store for a destructive download-refresh wipe: cancel the periodic flush, drop the
     * in-memory cache, and reject further writes/flushes until {@link #resumeAfterWipe()}. Closing the
     * write path here is what guarantees a deleted {@code *.properties} file cannot reappear with stale
     * pre-wipe state while {@code clearState} is removing it.
     */
    public synchronized void pauseForWipe() {
        if (paused) {
            return;
        }
        paused = true;
        ScheduledFuture<?> task = flushTask;
        if (task != null) {
            task.cancel(false);
            flushTask = null;
        }
        delegate.clear();
        log.info("Paused {} for refresh wipe (flush cancelled, in-memory state dropped)", description);
    }

    /**
     * Resume after a wipe: reload the in-memory cache from whatever is now on disk (the file may have
     * been deleted or replaced) and re-arm the periodic flush. Safe to call when not paused.
     */
    public synchronized void resumeAfterWipe() {
        if (!paused) {
            return;
        }
        delegate.reload();
        paused = false;
        if (started.get() && !stopped.get()) {
            flushTask = flusher.scheduleWithFixedDelay(
                    this::flushSafely,
                    flushIntervalMs,
                    flushIntervalMs,
                    TimeUnit.MILLISECONDS
            );
        }
        log.info("Resumed {} after refresh wipe (reloaded {} entries from disk)", description, delegate.size());
    }

    /** Whether the store is currently quiesced for a refresh wipe. */
    public boolean isPaused() {
        return paused;
    }

    private void flushSafely() {
        try {
            delegate.persistToDisk();
        } catch (PropertiesStoreException e) {
            log.error("Periodic flush failed for {}; the next scheduled flush will retry", description, e);
        }
    }

    // Delegate all PropertiesStore methods

    @Override
    public String get(String key) {
        return delegate.get(key);
    }

    @Override
    public String get(String key, String defaultValue) {
        return delegate.get(key, defaultValue);
    }

    @Override
    public void put(String key, String value) {
        if (paused) {
            return; // dropped during a refresh wipe so stale state cannot be re-persisted
        }
        delegate.put(key, value);
    }

    @Override
    public void putAll(Map<String, String> properties) {
        if (paused) {
            return;
        }
        delegate.putAll(properties);
    }

    @Override
    public void remove(String key) {
        if (paused) {
            return;
        }
        delegate.remove(key);
    }

    @Override
    public Map<String, String> getAll() {
        return delegate.getAll();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean contains(String key) {
        return delegate.contains(key);
    }

    @Override
    public void flush() {
        if (paused) {
            return; // do not write the wiped-out state back to disk mid-refresh
        }
        delegate.flush();
    }

    @Override
    public void clear() {
        delegate.clear();
    }
}
