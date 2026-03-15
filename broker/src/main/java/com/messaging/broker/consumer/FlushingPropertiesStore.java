package com.messaging.broker.consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

        this.delegate = new PropertiesFileStore(dataDir, fileName, description);
        this.flushIntervalMs = flushIntervalMs;
        this.description = description;

        this.flusher = Executors.newSingleThreadScheduledExecutor(runnable -> {
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
        flusher.scheduleWithFixedDelay(
                delegate::persistToDisk,
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
        log.info("Stopping periodic flush for {}...", description);

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
        delegate.put(key, value);
    }

    @Override
    public void putAll(Map<String, String> properties) {
        delegate.putAll(properties);
    }

    @Override
    public void remove(String key) {
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
        delegate.flush();
    }

    @Override
    public void clear() {
        delegate.clear();
    }
}
