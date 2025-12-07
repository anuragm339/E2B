package com.messaging.broker.consumer;

import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Tracks and persists consumer offsets to disk
 */
@Singleton
public class ConsumerOffsetTracker {
    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetTracker.class);
    private static final String OFFSET_FILE = "consumer-offsets.properties";
    private static final long FLUSH_INTERVAL_MS = 5000;

    private final String dataDir;
    private final Path offsetFilePath;
    private final Map<String, Long> offsets;
    private final ScheduledExecutorService flusher;

    public ConsumerOffsetTracker(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.dataDir = dataDir;
        this.offsetFilePath = Paths.get(dataDir, OFFSET_FILE);
        this.offsets = new ConcurrentHashMap<>();
        this.flusher = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable);
            t.setName("ConsumerOffsetFlusher");
            return t;
        });

        log.info("ConsumerOffsetTracker initialized with data dir: {}", dataDir);
    }

    /**
     * Initialize - load offsets from disk and start periodic flush
     */
    @PostConstruct
    public void init() {
        // Create data directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            log.error("Failed to create data directory", e);
        }

        // Load persisted offsets
        loadOffsets();

        // Start periodic flush
        flusher.scheduleWithFixedDelay(
            this::flushOffsets,
            FLUSH_INTERVAL_MS,
            FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Get offset for a consumer
     */
    public long getOffset(String consumerId) {
        return offsets.getOrDefault(consumerId, 0L);
    }

    /**
     * Update offset for a consumer
     */
    public void updateOffset(String consumerId, long offset) {
        offsets.put(consumerId, offset);
    }

    /**
     * Reset offset for a consumer
     */
    public void resetOffset(String consumerId, long offset) {
        updateOffset(consumerId, offset);
        flushOffsets(); // Immediate flush on reset
    }

    /**
     * Load offsets from disk (properties file format)
     */
    private void loadOffsets() {
        if (!Files.exists(offsetFilePath)) {
            log.info("No offset file found, starting fresh");
            return;
        }

        try (FileInputStream fis = new FileInputStream(offsetFilePath.toFile())) {
            Properties props = new Properties();
            props.load(fis);

            // Convert properties to offset map
            for (String key : props.stringPropertyNames()) {
                try {
                    long offset = Long.parseLong(props.getProperty(key));
                    offsets.put(key, offset);
                } catch (NumberFormatException e) {
                    log.warn("Invalid offset value for consumer {}: {}", key, props.getProperty(key));
                }
            }

            log.info("Loaded {} consumer offsets from disk", offsets.size());
        } catch (Exception e) {
            log.error("Failed to load offsets from disk", e);
        }
    }

    /**
     * Flush offsets to disk (properties file format)
     */
    private synchronized void flushOffsets() {
        try {
            // Write to temp file first
            Path tempFile = Paths.get(dataDir, OFFSET_FILE + ".tmp");

            // Convert offsets to properties
            Properties props = new Properties();
            for (Map.Entry<String, Long> entry : offsets.entrySet()) {
                props.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }

            // Write properties to file with comments
            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
                props.store(fos, "Consumer Offsets - Updated: " + new java.util.Date());
            }

            // Atomic rename
            Files.move(tempFile, offsetFilePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            log.debug("Flushed {} consumer offsets to disk", offsets.size());
        } catch (Exception e) {
            log.error("Failed to flush offsets to disk", e);
        }
    }

    /**
     * Shutdown - flush final offsets
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down ConsumerOffsetTracker...");

        flusher.shutdown();
        try {
            flusher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final flush
        flushOffsets();

        log.info("ConsumerOffsetTracker shutdown complete");
    }
}
