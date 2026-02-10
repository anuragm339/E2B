package com.messaging.broker.consumer;

import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Persist delivery state for (topic, consumer) pairs
 * Enables safe broker restart without duplicate delivery
 *
 * State format in properties file:
 * - {deliveryKey}.offset = last acknowledged offset
 * - {deliveryKey}.until = timestamp when in-flight expires
 *
 * Where deliveryKey = "clientId:topic"
 */
@Singleton
public class DeliveryStateStore {
    private static final Logger log = LoggerFactory.getLogger(DeliveryStateStore.class);

    private static final String STATE_FILE = "delivery-state.properties";
    private static final long FLUSH_INTERVAL_MS = 5000;  // 5 seconds (same as ConsumerOffsetTracker)

    private final Path stateFilePath;
    private final Map<String, DeliveryState> cache;
    private final ScheduledExecutorService flusher;

    public DeliveryStateStore(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.stateFilePath = Paths.get(dataDir, STATE_FILE);
        this.cache = new ConcurrentHashMap<>();
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("DeliveryStateStore-Flusher");
            t.setDaemon(true);
            return t;
        });

        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create data directory", e);
        }

        loadFromDisk();
        log.info("DeliveryStateStore initialized: stateFile={}, loaded {} entries",
                stateFilePath, cache.size());
    }

    /**
     * Start periodic flush to disk
     */
    @PostConstruct
    public void init() {
        // Start periodic flush
        flusher.scheduleWithFixedDelay(
            this::persistToDisk,
            FLUSH_INTERVAL_MS,
            FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
        log.info("DeliveryStateStore started with {}ms flush interval", FLUSH_INTERVAL_MS);
    }

    /**
     * Get delivery state for a consumer:topic pair
     *
     * @param deliveryKey Format: "clientId:topic"
     * @return DeliveryState (never null, returns zero state if not found)
     */
    public DeliveryState getState(String deliveryKey) {
        return cache.getOrDefault(deliveryKey, new DeliveryState(0, 0));
    }

    /**
     * Save delivery state for a consumer:topic pair
     * State is updated in memory immediately and flushed to disk periodically
     *
     * @param deliveryKey Format: "clientId:topic"
     * @param lastAckedOffset Last offset that was acknowledged
     * @param inFlightUntil Timestamp when in-flight status expires
     */
    public void saveState(String deliveryKey, long lastAckedOffset, long inFlightUntil) {
        DeliveryState state = new DeliveryState(lastAckedOffset, inFlightUntil);
        cache.put(deliveryKey, state);

        // NO immediate flush - periodic scheduler handles persistence
        // This eliminates blocking I/O from the adaptive polling loop

        log.trace("Saved delivery state: {}={}", deliveryKey, state);
    }

    /**
     * Update only the acknowledged offset (keep existing inFlightUntil)
     *
     * @param deliveryKey Format: "clientId:topic"
     * @param lastAckedOffset New acknowledged offset
     */
    public void updateAckedOffset(String deliveryKey, long lastAckedOffset) {
        DeliveryState current = getState(deliveryKey);
        saveState(deliveryKey, lastAckedOffset, current.inFlightUntil);
    }

    /**
     * Update only the in-flight expiration (keep existing lastAckedOffset)
     *
     * @param deliveryKey Format: "clientId:topic"
     * @param inFlightUntil Timestamp when in-flight expires
     */
    public void updateInFlightUntil(String deliveryKey, long inFlightUntil) {
        DeliveryState current = getState(deliveryKey);
        saveState(deliveryKey, current.lastAckedOffset, inFlightUntil);
    }

    /**
     * Immediate flush to disk (used during shutdown or critical operations)
     */
    public void flush() {
        persistToDisk();
    }

    /**
     * Check if delivery is currently in-flight
     *
     * @param deliveryKey Format: "clientId:topic"
     * @return true if in-flight and not expired
     */
    public boolean isInFlight(String deliveryKey) {
        DeliveryState state = getState(deliveryKey);
        return state.inFlightUntil > System.currentTimeMillis();
    }

    /**
     * Clear in-flight status (set expiration to 0)
     *
     * @param deliveryKey Format: "clientId:topic"
     */
    public void clearInFlight(String deliveryKey) {
        DeliveryState current = getState(deliveryKey);
        saveState(deliveryKey, current.lastAckedOffset, 0);
    }

    /**
     * Persist cache to disk
     */
    private synchronized void persistToDisk() {
        Properties props = new Properties();

        cache.forEach((key, state) -> {
            props.setProperty(key + ".offset", String.valueOf(state.lastAckedOffset));
            props.setProperty(key + ".until", String.valueOf(state.inFlightUntil));
        });

        try {
            // Write to temp file first, then atomic rename
            Path tempFile = stateFilePath.resolveSibling(STATE_FILE + ".tmp");

            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
                props.store(fos, "Delivery state - DO NOT EDIT MANUALLY");
            }

            Files.move(tempFile, stateFilePath, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            log.error("Failed to persist delivery state", e);
        }
    }

    /**
     * Load state from disk
     */
    private void loadFromDisk() {
        if (!Files.exists(stateFilePath)) {
            log.info("No existing delivery state file, starting fresh");
            return;
        }

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(stateFilePath.toFile())) {
            props.load(fis);

            // Parse properties back into cache
            props.stringPropertyNames().stream()
                .filter(k -> k.endsWith(".offset"))
                .forEach(k -> {
                    String key = k.replace(".offset", "");
                    long offset = Long.parseLong(props.getProperty(k));
                    long until = Long.parseLong(props.getProperty(key + ".until", "0"));
                    cache.put(key, new DeliveryState(offset, until));
                });

            log.info("Loaded {} delivery states from disk", cache.size());

        } catch (IOException e) {
            log.error("Failed to load delivery state, starting fresh", e);
        }
    }

    /**
     * Get all tracked delivery keys
     */
    public int getTrackedCount() {
        return cache.size();
    }

    /**
     * Shutdown - stop flusher and perform final flush
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down DeliveryStateStore...");
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(5, TimeUnit.SECONDS)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            flusher.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final flush to ensure all state is persisted
        persistToDisk();
        log.info("DeliveryStateStore shutdown complete");
    }

    /**
     * Delivery state for a (consumer, topic) pair
     */
    public static class DeliveryState {
        public final long lastAckedOffset;
        public final long inFlightUntil;  // Timestamp when in-flight expires

        public DeliveryState(long lastAckedOffset, long inFlightUntil) {
            this.lastAckedOffset = lastAckedOffset;
            this.inFlightUntil = inFlightUntil;
        }

        @Override
        public String toString() {
            return "DeliveryState{offset=" + lastAckedOffset +
                   ", inFlightUntil=" + inFlightUntil + "}";
        }
    }
}
