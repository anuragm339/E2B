package com.messaging.broker.consumer;

import com.messaging.broker.consumer.FlushingPropertiesStore;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks and persists consumer offsets to disk.
 */
@Singleton
public class ConsumerOffsetTracker {
    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetTracker.class);
    private static final String OFFSET_FILE = "consumer-offsets.properties";
    private static final long FLUSH_INTERVAL_MS = 5000;

    private final FlushingPropertiesStore repository;

    public ConsumerOffsetTracker(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.repository = new FlushingPropertiesStore(
                dataDir,
                OFFSET_FILE,
                "Consumer Offsets",
                FLUSH_INTERVAL_MS
        );

        log.info("ConsumerOffsetTracker initialized");
    }

    /**
     * Initialize - start periodic flush.
     */
    @PostConstruct
    public void init() {
        repository.start();
    }

    /**
     * Get offset for a consumer.
     */
    public long getOffset(String consumerId) {
        String value = repository.get(consumerId, "0");
        return Long.parseLong(value);
    }

    /**
     * Update offset for a consumer.
     */
    public void updateOffset(String consumerId, long offset) {
        repository.put(consumerId, String.valueOf(offset));
    }

    /**
     * Reset offset for a consumer with immediate flush.
     */
    public void resetOffset(String consumerId, long offset) {
        updateOffset(consumerId, offset);
        repository.flush(); // Immediate flush on reset
    }

    /**
     * Shutdown - flush final offsets.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down ConsumerOffsetTracker...");
        repository.stop();
        log.info("ConsumerOffsetTracker shutdown complete");
    }
}
