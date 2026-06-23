package com.messaging.broker.consumer;

import com.messaging.broker.consumer.FlushingPropertiesStore;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks and persists consumer offsets to disk.
 */
@Singleton
public class ConsumerOffsetTracker {
    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetTracker.class);
    private static final String OFFSET_FILE = "consumer-offsets.properties";
    private static final long FLUSH_INTERVAL_MS = 5000;
    // Minimum gap between synchronous flushes triggered by updateOffset(). Each flush
    // rewrites the whole properties file — doing that per ACK hammers flash storage.
    private static final long MIN_SYNC_FLUSH_GAP_MS = 1000;

    private final FlushingPropertiesStore repository;
    private final java.util.concurrent.atomic.AtomicLong lastSyncFlushMs =
            new java.util.concurrent.atomic.AtomicLong(0);

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
     * Get the committed offset for a consumer, or -1 when nothing has ever been committed.
     *
     * Unlike {@link #getOffset}, this distinguishes "no commit yet" from "committed through
     * offset 0" — callers that must decide between "start from earliest" and "resume after
     * the committed offset" need that distinction (a default of 0 silently makes the record
     * at offset 0 undeliverable).
     */
    public long getCommittedOffset(String consumerId) {
        String value = repository.get(consumerId);
        return value != null ? Long.parseLong(value) : -1L;
    }

    /**
     * Update offset for a consumer and flush to disk, rate-limited to one synchronous
     * flush per {@link #MIN_SYNC_FLUSH_GAP_MS}.
     *
     * The synchronous flush exists to bound offset loss on unexpected JVM exit (the
     * original EMFILE incident left in-memory offsets unflushed and consumers replayed
     * from 0). Flushing on EVERY ACK, however, rewrites the whole properties file per
     * batch — needless flash wear and IO on POS hardware. The rate limit bounds loss to
     * ~1s of ACK progress (offsets are at-least-once safe to lose); the 5s background
     * flusher and the final flush on stop cover the remainder.
     */
    public void updateOffset(String consumerId, long offset) {
        repository.put(consumerId, String.valueOf(offset));
        long now = System.currentTimeMillis();
        long last = lastSyncFlushMs.get();
        if (now - last >= MIN_SYNC_FLUSH_GAP_MS && lastSyncFlushMs.compareAndSet(last, now)) {
            repository.flush();
        }
    }

    /**
     * Return all committed offsets keyed by "group:topic".
     */
    public Map<String, Long> getAllOffsets() {
        Map<String, String> raw = repository.getAll();
        Map<String, Long> result = new HashMap<>(raw.size());
        for (Map.Entry<String, String> e : raw.entrySet()) {
            try {
                result.put(e.getKey(), Long.parseLong(e.getValue()));
            } catch (NumberFormatException ignored) {
                // skip malformed entries
            }
        }
        return result;
    }

    /**
     * Reset offset for a consumer with immediate flush.
     */
    public void resetOffset(String consumerId, long offset) {
        updateOffset(consumerId, offset);
        repository.flush(); // Immediate flush on reset
    }

    /**
     * Quiesce offset tracking for a destructive download-refresh wipe: cancel the periodic flush,
     * drop in-memory offsets, and reject further updates so a stray ACK cannot re-create
     * {@code consumer-offsets.properties} after {@code clearState} deletes it.
     */
    public void quiesceForWipe() {
        repository.pauseForWipe();
    }

    /** Resume after the wipe, reloading offsets from the (now wiped/restored) file on disk. */
    public void resumeAfterWipe() {
        repository.resumeAfterWipe();
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
