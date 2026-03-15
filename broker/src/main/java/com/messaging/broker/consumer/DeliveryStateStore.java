package com.messaging.broker.consumer;

import com.messaging.broker.consumer.FlushingPropertiesStore;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persists delivery state for a subscription delivery key.
 *
 * Enables safe broker restart without duplicate delivery.
 *
 * State format in properties file:
 * - {deliveryKey}.offset = last acknowledged offset
 * - {deliveryKey}.until = timestamp when in-flight expires
 *
 * Where deliveryKey = "group:topic".
 */
@Singleton
public class DeliveryStateStore {
    private static final Logger log = LoggerFactory.getLogger(DeliveryStateStore.class);

    private static final String STATE_FILE = "delivery-state.properties";
    private static final long FLUSH_INTERVAL_MS = 5000;

    private final FlushingPropertiesStore repository;
    private final Map<String, DeliveryState> cache;

    public DeliveryStateStore(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.repository = new FlushingPropertiesStore(
                dataDir,
                STATE_FILE,
                "Delivery State",
                FLUSH_INTERVAL_MS
        );
        this.cache = new ConcurrentHashMap<>();

        loadFromRepository();
        log.info("DeliveryStateStore initialized: loaded {} entries", cache.size());
    }

    /**
     * Start periodic flush to disk.
     */
    @PostConstruct
    public void init() {
        repository.start();
        log.info("DeliveryStateStore started with {}ms flush interval", FLUSH_INTERVAL_MS);
    }

    /**
     * Get delivery state for a subscription delivery key.
     *
     * @param deliveryKey Format: "group:topic"
     * @return DeliveryState (never null, returns zero state if not found)
     */
    public DeliveryState getState(String deliveryKey) {
        return cache.getOrDefault(deliveryKey, new DeliveryState(0, 0));
    }

    /**
     * Save delivery state for a subscription delivery key.
     * State is updated in memory immediately and flushed to disk periodically.
     *
     * @param deliveryKey Format: "group:topic"
     * @param lastAckedOffset Last offset that was acknowledged
     * @param inFlightUntil Timestamp when in-flight status expires
     */
    public void saveState(String deliveryKey, long lastAckedOffset, long inFlightUntil) {
        DeliveryState state = new DeliveryState(lastAckedOffset, inFlightUntil);
        cache.put(deliveryKey, state);

        // Persist to repository (periodic flush handles disk I/O)
        repository.put(deliveryKey + ".offset", String.valueOf(lastAckedOffset));
        repository.put(deliveryKey + ".until", String.valueOf(inFlightUntil));

        log.trace("Saved delivery state: {}={}", deliveryKey, state);
    }

    /**
     * Update only the acknowledged offset (keep existing inFlightUntil)
     *
     * @param deliveryKey Format: "group:topic"
     * @param lastAckedOffset New acknowledged offset
     */
    public void updateAckedOffset(String deliveryKey, long lastAckedOffset) {
        DeliveryState current = getState(deliveryKey);
        saveState(deliveryKey, lastAckedOffset, current.inFlightUntil);
    }

    /**
     * Update only the in-flight expiration (keep existing lastAckedOffset)
     *
     * @param deliveryKey Format: "group:topic"
     * @param inFlightUntil Timestamp when in-flight expires
     */
    public void updateInFlightUntil(String deliveryKey, long inFlightUntil) {
        DeliveryState current = getState(deliveryKey);
        saveState(deliveryKey, current.lastAckedOffset, inFlightUntil);
    }

    /**
     * Immediate flush to disk (used during shutdown or critical operations).
     */
    public void flush() {
        repository.flush();
    }

    /**
     * Check if delivery is currently in-flight.
     *
     * @param deliveryKey Format: "group:topic"
     * @return true if in-flight and not expired
     */
    public boolean isInFlight(String deliveryKey) {
        DeliveryState state = getState(deliveryKey);
        return state.inFlightUntil > System.currentTimeMillis();
    }

    /**
     * Clear in-flight status (set expiration to 0).
     *
     * @param deliveryKey Format: "group:topic"
     */
    public void clearInFlight(String deliveryKey) {
        DeliveryState current = getState(deliveryKey);
        saveState(deliveryKey, current.lastAckedOffset, 0);
    }

    /**
     * Load state from repository.
     */
    private void loadFromRepository() {
        Map<String, String> allProperties = repository.getAll();

        // Parse properties back into cache
        allProperties.keySet().stream()
            .filter(k -> k.endsWith(".offset"))
            .forEach(k -> {
                String key = k.replace(".offset", "");
                long offset = Long.parseLong(allProperties.get(k));
                long until = Long.parseLong(allProperties.getOrDefault(key + ".until", "0"));
                cache.put(key, new DeliveryState(offset, until));
            });

        log.info("Loaded {} delivery states from repository", cache.size());
    }

    /**
     * Get all tracked delivery keys.
     */
    public int getTrackedCount() {
        return cache.size();
    }

    /**
     * Remove all delivery state entries associated with a disconnected client.
     *
     * This cleanup is conservative: delivery state is persisted by subscription
     * key, but older entries may still be tied to client-specific prefixes.
     *
     * @param clientId Client identifier (includes ephemeral port)
     */
    public void removeConsumerState(String clientId) {
        String keyPrefix = clientId + ":";
        int removedCount = 0;

        // Remove all entries matching clientId prefix (all topics for this client)
        for (String key : cache.keySet()) {
            if (key.startsWith(keyPrefix)) {
                if (cache.remove(key) != null) {
                    // Remove both .offset and .until keys from repository
                    repository.remove(key + ".offset");
                    repository.remove(key + ".until");
                    removedCount++;
                }
            }
        }

        if (removedCount > 0) {
            log.info("Removed {} delivery state entries for disconnected clientId={}",
                    removedCount, clientId);
            // Trigger immediate flush to persist cleanup
            repository.flush();
        }
    }

    /**
     * Shutdown - stop repository and perform final flush.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down DeliveryStateStore...");
        repository.stop();
        log.info("DeliveryStateStore shutdown complete");
    }

    /**
     * Delivery state for a subscription delivery key.
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
