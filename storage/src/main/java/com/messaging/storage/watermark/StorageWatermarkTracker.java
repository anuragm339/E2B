package com.messaging.storage.watermark;

import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks monotonic watermarks for (topic, partition) pairs.
 * Updated atomically by Storage after successful append.
 * Read by Broker to check for new data without disk I/O.
 *
 * This is the KEY component enabling adaptive polling:
 * - Pipe updates watermark after storage.append()
 * - Broker checks watermark before reading storage
 * - Near-zero cost for idle topics (<1μs in-memory read)
 */
@Singleton
public class StorageWatermarkTracker {
    private static final Logger log = LoggerFactory.getLogger(StorageWatermarkTracker.class);

    // Map: "topic:partition" → latest committed offset
    private final ConcurrentHashMap<String, AtomicLong> watermarks;

    public StorageWatermarkTracker() {
        this.watermarks = new ConcurrentHashMap<>();
        log.info("StorageWatermarkTracker initialized");
    }

    /**
     * Update watermark after successful append
     * Called by Pipe/Storage layer
     *
     * @param topic Topic name
     * @param partition Partition number
     * @param offset The offset that was just appended
     */
    public void updateWatermark(String topic, int partition, long offset) {
        String key = toKey(topic, partition);
        watermarks.computeIfAbsent(key, k -> new AtomicLong(0))
                  .updateAndGet(current -> Math.max(current, offset));

        log.trace("Watermark updated: {}={}", key, offset);
    }

    /**
     * Get current watermark for topic-partition
     * Called by Broker to check if new data available
     *
     * @param topic Topic name
     * @param partition Partition number
     * @return Current watermark offset (0 if no data)
     */
    public long getWatermark(String topic, int partition) {
        String key = toKey(topic, partition);
        return watermarks.getOrDefault(key, new AtomicLong(0)).get();
    }

    /**
     * Check if new data available since last offset
     * This is the critical check that enables adaptive polling:
     * - Returns true → Broker reads storage
     * - Returns false → Broker skips read, exponential backoff
     *
     * @param topic Topic name
     * @param partition Partition number
     * @param lastDeliveredOffset Last offset delivered to consumer
     * @return true if watermark > lastDeliveredOffset
     */
    public boolean hasNewData(String topic, int partition, long lastDeliveredOffset) {
        long watermark = getWatermark(topic, partition);
        boolean hasData = watermark > lastDeliveredOffset;

        if (log.isTraceEnabled()) {
            log.trace("Watermark check: {}:{}  watermark={}, lastOffset={}, hasData={}",
                     topic, partition, watermark, lastDeliveredOffset, hasData);
        }

        return hasData;
    }

    /**
     * Get watermark keys for monitoring/debugging
     */
    public int getWatermarkCount() {
        return watermarks.size();
    }

    /**
     * Create composite key from topic and partition
     */
    private String toKey(String topic, int partition) {
        return topic + ":" + partition;
    }
}
