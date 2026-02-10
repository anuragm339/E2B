package com.messaging.storage.metadata;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating and caching SegmentMetadataStore instances per topic
 * Each topic gets its own metadata database for isolation
 */
@Singleton
public class SegmentMetadataStoreFactory {
    private static final Logger log = LoggerFactory.getLogger(SegmentMetadataStoreFactory.class);

    private final Path dataDir;
    private final Map<String, SegmentMetadataStore> stores;

    public SegmentMetadataStoreFactory(@Value("${broker.storage.data-dir:/data}") String dataDir) {
        this.dataDir = Paths.get(dataDir);
        this.stores = new ConcurrentHashMap<>();
        log.info("SegmentMetadataStoreFactory initialized: dataDir={}", this.dataDir);
    }

    /**
     * Get or create a SegmentMetadataStore for a specific topic
     * Each topic has its own metadata database at: {dataDir}/{topic}/segment_metadata.db
     *
     * @param topic Topic name
     * @return SegmentMetadataStore for this topic
     */
    public SegmentMetadataStore getStoreForTopic(String topic) {
        return stores.computeIfAbsent(topic, t -> {
            Path topicDir = dataDir.resolve(topic);
            SegmentMetadataStore store = new SegmentMetadataStore(topicDir);
            log.debug("Created SegmentMetadataStore for topic: {}", topic);
            return store;
        });
    }

    /**
     * Close all metadata stores (called during shutdown)
     */
    public void closeAll() {
        stores.forEach((topic, store) -> {
            try {
                store.close();
                log.debug("Closed SegmentMetadataStore for topic: {}", topic);
            } catch (Exception e) {
                log.error("Failed to close SegmentMetadataStore for topic: {}", topic, e);
            }
        });
        stores.clear();
        log.info("All SegmentMetadataStores closed");
    }
}
