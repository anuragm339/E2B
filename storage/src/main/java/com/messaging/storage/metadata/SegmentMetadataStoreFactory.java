package com.messaging.storage.metadata;

import com.messaging.common.exception.StorageException;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PreDestroy;
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
            try {
                Path topicDir = dataDir.resolve(topic);
                SegmentMetadataStore store = new SegmentMetadataStore(topicDir);
                log.debug("Created SegmentMetadataStore for topic: {}", topic);
                return store;
            } catch (StorageException e) {
                // Wrap in RuntimeException for computeIfAbsent
                throw new RuntimeException("Failed to create SegmentMetadataStore for topic: " + topic, e);
            }
        });
    }

    /**
     * Close all metadata stores on graceful shutdown.
     * B8-1 fix: @PreDestroy ensures SQLite connections are properly closed so WAL journals
     * are checkpointed and dirty pages flushed before JVM exit. Without this, the JVM
     * relied on GC finalizers which are not guaranteed to run before process termination.
     */
    @PreDestroy
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
