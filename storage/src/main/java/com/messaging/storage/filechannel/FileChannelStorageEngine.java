package com.messaging.storage.filechannel;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.metadata.SegmentMetadataStore;
import com.messaging.storage.segment.SegmentManager;
import com.messaging.storage.watermark.StorageWatermarkTracker;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FileChannel-based storage engine implementation
 * Uses FileChannel for direct I/O without memory mapping
 * Enables true zero-copy via Netty FileRegion
 */
@Singleton
@Requires(property = "broker.storage.type", value = "filechannel")
public class FileChannelStorageEngine implements StorageEngine {
    private static final Logger log = LoggerFactory.getLogger(FileChannelStorageEngine.class);

    private final Path dataDir;
    private final long maxSegmentSize;
    private final Map<TopicPartition, SegmentManager> managers;
    private final StorageWatermarkTracker watermarkTracker;
    private final com.messaging.storage.metadata.SegmentMetadataStoreFactory metadataStoreFactory;

    @Inject
    public FileChannelStorageEngine(
            @Value("${broker.storage.data-dir:/data}") String dataDir,
            @Value("${broker.storage.segment-size:1073741824}") long maxSegmentSize,
            StorageWatermarkTracker watermarkTracker,
            com.messaging.storage.metadata.SegmentMetadataStoreFactory metadataStoreFactory) {

        this.dataDir = Paths.get(dataDir);
        this.maxSegmentSize = maxSegmentSize;
        this.watermarkTracker = watermarkTracker;
        this.metadataStoreFactory = metadataStoreFactory;
        this.managers = new ConcurrentHashMap<>();

        log.info("Initialized FileChannelStorageEngine: dataDir={}, maxSegmentSize={}, watermarkTracker={}, metadataStoreFactory={}",
                this.dataDir, this.maxSegmentSize, watermarkTracker != null, metadataStoreFactory != null);
    }

    @Override
    public long append(String topic, int partition, MessageRecord record) throws MessagingException {
        SegmentManager manager = getOrCreateManager(topic, partition);
        long offset = manager.append(record);

        // Update watermark atomically after successful append
        // This enables Broker to check for new data without disk I/O
        watermarkTracker.updateWatermark(topic, partition, offset);

        return offset;
    }

    @Override
    public List<MessageRecord> read(String topic, int partition, long fromOffset, int maxRecords) throws MessagingException {
        SegmentManager manager = managers.get(new TopicPartition(topic, partition));
        if (manager == null) {
            return new ArrayList<>();
        }
        List<MessageRecord> read = manager.read(fromOffset, maxRecords);
        return read;
    }

    /**
     * Zero-copy batch read: Get FileRegion for direct file-to-network transfer
     * This enables Kafka-style zero-copy using sendfile() syscall
     * @return BatchFileRegion containing FileRegion for zero-copy transfer + metadata
     */
    public com.messaging.storage.segment.Segment.BatchFileRegion getZeroCopyBatch(
            String topic, int partition, long fromOffset, long maxBytes) throws MessagingException{
        try {
            SegmentManager manager = managers.get(new TopicPartition(topic, partition));
            if (manager == null) {
                log.debug("No segment manager for topic={}, partition={}", topic, partition);
                return new com.messaging.storage.segment.Segment.BatchFileRegion(null, null, 0, 0, 0, fromOffset);
            }
            return manager.getZeroCopyBatch(fromOffset, maxBytes);
        } catch (Exception e) {
            throw ExceptionLogger.logAndThrow(log, new StorageException(ErrorCode.STORAGE_READ_FAILED, String.format("Failed to get zero-copy batch: topic=%s partition=%d fromOffset=%d maxBytes=%d", topic, partition, fromOffset, maxBytes), e));
        }
    }

    @Override
    public long getCurrentOffset(String topic, int partition) {
        SegmentManager manager = managers.get(new TopicPartition(topic, partition));
        if (manager == null) {
            return -1;
        }

        return manager.getCurrentOffset();
    }

    @Override
    public long getMaxOffsetFromMetadata(String topic, int partition) {
        SegmentManager manager = managers.get(new TopicPartition(topic, partition));
        if (manager == null) {
            return -1;
        }

        return manager.getMaxOffsetFromMetadata();
    }

    @Override
    public long getEarliestOffset(String topic, int partition) {
        SegmentManager manager = managers.get(new TopicPartition(topic, partition));
        if (manager == null) {
            return 0;  // No segments exist, start from 0
        }

        return manager.getEarliestOffset();
    }

    @Override
    public void compact(String topic, int partition) {
        // Compaction will be implemented in CompactionEngine
        log.info("Compaction requested for topic={}, partition={}", topic, partition);
        // TODO: Trigger compaction
    }

    @Override
    public ValidationResult validate(String topic, int partition) {
        // Validation will be implemented in ValidationService
        log.info("Validation requested for topic={}, partition={}", topic, partition);

        List<String> errors = new ArrayList<>();
        // TODO: Implement validation logic

        return new ValidationResult(errors.isEmpty(), errors);
    }

    @Override
    public void recover() throws MessagingException {
        try {
            log.info("Starting recovery from data directory: {}", dataDir);

            if (!Files.exists(dataDir)) {
                Files.createDirectories(dataDir);
                log.info("Created data directory: {}", dataDir);
                return;
            }

            // Discover all topic-partition directories
            Files.list(dataDir)
                    .filter(Files::isDirectory)
                    .forEach(topicDir -> {
                        String topicName = topicDir.getFileName().toString();

                        try {
                            Files.list(topicDir)
                                    .filter(Files::isDirectory)
                                    .filter(p -> p.getFileName().toString().startsWith("partition-"))
                                    .forEach(partitionDir -> {
                                        try {
                                            String partitionStr = partitionDir.getFileName().toString()
                                                    .replace("partition-", "");
                                            int partition = Integer.parseInt(partitionStr);

                                            log.info("Recovering topic={}, partition={}", topicName, partition);
                                            getOrCreateManager(topicName, partition);

                                        } catch (Exception e) {
                                            log.error("Failed to recover partition: {}", partitionDir, e);
                                        }
                                    });
                        } catch (Exception e) {
                            log.error("Failed to recover topic: {}", topicName, e);
                        }
                    });

            log.info("Recovery completed. Loaded {} topic-partitions", managers.size());

        } catch (IOException e) {
            throw ExceptionLogger.logAndThrow(log, new StorageException(ErrorCode.STORAGE_RECOVERY_FAILED, "Failed to recover storage engine", e));
        }
    }

    @Override
    public void close() {
        log.info("Closing storage engine, flushing all data...");

        for (Map.Entry<TopicPartition, SegmentManager> entry : managers.entrySet()) {
            try {
                entry.getValue().close();
                log.info("Closed segment manager for {}", entry.getKey());
            } catch (MessagingException e) {
                log.error("Failed to close segment manager for {}", entry.getKey(), e);
            }
        }

        managers.clear();
        log.info("Storage engine closed");
    }

    /**
     * Get or create segment manager for topic-partition
     */
    private SegmentManager getOrCreateManager(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);

        return managers.computeIfAbsent(tp, key -> {
            try {
                Path partitionDir = dataDir
                        .resolve(topic)
                        .resolve("partition-" + partition);

                // Get topic-specific metadata store from factory
                SegmentMetadataStore metadataStore = metadataStoreFactory.getStoreForTopic(topic);
                return new SegmentManager(topic, partition, partitionDir, maxSegmentSize, metadataStore);
            } catch (StorageException e) {
                throw new RuntimeException(e); // Wrap for computeIfAbsent
            }
        });
    }

    /**
     * Topic-Partition key
     */
    private static class TopicPartition {
        private final String topic;
        private final int partition;

        public TopicPartition(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicPartition that = (TopicPartition) o;
            return partition == that.partition && topic.equals(that.topic);
        }

        @Override
        public int hashCode() {
            return 31 * topic.hashCode() + partition;
        }

        @Override
        public String toString() {
            return topic + "-" + partition;
        }
    }
}
