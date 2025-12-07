package com.messaging.storage.mmap;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.segment.SegmentManager;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
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
 * Memory-mapped file based storage engine implementation
 */
@Singleton
@Requires(property = "broker.storage.type", value = "mmap")
public class MMapStorageEngine implements StorageEngine {
    private static final Logger log = LoggerFactory.getLogger(MMapStorageEngine.class);

    private final Path dataDir;
    private final long maxSegmentSize;
    private final Map<TopicPartition, SegmentManager> managers;

    public MMapStorageEngine(
            @Value("${broker.storage.data-dir:/data}") String dataDir,
            @Value("${broker.storage.segment-size:1073741824}") long maxSegmentSize) {

        this.dataDir = Paths.get(dataDir);
        this.maxSegmentSize = maxSegmentSize;
        this.managers = new ConcurrentHashMap<>();

        log.info("Initialized MMapStorageEngine: dataDir={}, maxSegmentSize={}",
                this.dataDir, this.maxSegmentSize);
    }

    @Override
    public long append(String topic, int partition, MessageRecord record) {
        try {
            SegmentManager manager = getOrCreateManager(topic, partition);
            return manager.append(record);
        } catch (IOException e) {
            log.error("Failed to append record: topic={}, partition={}", topic, partition, e);
            throw new RuntimeException("Failed to append record", e);
        }
    }

    @Override
    public List<MessageRecord> read(String topic, int partition, long fromOffset, int maxRecords) {
        try {
            SegmentManager manager = managers.get(new TopicPartition(topic, partition));
            if (manager == null) {
                return new ArrayList<>();
            }

            return manager.read(fromOffset, maxRecords);
        } catch (IOException e) {
            log.error("Failed to read records: topic={}, partition={}, offset={}",
                    topic, partition, fromOffset, e);
            throw new RuntimeException("Failed to read records", e);
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
    public void recover() {
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
                        } catch (IOException e) {
                            log.error("Failed to list partitions for topic: {}", topicName, e);
                        }
                    });

            log.info("Recovery completed. Loaded {} topic-partitions", managers.size());

        } catch (IOException e) {
            log.error("Failed to recover from data directory", e);
            throw new RuntimeException("Recovery failed", e);
        }
    }

    @Override
    public void close() {
        log.info("Closing storage engine, flushing all data...");

        for (Map.Entry<TopicPartition, SegmentManager> entry : managers.entrySet()) {
            try {
                entry.getValue().close();
                log.info("Closed segment manager for {}", entry.getKey());
            } catch (IOException e) {
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

                return new SegmentManager(topic, partition, partitionDir, maxSegmentSize);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create segment manager", e);
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
