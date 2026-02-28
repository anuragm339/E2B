package com.messaging.broker.legacy;

import com.messaging.broker.consumer.ConsumerOffsetTracker;
import com.messaging.broker.metrics.BrokerMetrics;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.MessageRecord;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Delivery manager for legacy clients.
 * Implements multi-topic streaming merge using Kafka-style k-way merge.
 *
 * Algorithm:
 * 1. Create TopicCursor for each topic subscribed by the consumer
 * 2. Use PriorityQueue (min-heap) to merge by global offset
 * 3. Poll cursor with smallest offset, read message from storage
 * 4. Add to MergedBatch, re-add cursor to heap if it has more
 * 5. Continue until batch size limit reached or all cursors exhausted
 *
 * Complexity:
 * - Time: O(n log k) where n = messages in batch, k = number of topics
 * - Space: O(k) - constant memory regardless of data size
 */
@Singleton
public class LegacyConsumerDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(LegacyConsumerDeliveryManager.class);

    private final StorageEngine storage;
    private final ConsumerOffsetTracker offsetTracker;
    private final BrokerMetrics metrics;
    private final String dataDir;

    @Inject
    public LegacyConsumerDeliveryManager(StorageEngine storage,
                                         ConsumerOffsetTracker offsetTracker,
                                         BrokerMetrics metrics) {
        this.storage = storage;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;

        // Get data directory from system property or environment variable
        this.dataDir = System.getProperty("broker.storage.dataDir",
                System.getenv().getOrDefault("DATA_DIR", "./data"));

        log.info("LegacyConsumerDeliveryManager initialized with dataDir={}", dataDir);
    }

    /**
     * Build a merged batch of messages from multiple topics,
     * sorted by global offset.
     *
     * @param topics List of topics to merge
     * @param consumerGroup Consumer group name (for offset tracking)
     * @param maxBytes Maximum batch size in bytes
     * @return MergedBatch containing messages sorted by global offset
     */
    public MergedBatch buildMergedBatch(List<String> topics,
                                        String consumerGroup,
                                        long maxBytes) throws MessagingException {
        log.debug("Building merged batch: topics={}, group={}, maxBytes={}",
                topics, consumerGroup, maxBytes);

        if (topics == null || topics.isEmpty()) {
            log.warn("No topics provided for merge");
            return new MergedBatch();
        }

        MergedBatch batch = new MergedBatch();
        List<TopicCursor> cursors = new ArrayList<>();

        try {
            // 1. Initialize cursors for each topic
            PriorityQueue<TopicCursor> heap = new PriorityQueue<>(
                    topics.size(),
                    Comparator.comparingLong(cursor -> {
                        try {
                            IndexEntry entry = cursor.peek();
                            return entry != null ? entry.offset : Long.MAX_VALUE;
                        } catch (IOException e) {
                            log.error("Error peeking cursor for topic: {}", cursor.getTopic(), e);
                            return Long.MAX_VALUE;
                        }
                    })
            );

            for (String topic : topics) {
                try {
                    // Get starting offset for this topic
                    long startOffset = offsetTracker.getOffset(consumerGroup + ":" + topic);
                    if (startOffset < 0) {
                        startOffset = storage.getEarliestOffset(topic, 0);
                    } else {
                        startOffset++; // Next offset after last committed
                    }

                    // Create cursor for this topic
                    TopicCursor cursor = createCursor(topic, startOffset);
                    if (cursor != null && cursor.hasMore()) {
                        cursors.add(cursor);
                        heap.add(cursor);
                        log.debug("Added cursor: topic={}, startOffset={}", topic, startOffset);
                    } else {
                        log.debug("No data for topic: {}, startOffset={}", topic, startOffset);
                        if (cursor != null) {
                            cursor.close();
                        }
                    }
                } catch (IOException e) {
                    log.error("Failed to create cursor for topic: {}", topic, e);
                    // Continue with other topics
                }
            }

            if (heap.isEmpty()) {
                log.debug("No cursors available - all topics exhausted");
                return batch;
            }

            // 2. K-way merge using min-heap
            while (!heap.isEmpty() && batch.getTotalBytes() < maxBytes) {
                TopicCursor cursor = heap.poll(); // O(log k)

                try {
                    IndexEntry entry = cursor.advance();
                    if (entry == null) {
                        continue; // Cursor exhausted
                    }

                    // Read actual message data from storage
                    List<MessageRecord> messages = storage.read(cursor.getTopic(), 0, entry.offset, 1);

                    if (messages != null && !messages.isEmpty()) {
                        MessageRecord msg = messages.get(0);
                        batch.add(cursor.getTopic(), msg);

                        log.trace("Merged message: topic={}, offset={}, key={}",
                                cursor.getTopic(), msg.getOffset(), msg.getKey());
                    } else {
                        log.warn("No message found at offset {} for topic {}", entry.offset, cursor.getTopic());
                    }

                    // Re-add cursor to heap if it has more entries
                    if (cursor.hasMore()) {
                        heap.add(cursor); // O(log k)
                    }

                } catch (IOException e) {
                    log.error("Error reading from cursor: topic={}", cursor.getTopic(), e);
                    // Continue with other topics
                }
            }

            log.debug("Merged batch complete: messages={}, bytes={}, topics={}",
                    batch.getMessageCount(), batch.getTotalBytes(), batch.getMaxOffsetPerTopic());

            return batch;

        } finally {
            // 3. Close all cursors
            for (TopicCursor cursor : cursors) {
                try {
                    cursor.close();
                } catch (IOException e) {
                    log.error("Error closing cursor: topic={}", cursor.getTopic(), e);
                }
            }
        }
    }

    /**
     * Create a TopicCursor for reading index entries from a topic.
     */
    private TopicCursor createCursor(String topic, long startOffset) throws IOException {
        // Find the segment containing startOffset
        // For simplicity, we'll use the active segment (partition 0)
        // In production, we'd query SegmentManager for the correct segment

        Path indexPath = findIndexPath(topic, startOffset);
        if (indexPath == null) {
            log.warn("No index file found for topic: {}, startOffset={}", topic, startOffset);
            return null;
        }

        return new TopicCursor(topic, indexPath, startOffset);
    }

    /**
     * Find the index file path for a topic at the given offset.
     * This is a simplified version - in production, we'd use SegmentManager.
     */
    private Path findIndexPath(String topic, long startOffset) {
        // Format: {dataDir}/{topic}/partition-0/00000000000000000000.index
        // For now, assume there's only one segment (baseOffset=0)
        // TODO: Query SegmentManager to find correct segment

        Path topicDir = Paths.get(dataDir, topic, "partition-0");
        Path indexPath = topicDir.resolve("00000000000000000000.index");

        if (!indexPath.toFile().exists()) {
            log.warn("Index file not found: {}", indexPath);
            return null;
        }

        return indexPath;
    }

    /**
     * Handle acknowledgment of a merged batch.
     * Updates offset for each topic that was in the batch.
     */
    public void handleMergedBatchAck(String consumerGroup, MergedBatch batch) {
        log.debug("Handling merged batch ACK: group={}, topics={}",
                consumerGroup, batch.getMaxOffsetPerTopic().keySet());

        // Update offset for EACH topic that was in the batch
        for (Map.Entry<String, Long> entry : batch.getMaxOffsetPerTopic().entrySet()) {
            String topic = entry.getKey();
            long maxOffset = entry.getValue();

            String offsetKey = consumerGroup + ":" + topic;
            offsetTracker.updateOffset(offsetKey, maxOffset);

            log.debug("Updated offset: topic={}, group={}, offset={}",
                    topic, consumerGroup, maxOffset);
        }
    }
}
