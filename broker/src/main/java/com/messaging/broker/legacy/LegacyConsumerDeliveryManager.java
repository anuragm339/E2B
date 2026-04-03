package com.messaging.broker.legacy;

import com.messaging.broker.consumer.ConsumerOffsetTracker;
import com.messaging.broker.monitoring.BrokerMetrics;
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
 * 3. Poll cursor with smallest offset, read messages from storage in chunks
 * 4. Add to MergedBatch, re-add cursor to heap if it has more
 * 5. Continue until batch size limit reached or all cursors exhausted
 *
 * Complexity:
 * - Time: O(n log k) where n = messages in batch, k = number of topics
 * - Space: O(k * MSG_CHUNK) — bounded pre-fetch per topic
 * - storage.read() calls: O(n / MSG_CHUNK) instead of O(n)
 */
@Singleton
public class LegacyConsumerDeliveryManager {
    private static final Logger log = LoggerFactory.getLogger(LegacyConsumerDeliveryManager.class);

    // Pre-fetch chunk size for storage.read() — reduces calls from O(n) to O(n/50)
    private static final int MSG_CHUNK = 50;

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

        log.info("event=legacy_delivery.initialized dataDir={}", dataDir);
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
        // Enable detailed logging only for price-quote consumer group
        boolean debugPriceQuote = consumerGroup != null && consumerGroup.contains("price-quote");

        log.debug("Building merged batch: topics={}, group={}, maxBytes={}",
                topics, consumerGroup, maxBytes);

        if (topics == null || topics.isEmpty()) {
            log.warn("event=legacy_delivery.merge_skipped reason=no_topics");
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
                    long committedOffset = offsetTracker.getOffset(consumerGroup + ":" + topic);
                    long earliestOffset = storage.getEarliestOffset(topic, 0);
                    long currentOffset = storage.getCurrentOffset(topic, 0);

                    long startOffset;
                    if (committedOffset < 0) {
                        startOffset = earliestOffset;
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - No committed offset, starting from earliest: {} (current: {})",
                                    topic, startOffset, currentOffset);
                        }
                    } else {
                        startOffset = committedOffset + 1; // Next offset after last committed
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - Committed offset: {}, starting from: {} (earliest: {}, current: {})",
                                    topic, committedOffset, startOffset, earliestOffset, currentOffset);
                        }
                    }

                    // Validate offset range
                    if (currentOffset < 0) {
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - No data available (currentOffset: -1)", topic);
                        }
                        continue;
                    }

                    if (startOffset > currentOffset) {
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - startOffset ({}) beyond currentOffset ({}) - no new data",
                                    topic, startOffset, currentOffset);
                        }
                        continue;
                    }

                    // Create cursor for this topic
                    if (debugPriceQuote) {
                        log.debug("Creating cursor for topic {} from offset {}", topic, startOffset);
                    }
                    TopicCursor cursor = createCursor(topic, startOffset, debugPriceQuote);

                    if (cursor == null) {
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - createCursor returned NULL (startOffset: {})", topic, startOffset);
                        }
                        continue;
                    }

                    boolean hasMore = cursor.hasMore();
                    if (debugPriceQuote) {
                        log.debug("Topic {} - cursor.hasMore() = {}", topic, hasMore);
                    }

                    if (hasMore) {
                        cursors.add(cursor);
                        heap.add(cursor);
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Added cursor: topic={}, startOffset={}", topic, startOffset);
                        }
                    } else {
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - cursor.hasMore() returned false (startOffset: {}, current: {})",
                                    topic, startOffset, currentOffset);
                        }
                        cursor.close();
                    }
                } catch (IOException e) {
                    log.error("❌ Failed to create cursor for topic: {}", topic, e);
                    // Continue with other topics
                }
            }

            if (heap.isEmpty()) {
                if (debugPriceQuote) {
                    log.debug("[PRICE-QUOTE] EMPTY HEAP - No cursors available after processing {} topics (group: {}). " +
                              "Check if: (1) startOffset > currentOffset, (2) createCursor() failed, " +
                              "(3) cursor.hasMore() returned false",
                              topics.size(), consumerGroup);
                } else {
                    log.debug("No cursors available - all topics exhausted");
                }
                return batch;
            }

            if (debugPriceQuote) {
                log.debug("[PRICE-QUOTE] K-way merge starting with {} cursors from {} topics", heap.size(), topics.size());
            }

            // 2. K-way merge using min-heap with per-topic message pre-fetch buffers
            Map<String, ArrayDeque<MessageRecord>> msgBuffers = new HashMap<>();

            while (!heap.isEmpty() && batch.getTotalBytes() < maxBytes) {
                TopicCursor cursor = heap.poll(); // O(log k)
                String topic = cursor.getTopic();
                ArrayDeque<MessageRecord> buf =
                        msgBuffers.computeIfAbsent(topic, t -> new ArrayDeque<>());

                try {
                    // Refill message buffer if empty: one storage.read() for MSG_CHUNK messages
                    if (buf.isEmpty()) {
                        IndexEntry nextEntry = cursor.peek(); // does NOT advance cursor
                        if (nextEntry != null) {
                            List<MessageRecord> fetched =
                                    storage.read(topic, 0, nextEntry.offset, MSG_CHUNK);
                            if (fetched != null) buf.addAll(fetched);
                        }
                    }

                    // Advance cursor index (always paired with buf.poll() below)
                    IndexEntry entry = cursor.advance();
                    if (entry == null) {
                        continue; // Cursor exhausted
                    }

                    MessageRecord msg = buf.poll(); // paired with cursor.advance()
                    if (msg == null) {
                        // Fallback: gap or pre-fetch mismatch — single direct read
                        List<MessageRecord> messages = storage.read(topic, 0, entry.offset, 1);
                        if (messages != null && !messages.isEmpty()) {
                            msg = messages.get(0);
                        }
                    }

                    if (msg != null) {
                        batch.add(topic, msg);
                        log.trace("Merged message: topic={}, offset={}, key={}",
                                topic, msg.getOffset(), msg.getMsgKey());
                    } else {
                        log.debug("No message found at offset {} for topic {}", entry.offset, topic);
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
    private TopicCursor createCursor(String topic, long startOffset, boolean debugPriceQuote) throws IOException {
        // Find the segment containing startOffset
        // For simplicity, we'll use the active segment (partition 0)
        // In production, we'd query SegmentManager for the correct segment

        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] createCursor: topic={}, startOffset={}", topic, startOffset);
        }
        Path indexPath = findIndexPath(topic, startOffset, debugPriceQuote);
        if (indexPath == null) {
            if (debugPriceQuote) {
                log.debug("[PRICE-QUOTE] No index file found for topic: {}, startOffset={}", topic, startOffset);
            }
            return null;
        }

        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] Found index path: {}", indexPath);
        }
        TopicCursor cursor = new TopicCursor(topic, indexPath, startOffset);
        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] TopicCursor created for topic: {}", topic);
        }
        return cursor;
    }

    /**
     * Find the index file path for a topic at the given offset.
     *
     * Scans all *.index files in the partition directory, parses the base offset
     * from each filename ({20-digit-zero-padded-offset}.index), and returns the
     * file with the highest base offset that is still ≤ startOffset — i.e. the
     * segment that contains startOffset.
     */
    private Path findIndexPath(String topic, long startOffset, boolean debugPriceQuote) {
        Path topicDir = Paths.get(dataDir, topic, "partition-0");
        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] Looking for index in directory: {}", topicDir);
        }

        if (!topicDir.toFile().exists()) {
            if (debugPriceQuote) {
                log.debug("[PRICE-QUOTE] Topic directory does not exist: {}", topicDir);
            }
            return null;
        }

        java.io.File[] indexFiles = topicDir.toFile().listFiles(
                f -> f.isFile() && f.getName().endsWith(".index"));

        if (indexFiles == null || indexFiles.length == 0) {
            if (debugPriceQuote) {
                log.debug("[PRICE-QUOTE] No .index files found in: {}", topicDir);
            }
            return null;
        }

        if (debugPriceQuote) {
            String fileDetails = java.util.Arrays.stream(indexFiles)
                    .map(f -> String.format("%s (%s)", f.getName(), formatFileSize(f.length())))
                    .collect(java.util.stream.Collectors.joining(", "));
            log.debug("[PRICE-QUOTE] Index files in {}: {}", topicDir, fileDetails);
        }

        // Find the segment with the highest base offset that is <= startOffset
        Path bestIndexPath = null;
        long bestBaseOffset = -1;

        for (java.io.File indexFile : indexFiles) {
            String name = indexFile.getName();
            // Strip ".index" suffix to get the zero-padded base-offset string
            String offsetStr = name.substring(0, name.length() - ".index".length());
            try {
                long baseOffset = Long.parseLong(offsetStr);
                if (baseOffset <= startOffset && baseOffset > bestBaseOffset) {
                    bestBaseOffset = baseOffset;
                    bestIndexPath = indexFile.toPath();
                }
            } catch (NumberFormatException e) {
                log.warn("event=legacy_delivery.index_file_skipped filename={} reason=non_standard_name", name);
            }
        }

        if (bestIndexPath == null) {
            if (debugPriceQuote) {
                log.debug("[PRICE-QUOTE] No index file covers startOffset={} in {}", startOffset, topicDir);
            }
            return null;
        }

        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] Selected index: {} (baseOffset={}) for startOffset={}",
                    bestIndexPath.getFileName(), bestBaseOffset, startOffset);
        }
        return bestIndexPath;
    }

    /**
     * Format file size in human-readable format
     */
    private String formatFileSize(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1fKB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1fMB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
        }
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
