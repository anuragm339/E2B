package com.messaging.broker.legacy;

import com.messaging.broker.consumer.ConsumerOffsetTracker;
import com.messaging.broker.monitoring.BrokerMetrics;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Value;
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
                                         BrokerMetrics metrics,
                                         @Value("${broker.storage.data-dir:./data}") String dataDir) {
        this.storage = storage;
        this.offsetTracker = offsetTracker;
        this.metrics = metrics;
        this.dataDir = dataDir;
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
        Map<String, String> topicStates = new LinkedHashMap<>();
        Map<String, Long> committedOffsets = new HashMap<>();

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
                    committedOffsets.put(topic, committedOffset);

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
                        topicStates.put(topic, String.format("no_data(committed=%d,earliest=%d,current=%d)",
                                committedOffset, earliestOffset, currentOffset));
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Topic {} - No data available (currentOffset: -1)", topic);
                        }
                        continue;
                    }

                    if (startOffset > currentOffset) {
                        topicStates.put(topic, String.format("caught_up(committed=%d,start=%d,current=%d)",
                                committedOffset, startOffset, currentOffset));
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
                        topicStates.put(topic, String.format("cursor_missing(committed=%d,start=%d,current=%d,earliest=%d)",
                                committedOffset, startOffset, currentOffset, earliestOffset));
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
                        IndexEntry firstEntry = cursor.peek();
                        if (firstEntry != null) {
                            log.info("event=legacy_delivery.cursor_ready group={} topic={} startOffset={} " +
                                     "firstCursorOffset={} committedOffset={} currentOffset={} earliestOffset={}",
                                    consumerGroup, topic, startOffset, firstEntry.offset,
                                    committedOffset, currentOffset, earliestOffset);
                            if (firstEntry.offset < startOffset) {
                                log.warn("event=legacy_delivery.cursor_before_start_offset group={} topic={} " +
                                         "startOffset={} firstCursorOffset={} committedOffset={} currentOffset={} earliestOffset={}",
                                        consumerGroup, topic, startOffset, firstEntry.offset,
                                        committedOffset, currentOffset, earliestOffset);
                            }
                        }
                        cursors.add(cursor);
                        heap.add(cursor);
                        topicStates.put(topic, String.format("eligible(committed=%d,start=%d,current=%d,earliest=%d)",
                                committedOffset, startOffset, currentOffset, earliestOffset));
                        if (debugPriceQuote) {
                            log.debug("[PRICE-QUOTE] Added cursor: topic={}, startOffset={}", topic, startOffset);
                        }
                    } else {
                        topicStates.put(topic, String.format("cursor_exhausted(committed=%d,start=%d,current=%d,earliest=%d)",
                                committedOffset, startOffset, currentOffset, earliestOffset));
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
                log.warn("event=legacy_delivery.empty_batch group={} topicsRequested={} " +
                         "reason=all_cursors_null_or_exhausted topicStates={} — no data will be sent to legacy consumer",
                         consumerGroup, topics.size(), topicStates);
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
                            if (fetched != null && !fetched.isEmpty()) {
                                List<MessageRecord> filtered = fetched.stream()
                                        .filter(record -> record.getOffset() >= nextEntry.offset)
                                        .toList();
                                MessageRecord firstFetched = fetched.get(0);
                                MessageRecord lastFetched = fetched.get(fetched.size() - 1);
                                log.info("event=legacy_delivery.prefetch group={} topic={} requestOffset={} " +
                                         "fetchedCount={} firstFetchedOffset={} lastFetchedOffset={}",
                                        consumerGroup, topic, nextEntry.offset, fetched.size(),
                                        firstFetched.getOffset(), lastFetched.getOffset());
                                if (firstFetched.getOffset() < nextEntry.offset) {
                                    log.warn("event=legacy_delivery.prefetch_before_request group={} topic={} " +
                                             "requestOffset={} firstFetchedOffset={} fetchedCount={}",
                                            consumerGroup, topic, nextEntry.offset,
                                            firstFetched.getOffset(), fetched.size());
                                }
                                if (filtered.isEmpty()) {
                                    log.warn("event=legacy_delivery.prefetch_all_stale group={} topic={} " +
                                             "requestOffset={} fetchedCount={} firstFetchedOffset={} lastFetchedOffset={}",
                                            consumerGroup, topic, nextEntry.offset, fetched.size(),
                                            firstFetched.getOffset(), lastFetched.getOffset());
                                } else {
                                    MessageRecord firstFiltered = filtered.get(0);
                                    MessageRecord lastFiltered = filtered.get(filtered.size() - 1);
                                    if (filtered.size() != fetched.size()) {
                                        log.warn("event=legacy_delivery.prefetch_trimmed_stale group={} topic={} " +
                                                 "requestOffset={} droppedCount={} firstFilteredOffset={} lastFilteredOffset={}",
                                                consumerGroup, topic, nextEntry.offset,
                                                fetched.size() - filtered.size(),
                                                firstFiltered.getOffset(), lastFiltered.getOffset());
                                    }
                                    if (firstFiltered.getOffset() != nextEntry.offset) {
                                        log.warn("event=legacy_delivery.prefetch_first_offset_mismatch group={} topic={} " +
                                                 "requestOffset={} firstFilteredOffset={} filteredCount={}",
                                                consumerGroup, topic, nextEntry.offset,
                                                firstFiltered.getOffset(), filtered.size());
                                    }
                                    buf.addAll(filtered);
                                }
                            }
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
                            MessageRecord candidate = messages.get(0);
                            if (candidate.getOffset() < entry.offset) {
                                log.warn("event=legacy_delivery.single_read_before_request group={} topic={} " +
                                         "requestOffset={} candidateOffset={}",
                                        consumerGroup, topic, entry.offset, candidate.getOffset());
                            } else {
                                msg = candidate;
                            }
                        }
                    }

                    if (msg != null) {
                        if (msg.getOffset() != entry.offset) {
                            log.warn("event=legacy_delivery.cursor_record_mismatch group={} topic={} " +
                                     "cursorOffset={} recordOffset={} committedOffset={} msgKey={}",
                                    consumerGroup, topic, entry.offset, msg.getOffset(),
                                    committedOffsets.getOrDefault(topic, -1L), msg.getMsgKey());
                        }
                        long committedOffset = committedOffsets.getOrDefault(topic, -1L);
                        if (msg.getOffset() <= committedOffset) {
                            log.warn("event=legacy_delivery.skip_stale_record group={} topic={} committedOffset={} candidateOffset={} msgKey={}",
                                    consumerGroup, topic, committedOffset, msg.getOffset(), msg.getMsgKey());
                            continue;
                        }
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

            for (Map.Entry<String, Long> entry : batch.getMaxOffsetPerTopic().entrySet()) {
                String topic = entry.getKey();
                long committedOffset = committedOffsets.getOrDefault(topic, -1L);
                long batchMaxOffset = entry.getValue();
                if (batchMaxOffset <= committedOffset) {
                    log.warn("event=legacy_delivery.non_monotonic_batch group={} topic={} committedOffset={} batchMaxOffset={} topicMessageCount={}",
                            consumerGroup, topic, committedOffset, batchMaxOffset,
                            batch.getMessageCountPerTopic().getOrDefault(topic, 0));
                }
            }

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
     * Handles two edge cases:
     * - Compacted index files (.compacted.index) must be recognized as valid segments.
     * - Gap between compacted segment end and active segment base: if the floor-entry
     *   index yields an immediately-exhausted cursor (no records >= startOffset), fall
     *   back to the next segment (ceiling-entry).
     */
    private TopicCursor createCursor(String topic, long startOffset, boolean debugPriceQuote) throws IOException {
        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] createCursor: topic={}, startOffset={}", topic, startOffset);
        }
        Path indexPath = findIndexPath(topic, startOffset, debugPriceQuote);
        if (indexPath == null) {
            // No segment covers startOffset — either the race window deleted it temporarily,
            // or compaction removed the entire segment (survivorCount==0).
            // Try falling back to the earliest available segment so the consumer can continue
            // rather than stalling permanently when data has been compacted away.
            Path earliestPath = findEarliestIndexPath(topic);
            if (earliestPath != null) {
                long earliestBase = parseIndexBaseOffset(earliestPath.getFileName().toString());
                if (earliestBase > startOffset) {
                    log.warn("event=legacy_delivery.offset_reset_to_earliest " +
                             "topic={} staleOffset={} earliestBase={} " +
                             "reason=segment_compacted_away — consumer skipped ahead to earliest available segment",
                             topic, startOffset, earliestBase);
                    return new TopicCursor(topic, earliestPath, earliestBase);
                }
            }
            log.warn("event=legacy_delivery.topic_dropped topic={} startOffset={} " +
                     "reason=no_index_found — topic will be absent from this merged batch",
                     topic, startOffset);
            return null;
        }

        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] Found index path: {}", indexPath);
        }
        TopicCursor cursor = new TopicCursor(topic, indexPath, startOffset);
        IndexEntry initialEntry = cursor.peek();
        if (initialEntry != null) {
            log.info("event=legacy_delivery.cursor_created topic={} indexFile={} startOffset={} initialOffset={}",
                    topic, indexPath.getFileName(), startOffset, initialEntry.offset);
            if (initialEntry.offset < startOffset) {
                log.warn("event=legacy_delivery.cursor_seek_mismatch topic={} indexFile={} startOffset={} initialOffset={}",
                        topic, indexPath.getFileName(), startOffset, initialEntry.offset);
            }
        } else {
            log.info("event=legacy_delivery.cursor_created_empty topic={} indexFile={} startOffset={}",
                    topic, indexPath.getFileName(), startOffset);
        }

        // Gap fallback: if the floor-entry segment has no records >= startOffset
        // (e.g. startOffset is in the hole between a compacted segment and the active segment),
        // advance to the next segment rather than returning an immediately-exhausted cursor.
        if (!cursor.hasMore()) {
            cursor.close();
            long floorBase = parseIndexBaseOffset(indexPath.getFileName().toString());
            Path nextPath = findNextIndexPath(topic, floorBase, debugPriceQuote);
            if (nextPath == null) {
                if (debugPriceQuote) {
                    log.debug("[PRICE-QUOTE] Gap fallback: no next segment after base={}", floorBase);
                }
                return null;
            }
            if (debugPriceQuote) {
                log.debug("[PRICE-QUOTE] Gap fallback: advancing from base={} to {}", floorBase, nextPath.getFileName());
            }
            cursor = new TopicCursor(topic, nextPath, startOffset);
            IndexEntry nextInitialEntry = cursor.peek();
            if (nextInitialEntry != null) {
                log.info("event=legacy_delivery.cursor_gap_fallback topic={} indexFile={} startOffset={} initialOffset={}",
                        topic, nextPath.getFileName(), startOffset, nextInitialEntry.offset);
                if (nextInitialEntry.offset < startOffset) {
                    log.warn("event=legacy_delivery.cursor_gap_fallback_mismatch topic={} indexFile={} " +
                             "startOffset={} initialOffset={}",
                            topic, nextPath.getFileName(), startOffset, nextInitialEntry.offset);
                }
            } else {
                log.info("event=legacy_delivery.cursor_gap_fallback_empty topic={} indexFile={} startOffset={}",
                        topic, nextPath.getFileName(), startOffset);
            }
        }

        if (debugPriceQuote) {
            log.debug("[PRICE-QUOTE] TopicCursor created for topic: {}", topic);
        }
        return cursor;
    }

    /**
     * Parse the base offset from an index filename.
     * Handles both standard (NNNN.index) and compacted (NNNN.compacted.index) names.
     */
    private long parseIndexBaseOffset(String filename) {
        String offsetStr;
        if (filename.endsWith(".compacted.index")) {
            offsetStr = filename.substring(0, filename.length() - ".compacted.index".length());
        } else {
            offsetStr = filename.substring(0, filename.length() - ".index".length());
        }
        try {
            return Long.parseLong(offsetStr);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Find the index file with the smallest baseOffset in the partition directory.
     * Used as a recovery fallback when the consumer's committed offset falls in a
     * segment that was deleted by compaction (survivorCount==0 path).
     */
    private Path findEarliestIndexPath(String topic) {
        Path topicDir = Paths.get(dataDir, topic, "partition-0");
        java.io.File[] indexFiles = topicDir.toFile().listFiles(
                f -> f.isFile() && f.getName().endsWith(".index"));
        if (indexFiles == null) return null;

        Path bestPath = null;
        long bestBase = Long.MAX_VALUE;
        for (java.io.File f : indexFiles) {
            long base = parseIndexBaseOffset(f.getName());
            if (base >= 0 && base < bestBase) {
                bestBase = base;
                bestPath = f.toPath();
            }
        }
        return bestPath;
    }

    /**
     * Find the index file for the next segment after afterBaseOffset (ceiling-entry).
     * Returns the file with the smallest baseOffset that is strictly > afterBaseOffset.
     */
    private Path findNextIndexPath(String topic, long afterBaseOffset, boolean debugPriceQuote) {
        Path topicDir = Paths.get(dataDir, topic, "partition-0");
        java.io.File[] indexFiles = topicDir.toFile().listFiles(
                f -> f.isFile() && f.getName().endsWith(".index"));
        if (indexFiles == null) return null;

        Path bestPath = null;
        long bestBase = Long.MAX_VALUE;
        for (java.io.File f : indexFiles) {
            long base = parseIndexBaseOffset(f.getName());
            if (base > afterBaseOffset && base < bestBase) {
                bestBase = base;
                bestPath = f.toPath();
            }
        }
        return bestPath;
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
            // Silently skip staging files written by CompactionRewriter mid-run (.compacting.*)
            if (name.contains(".compacting.")) {
                log.debug("event=legacy_delivery.skip_staging_index filename={}", name);
                continue;
            }
            long baseOffset = parseIndexBaseOffset(name);
            if (baseOffset < 0) {
                log.warn("event=legacy_delivery.index_file_skipped filename={} reason=non_standard_name", name);
                continue;
            }
            if (baseOffset <= startOffset && baseOffset > bestBaseOffset) {
                bestBaseOffset = baseOffset;
                bestIndexPath = indexFile.toPath();
            }
        }

        if (bestIndexPath == null) {
            // Build a concise picture of what IS on disk to help diagnose race windows vs deleted segments
            String presentFiles = java.util.Arrays.stream(indexFiles)
                    .map(f -> f.getName() + "(" + formatFileSize(f.length()) + ")")
                    .collect(java.util.stream.Collectors.joining(", "));
            log.warn("event=legacy_delivery.no_covering_index topic={} startOffset={} " +
                     "presentIndexFiles=[{}] — segment may have been deleted by compaction or race window active",
                     topic, startOffset, presentFiles);
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
            long currentOffset = offsetTracker.getOffset(offsetKey);
            if (maxOffset < currentOffset) {
                log.warn("event=legacy_batch_ack.stale_offset_ignored group={} topic={} currentOffset={} ackOffset={}",
                        consumerGroup, topic, currentOffset, maxOffset);
                continue;
            }

            if (maxOffset == currentOffset) {
                log.debug("Legacy batch ACK offset unchanged: topic={}, group={}, offset={}",
                        topic, consumerGroup, maxOffset);
                continue;
            }

            offsetTracker.updateOffset(offsetKey, maxOffset);

            log.debug("Updated offset: topic={}, group={}, offset={}",
                    topic, consumerGroup, maxOffset);
        }
    }
}
