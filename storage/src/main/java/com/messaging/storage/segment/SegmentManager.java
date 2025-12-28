package com.messaging.storage.segment;

import com.messaging.common.model.MessageRecord;
import com.messaging.storage.metadata.SegmentMetadata;
import com.messaging.storage.metadata.SegmentMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Manages multiple segments for a topic-partition
 */
public class SegmentManager {
    private static final Logger log = LoggerFactory.getLogger(SegmentManager.class);
    private static final Pattern SEGMENT_PATTERN = Pattern.compile("(\\d{20})\\.log");
    private static final int METADATA_UPDATE_INTERVAL = 1000; // Update metadata every 1000 appends

    private final String topic;
    private final int partition;
    private final Path dataDir;
    private final long maxSegmentSize;
    private final ConcurrentSkipListMap<Long, Segment> segments; // baseOffset -> Segment
    private final AtomicReference<Segment> activeSegment;
    private final SegmentMetadataStore metadataStore;
    private final AtomicLong appendsSinceMetadataUpdate; // Track appends for periodic metadata updates

    public SegmentManager(String topic, int partition, Path dataDir, long maxSegmentSize) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.dataDir = dataDir;
        this.maxSegmentSize = maxSegmentSize;
        this.segments = new ConcurrentSkipListMap<>();
        this.activeSegment = new AtomicReference<>();
        this.metadataStore = new SegmentMetadataStore(dataDir.getParent());
        this.appendsSinceMetadataUpdate = new AtomicLong(0);

        // Create data directory if it doesn't exist
        Files.createDirectories(dataDir);

        // Load existing segments
        loadSegments();

        // Create initial segment if none exist
        if (segments.isEmpty()) {
            createNewSegment(0L);
        } else {
            // Set the last segment as active
            Segment last = segments.lastEntry().getValue();
            if (!last.isFull(maxSegmentSize)) {
                activeSegment.set(last);
            } else {
                createNewSegment(last.getNextOffset());
            }
        }
    }

    /**
     * Load existing segments from disk
     */
    private void loadSegments() throws IOException {
        List<Path> logFiles = new ArrayList<>();

        // Collect all log files first
        try (Stream<Path> paths = Files.list(dataDir)) {
            paths.filter(path -> path.toString().endsWith(".log"))
                    .forEach(logFiles::add);
        }

        // Sort by filename (which contains offset)
        logFiles.sort(Path::compareTo);

        // Load segments - seal all except the last one
        for (int i = 0; i < logFiles.size(); i++) {
            Path logPath = logFiles.get(i);
            boolean isLastSegment = (i == logFiles.size() - 1);

            try {
                long baseOffset = extractOffsetFromFilename(logPath.getFileName().toString());
                Path indexPath = dataDir.resolve(String.format("%020d.index", baseOffset));

                Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize);

                // Only seal old segments, keep the last one active
                if (!isLastSegment) {
                    segment.seal();
                    log.info("Loaded and sealed segment: topic={}, partition={}, baseOffset={}",
                            topic, partition, baseOffset);
                } else {
                    log.info("Loaded active segment: topic={}, partition={}, baseOffset={}",
                            topic, partition, baseOffset);
                }

                segments.put(baseOffset, segment);

//                // Save metadata to database
//                saveSegmentMetadata(segment);

            } catch (IOException e) {
                log.error("Failed to load segment: {}", logPath, e);
            }
        }
    }

    /**
     * Extract base offset from segment filename
     */
    private long extractOffsetFromFilename(String filename) {
        Matcher matcher = SEGMENT_PATTERN.matcher(filename);
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Invalid segment filename: " + filename);
    }

    /**
     * Append a message record
     * NOTE: Does NOT modify the input record object
     */
    public long append(MessageRecord record) throws IOException {
        Segment current = activeSegment.get();

        // Check if we need to roll to a new segment
        if (current.isFull(maxSegmentSize)) {
            synchronized (this) {
                current = activeSegment.get();
                if (current.isFull(maxSegmentSize)) {
                    rollSegment();
                    current = activeSegment.get();
                }
            }
        }

        // Don't modify the record - topic and partition are not written to segment file
        // The segment only stores: offset, msgKey, eventType, data, createdAt, crc32

        long offset = current.append(record);

        // Periodically update metadata for active segment (every METADATA_UPDATE_INTERVAL appends)
        saveSegmentMetadata(current);

        return offset;
    }

    /**
     * Roll to a new segment
     */
    private synchronized void rollSegment() throws IOException {
        Segment current = activeSegment.get();
        if (current == null) {
            throw new IllegalStateException("No active segment");
        }

        // Seal the current segment
        current.seal();

        // Add to segments map
        segments.put(current.getBaseOffset(), current);

        // Save metadata for sealed segment
        saveSegmentMetadata(current);

        // Reset append counter for new active segment
        appendsSinceMetadataUpdate.set(0);

        // Create new active segment
        createNewSegment(current.getNextOffset());

        log.info("Rolled segment: topic={}, partition={}, newBaseOffset={}",
                topic, partition, current.getNextOffset());
    }

    /**
     * Create a new segment
     */
    private void createNewSegment(long baseOffset) throws IOException {
        Path logPath = dataDir.resolve(String.format("%020d.log", baseOffset));
        Path indexPath = dataDir.resolve(String.format("%020d.index", baseOffset));

        Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize);
        activeSegment.set(segment);

        log.info("Created new segment: topic={}, partition={}, baseOffset={}",
                topic, partition, baseOffset);
    }

    /**
     * Read messages from a starting offset
     */
    public List<MessageRecord> read(long fromOffset, int maxRecords) throws IOException {
        // Default max size: 1MB
        return readWithSizeLimit(fromOffset, maxRecords, 1048576);
    }

    /**
     * Read messages with cumulative size batching (SQL-like SUM() OVER pattern)
     * Reads from first record >= fromOffset, handles offset gaps gracefully
     */
    public List<MessageRecord> readWithSizeLimit(long fromOffset, int maxRecords, int maxBytes) throws IOException {
     //   log.info("SegmentManager.read() called: topic={}, partition={}, fromOffset={}, maxRecords={}, maxBytes={}", topic, partition, fromOffset, maxRecords, maxBytes);

        List<MessageRecord> records = new ArrayList<>();
        int cumulativeSize = 0;

        // Find the segment that might contain data >= fromOffset
        var entry = segments.floorEntry(fromOffset);

        if (entry == null) {
            // Check active segment
            Segment active = activeSegment.get();
            if (active != null && fromOffset < active.getNextOffset()) {
//                log.info("Using active segment for read");
                entry = new java.util.AbstractMap.SimpleEntry<>(active.getBaseOffset(), active);
            } else {
                log.debug("No segment found for offset {}, returning empty list", fromOffset);
                return records; // No data at or after this offset
            }
        }

        long currentOffset = fromOffset;
        Segment currentSegment = entry.getValue();

        while (records.size() < maxRecords && cumulativeSize < maxBytes) {
            try {
                // Try to read from current segment (will find exact or next available offset)
                if (currentOffset < currentSegment.getNextOffset()) {
                    MessageRecord record = currentSegment.read(currentOffset);

                    // Handle null return (no record at or after currentOffset in this segment)
                    if (record == null) {
                        // Move to next segment
                        var nextEntry = segments.higherEntry(currentSegment.getBaseOffset());
                        if (nextEntry == null) {
                            // Check if active segment has more data
                            Segment active = activeSegment.get();
                            if (active != null && active != currentSegment) {
                                currentSegment = active;
                                currentOffset = active.getBaseOffset();
                                continue;
                            } else {
                                break; // No more data
                            }
                        } else {
                            currentSegment = nextEntry.getValue();
                            currentOffset = currentSegment.getBaseOffset();
                            continue;
                        }
                    }

                    // Calculate record size (approximate)
                    int recordSize = calculateRecordSize(record);

                    // Check if adding this record would exceed size limit (cumulative batching)
                    if (cumulativeSize + recordSize > maxBytes && !records.isEmpty()) {
                        log.debug("Size limit reached: cumulativeSize={}, recordSize={}, maxBytes={}",
                                cumulativeSize, recordSize, maxBytes);
                        break;
                    }

                    records.add(record);
                    cumulativeSize += recordSize;
                    // IMPORTANT: Use actual record offset + 1, not currentOffset + 1
                    // This handles offset gaps correctly
                    currentOffset = record.getOffset() + 1;

                } else {
                    // Move to next segment
                    var nextEntry = segments.higherEntry(currentSegment.getBaseOffset());
                    if (nextEntry == null) {
                        // Check if active segment has more data
                        Segment active = activeSegment.get();
                        if (active != null && active != currentSegment) {
                            currentSegment = active;
                            currentOffset = active.getBaseOffset();
                        } else {
                            break; // No more data
                        }
                    } else {
                        currentSegment = nextEntry.getValue();
                        currentOffset = currentSegment.getBaseOffset();
                    }
                }
            } catch (Exception e) {
                log.error("Error reading offset {}", currentOffset, e);
                break;
            }
        }

//        log.info("Read {} records, total size: {} bytes", records.size(), cumulativeSize);
        return records;
    }

    /**
     * Zero-copy batch read: Get FileRegion for direct file-to-network transfer
     * Currently only supports reading from a single segment (no cross-segment batches)
     */
    public Segment.BatchFileRegion getZeroCopyBatch(long fromOffset, int maxRecords, long maxBytes) throws IOException {
        log.debug("SegmentManager.getZeroCopyBatch() called: topic={}, partition={}, fromOffset={}, maxRecords={}, maxBytes={}",
                topic, partition, fromOffset, maxRecords, maxBytes);

        // Find the segment that contains this offset
        var entry = segments.floorEntry(fromOffset);

        if (entry == null) {
            // Check active segment
            Segment active = activeSegment.get();
            if (active != null && fromOffset < active.getNextOffset()) {
                log.debug("Using active segment for zero-copy read");
                return active.getBatchFileRegion(fromOffset, maxRecords, maxBytes);
            } else {
                log.debug("No segment found for offset {}, returning null", fromOffset);
                return new Segment.BatchFileRegion(null, null, 0, 0, 0, fromOffset);
            }
        }

        Segment currentSegment = entry.getValue();

        // Get zero-copy batch from segment
        return currentSegment.getBatchFileRegion(fromOffset, maxRecords, maxBytes);
    }

    /**
     * Calculate approximate size of a message record
     */
    private int calculateRecordSize(MessageRecord record) {
        // Binary format: offset(8) + keyLen(4) + key + eventType(1) + dataLen(4) + data + timestamp(8) + crc(4)
        int size = 8 + 4 + record.getMsgKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 1 + 4 + 8 + 4;
        if (record.getData() != null) {
            size += record.getData().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
        return size;
    }

    /**
     * Get current highest offset
     */
    public long getCurrentOffset() {
        Segment active = activeSegment.get();
        if (active != null) {
            return active.getNextOffset() - 1;
        }

        if (!segments.isEmpty()) {
            Segment last = segments.lastEntry().getValue();
            return last.getNextOffset() - 1;
        }

        return -1; // No messages
    }

    /**
     * Get maximum offset from segment metadata (for data refresh)
     * This reads from the persistent metadata database and returns the true max offset,
     * unlike getCurrentOffset() which returns the in-memory write head.
     * Used during data refresh to determine when all historical data has been replayed.
     *
     * Metadata is now updated periodically (every 1000 appends), on segment roll, and on close,
     * so the database should always have accurate data within the update interval.
     */
    public long getMaxOffsetFromMetadata() {
        try {
            List<SegmentMetadata> segmentList = metadataStore.getSegments(topic, partition);
            if (segmentList.isEmpty()) {
                return -1;
            }

            // Find the segment with the highest max_offset
            long maxOffset = segmentList.stream()
                    .mapToLong(SegmentMetadata::getMaxOffset)
                    .max()
                    .orElse(-1);

            log.debug("Max offset from metadata for topic={}, partition={}: {}", topic, partition, maxOffset);
            return maxOffset;
        } catch (Exception e) {
            log.error("Failed to get max offset from metadata for topic={}, partition={}", topic, partition, e);
            // Fallback to in-memory offset
            return getCurrentOffset();
        }
    }

    /**
     * Get earliest (lowest) available offset
     * Returns the base offset of the first segment.
     * This may NOT be 0 if old segments have been deleted or after compaction.
     */
    public long getEarliestOffset() {
        if (!segments.isEmpty()) {
            // ConcurrentSkipListMap keeps entries sorted by key (baseOffset)
            // firstEntry() returns the segment with the lowest baseOffset
            return segments.firstEntry().getKey();
        }
        return 0; // No segments, start from 0
    }

    /**
     * Get all segments (for compaction)
     */
    public List<Segment> getAllSegments() {
        List<Segment> allSegments = new ArrayList<>(segments.values());
        Segment active = activeSegment.get();
        if (active != null && !allSegments.contains(active)) {
            allSegments.add(active);
        }
        return allSegments;
    }

    /**
     * Get inactive segments (for compaction)
     */
    public List<Segment> getInactiveSegments() {
        return new ArrayList<>(segments.values());
    }

    /**
     * Replace segments after compaction
     */
    public synchronized void replaceSegments(List<Segment> oldSegments, Segment newSegment) throws IOException {
        // Remove old segments from map
        for (Segment old : oldSegments) {
            segments.remove(old.getBaseOffset());
            old.close();

            // Delete files
            Files.deleteIfExists(old.getLogPath());
            Files.deleteIfExists(old.getIndexPath());
        }

        // Add new compacted segment
        segments.put(newSegment.getBaseOffset(), newSegment);

        log.info("Replaced {} segments with compacted segment at offset {}",
                oldSegments.size(), newSegment.getBaseOffset());
    }

    /**
     * Save segment metadata to database
     */
    private void saveSegmentMetadata(Segment segment) {
        try {
            long recordCount = Optional.ofNullable(metadataStore.getSegments(topic, partition))
                    .filter(list -> !list.isEmpty())
                    .map(list -> list.get(0).getRecordCount())
                    .orElse(0L) + 1;

            SegmentMetadata metadata = SegmentMetadata.builder()
                    .topic(topic)
                    .partition(partition)
                    .baseOffset(segment.getBaseOffset())
                    .maxOffset(segment.getNextOffset() - 1)
                    .logFilePath(segment.getLogPath().toString())
                    .indexFilePath(segment.getIndexPath().toString())
                    .sizeBytes(segment.getSize())
                    .recordCount(recordCount)
                    .build();

            metadataStore.saveSegment(metadata);
        } catch (Exception e) {
            log.error("Failed to save segment metadata", e);
        }
    }

    /**
     * Close all segments
     */
    public void close() throws IOException {
        Segment active = activeSegment.get();
        if (active != null) {
            // Save final metadata for active segment before closing
            saveSegmentMetadata(active);
            log.info("Saved final metadata for active segment on close: topic={}, partition={}, maxOffset={}",
                    topic, partition, active.getNextOffset() - 1);
            active.close();
        }

        for (Segment segment : segments.values()) {
            segment.close();
        }

        // Close metadata store
        metadataStore.close();
    }

    // Getters
    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }
}
