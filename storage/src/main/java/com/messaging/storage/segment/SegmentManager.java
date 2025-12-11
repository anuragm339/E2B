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
import java.util.concurrent.ConcurrentSkipListMap;
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

    private final String topic;
    private final int partition;
    private final Path dataDir;
    private final long maxSegmentSize;
    private final ConcurrentSkipListMap<Long, Segment> segments; // baseOffset -> Segment
    private final AtomicReference<Segment> activeSegment;
    private final SegmentMetadataStore metadataStore;

    public SegmentManager(String topic, int partition, Path dataDir, long maxSegmentSize) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.dataDir = dataDir;
        this.maxSegmentSize = maxSegmentSize;
        this.segments = new ConcurrentSkipListMap<>();
        this.activeSegment = new AtomicReference<>();
        this.metadataStore = new SegmentMetadataStore(dataDir.getParent());

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

                // Save metadata to database
                saveSegmentMetadata(segment);

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

        return current.append(record);
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
     * Read messages with size limit (1MB batching)
     */
    public List<MessageRecord> readWithSizeLimit(long fromOffset, int maxRecords, int maxBytes) throws IOException {
     //   log.info("SegmentManager.read() called: topic={}, partition={}, fromOffset={}, maxRecords={}, maxBytes={}", topic, partition, fromOffset, maxRecords, maxBytes);

        List<MessageRecord> records = new ArrayList<>();
        int cumulativeSize = 0;

        // Find the segment containing the starting offset
        var entry = segments.floorEntry(fromOffset);
       // log.info("segments.floorEntry({}) returned: {}", fromOffset, entry != null ? entry.getKey() : "null");

        if (entry == null) {
            // Check active segment
            Segment active = activeSegment.get();
//            log.info("Checking active segment: baseOffset={}, nextOffset={}",
//                    active != null ? active.getBaseOffset() : "null",
//                    active != null ? active.getNextOffset() : "null");

            if (active != null && fromOffset >= active.getBaseOffset() && fromOffset < active.getNextOffset()) {
//                log.info("Using active segment for read");
                entry = new java.util.AbstractMap.SimpleEntry<>(active.getBaseOffset(), active);
            } else {
                log.info("No segment found for offset {}, returning empty list", fromOffset);
                return records; // No data at this offset
            }
        }

        long currentOffset = fromOffset;
        Segment currentSegment = entry.getValue();

        while (records.size() < maxRecords && cumulativeSize < maxBytes) {
            try {
                // Try to read from current segment
                if (currentOffset >= currentSegment.getBaseOffset() &&
                        currentOffset < currentSegment.getNextOffset()) {

                    MessageRecord record = currentSegment.read(currentOffset);

                    // Calculate record size (approximate)
                    int recordSize = calculateRecordSize(record);

                    // Check if adding this record would exceed size limit
                    if (cumulativeSize + recordSize > maxBytes && !records.isEmpty()) {
                        log.info("Size limit reached: cumulativeSize={}, recordSize={}, maxBytes={}",
                                cumulativeSize, recordSize, maxBytes);
                        break;
                    }

                    records.add(record);
                    cumulativeSize += recordSize;
                    currentOffset++;

                } else {
                    // Move to next segment
                    var nextEntry = segments.higherEntry(currentSegment.getBaseOffset());
                    if (nextEntry == null) {
                        // Check if active segment has more data
                        Segment active = activeSegment.get();
                        if (active != null && active != currentSegment &&
                            currentOffset >= active.getBaseOffset() && currentOffset < active.getNextOffset()) {
                            currentSegment = active;
                        } else {
                            break; // No more data
                        }
                    } else {
                        currentSegment = nextEntry.getValue();
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
            SegmentMetadata metadata = SegmentMetadata.builder()
                    .topic(topic)
                    .partition(partition)
                    .baseOffset(segment.getBaseOffset())
                    .maxOffset(segment.getNextOffset() - 1)
                    .logFilePath(segment.getLogPath().toString())
                    .indexFilePath(segment.getIndexPath().toString())
                    .sizeBytes(segment.getSize())
                    .recordCount(segment.getNextOffset() - segment.getBaseOffset())
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
