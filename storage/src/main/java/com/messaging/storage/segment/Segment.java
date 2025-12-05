package com.messaging.storage.segment;

import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * Represents a single segment file with memory-mapped I/O
 */
public class Segment {
    private static final Logger log = LoggerFactory.getLogger(Segment.class);
    private static final int INDEX_ENTRY_SIZE = 8; // 4 bytes relative offset + 4 bytes position
    private static final long INITIAL_MMAP_SIZE = 64 * 1024 * 1024; // Start with 64MB
    private static final long MMAP_GROWTH_SIZE = 64 * 1024 * 1024; // Grow by 64MB chunks
    private static final long INDEX_INITIAL_SIZE = 1 * 1024 * 1024; // 1MB for index

    private final long baseOffset;
    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;
    private final long maxSize;
    private MappedByteBuffer logBuffer;
    private MappedByteBuffer indexBuffer;
    private long currentLogMappedSize;
    private long currentIndexMappedSize;

    private long nextOffset;
    private int logPosition;
    private int indexPosition;
    private boolean active;

    public Segment(Path logPath, Path indexPath, long baseOffset, long maxSize) throws IOException {
        this.baseOffset = baseOffset;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.nextOffset = baseOffset;
        this.logPosition = 0;
        this.indexPosition = 0;
        this.active = true;
        this.maxSize = maxSize;

        // Open log file channel
        this.logChannel = FileChannel.open(logPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

        // Open index file channel
        this.indexChannel = FileChannel.open(indexPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

        // Determine initial mapping size based on existing file size
        long existingLogSize = logChannel.size();
        long existingIndexSize = indexChannel.size();

        // For existing files, map the actual size + growth buffer
        // For new files, map initial small size
        if (existingLogSize > 0) {
            this.currentLogMappedSize = Math.min(existingLogSize + MMAP_GROWTH_SIZE, maxSize);
            this.logPosition = (int) existingLogSize;
        } else {
            this.currentLogMappedSize = Math.min(INITIAL_MMAP_SIZE, maxSize);
        }

        if (existingIndexSize > 0) {
            this.currentIndexMappedSize = existingIndexSize + (INDEX_INITIAL_SIZE / 2);
            this.indexPosition = (int) existingIndexSize;
        } else {
            this.currentIndexMappedSize = INDEX_INITIAL_SIZE;
        }

        // Memory-map the log file with initial size
        this.logBuffer = logChannel.map(FileChannel.MapMode.READ_WRITE, 0, currentLogMappedSize);

        // Memory-map the index file with initial size
        this.indexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, currentIndexMappedSize);

        log.info("Created segment with baseOffset={} at {}, logMapped={}MB, indexMapped={}KB",
                baseOffset, logPath, currentLogMappedSize / (1024 * 1024), currentIndexMappedSize / 1024);
    }

    /**
     * Append a message record to the segment
     * @return The assigned offset
     */
    public synchronized long append(MessageRecord record) throws IOException {
        if (!active) {
            throw new IllegalStateException("Segment is not active");
        }

        long offset = nextOffset++;

        // Calculate CRC32 for the record with the new offset
        int crc32 = calculateCRC32(record, offset);

        // Write record to log buffer with assigned offset and CRC32
        // NOTE: This does NOT modify the input record object
        int recordSize = writeRecord(record, offset, crc32);

        // Update index every 4KB
        if (logPosition % 4096 < recordSize) {
            addIndexEntry(offset, logPosition - recordSize);
        }

        return offset;
    }

    /**
     * Write record to log buffer in binary format
     * Returns the number of bytes written
     * NOTE: Does NOT modify the input record object
     */
    private int writeRecord(MessageRecord record, long offset, int crc32) throws IOException {
        int startPosition = logPosition;

        // Calculate record size first
        byte[] keyBytes = record.getMsgKey().getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = record.getData() != null ?
                record.getData().getBytes(StandardCharsets.UTF_8) : new byte[0];

        int recordSize = 8 + 4 + keyBytes.length + 1 + 4 + dataBytes.length + 8 + 4;

        // Check if we need to expand the mapped region
        if (logPosition + recordSize > currentLogMappedSize) {
            expandLogMapping(logPosition + recordSize);
        }

        // Format: [offset:8][key_len:4][key:var][event_type:1][data_len:4][data:var][created_at:8][crc32:4]

        // Write offset (use the provided offset, not record.getOffset())
        logBuffer.putLong(logPosition, offset);
        logPosition += 8;

        // Write msg_key
        logBuffer.putInt(logPosition, keyBytes.length);
        logPosition += 4;
        logBuffer.position(logPosition);
        logBuffer.put(keyBytes);
        logPosition += keyBytes.length;

        // Write event_type
        logBuffer.put(logPosition, (byte) record.getEventType().getCode());
        logPosition += 1;

        // Write data (null for DELETE events)
        logBuffer.putInt(logPosition, dataBytes.length);
        logPosition += 4;
        if (dataBytes.length > 0) {
            logBuffer.position(logPosition);
            logBuffer.put(dataBytes);
            logPosition += dataBytes.length;
        }

        // Write created_at (epoch millis)
        logBuffer.putLong(logPosition, record.getCreatedAt().toEpochMilli());
        logPosition += 8;

        // Write CRC32 (use the provided crc32, not record.getCrc32())
        logBuffer.putInt(logPosition, crc32);
        logPosition += 4;

        return logPosition - startPosition;
    }

    /**
     * Expand the log file mapping when needed
     */
    private synchronized void expandLogMapping(long requiredSize) throws IOException {
        long newSize = Math.min(
            Math.max(currentLogMappedSize + MMAP_GROWTH_SIZE, requiredSize + MMAP_GROWTH_SIZE),
            maxSize
        );

        if (newSize <= currentLogMappedSize) {
            return; // Already large enough
        }

        log.info("Expanding log mapping from {}MB to {}MB for segment at offset {}",
                currentLogMappedSize / (1024 * 1024), newSize / (1024 * 1024), baseOffset);

        // Unmap old buffer (this is important for releasing memory)
        try {
            java.lang.reflect.Method cleanerMethod = logBuffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(logBuffer);
            if (cleaner != null) {
                java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            // Cleaner not available, let GC handle it
            log.debug("Buffer cleaner not available, relying on GC");
        }

        // Remap with new size
        logBuffer = logChannel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
        currentLogMappedSize = newSize;
    }

    /**
     * Add an index entry
     */
    private void addIndexEntry(long offset, int position) throws IOException {
        // Check if we need to expand index mapping
        if (indexPosition + INDEX_ENTRY_SIZE > currentIndexMappedSize) {
            expandIndexMapping();
        }

        int relativeOffset = (int) (offset - baseOffset);
        indexBuffer.putInt(indexPosition, relativeOffset);
        indexPosition += 4;
        indexBuffer.putInt(indexPosition, position);
        indexPosition += 4;
    }

    /**
     * Expand the index file mapping when needed
     */
    private synchronized void expandIndexMapping() throws IOException {
        long newSize = currentIndexMappedSize + INDEX_INITIAL_SIZE;

        log.info("Expanding index mapping from {}KB to {}KB for segment at offset {}",
                currentIndexMappedSize / 1024, newSize / 1024, baseOffset);

        // Unmap old buffer
        try {
            java.lang.reflect.Method cleanerMethod = indexBuffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(indexBuffer);
            if (cleaner != null) {
                java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            log.debug("Buffer cleaner not available, relying on GC");
        }

        // Remap with new size
        indexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
        currentIndexMappedSize = newSize;
    }

    /**
     * Read a record at the given offset
     */
    public MessageRecord read(long offset) throws IOException {
        log.info("Segment.read() called: offset={}, baseOffset={}, nextOffset={}",
                 offset, baseOffset, nextOffset);

        if (offset < baseOffset || offset >= nextOffset) {
            throw new IllegalArgumentException("Offset " + offset + " out of range [" + baseOffset + ", " + nextOffset + ")");
        }

        // Find position using index
        log.info("Calling findPosition() for offset={}", offset);
        int position = findPosition(offset);
        log.info("findPosition() returned position={}", position);

        log.info("Calling readRecordAt() at position={}", position);
        MessageRecord record = readRecordAt(position, offset);
        log.info("Successfully read record: key={}", record.getMsgKey());

        return record;
    }

    /**
     * Find the file position for a given offset using the index
     */
    private int findPosition(long offset) {
        int relativeOffset = (int) (offset - baseOffset);

        log.info("findPosition(): relativeOffset={}, indexPosition={}, logPosition={}, active={}",
                 relativeOffset, indexPosition, logPosition, active);

        // For active segments with no index or small index, use sequential scan from start
        // This is similar to how Kafka handles active segment reads
        if (active && indexPosition < INDEX_ENTRY_SIZE) {
            log.info("Active segment with no index entries, scanning from position 0");
            return scanToOffset(0, offset);
        }

        // Binary search in index for sealed segments or active segments with index
        int low = 0;
        int high = (indexPosition / INDEX_ENTRY_SIZE) - 1;
        int resultPosition = 0;

        log.info("Binary search range: low={}, high={}", low, high);

        while (low <= high) {
            int mid = (low + high) / 2;
            int midOffset = indexBuffer.getInt(mid * INDEX_ENTRY_SIZE);

            if (midOffset == relativeOffset) {
                return indexBuffer.getInt(mid * INDEX_ENTRY_SIZE + 4);
            } else if (midOffset < relativeOffset) {
                resultPosition = indexBuffer.getInt(mid * INDEX_ENTRY_SIZE + 4);
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        log.info("Binary search complete. Calling scanToOffset(resultPosition={}, offset={})",
                 resultPosition, offset);

        // Scan forward from the nearest index entry
        return scanToOffset(resultPosition, offset);
    }

    /**
     * Scan forward from a position to find the exact offset
     */
    private int scanToOffset(int startPosition, long targetOffset) {
        int position = startPosition;

        log.info("scanToOffset(): startPosition={}, targetOffset={}, logPosition={}",
                 startPosition, targetOffset, logPosition);

        int iterations = 0;
        while (position < logPosition) {
            iterations++;
            if (iterations > 1000) {
                log.error("scanToOffset() exceeded 1000 iterations! position={}, logPosition={}, startPosition={}",
                         position, logPosition, startPosition);
                throw new IllegalStateException("Scan loop exceeded maximum iterations");
            }

            log.debug("scanToOffset iteration {}: position={}", iterations, position);

            long recordOffset = logBuffer.getLong(position);
            log.debug("Read recordOffset={} at position={}", recordOffset, position);

            if (recordOffset == targetOffset) {
                log.info("Found target offset at position={}", position);
                return position;
            }

            if (recordOffset > targetOffset) {
                log.error("Offset not found: targetOffset={}, recordOffset={}", targetOffset, recordOffset);
                throw new IllegalStateException("Offset not found: " + targetOffset);
            }

            // Skip to next record
            position += 8; // offset
            int keyLen = logBuffer.getInt(position);
            log.debug("keyLen={}", keyLen);
            position += 4 + keyLen; // key_len + key
            position += 1; // event_type
            int dataLen = logBuffer.getInt(position);
            log.debug("dataLen={}", dataLen);
            position += 4 + dataLen; // data_len + data
            position += 8; // created_at
            position += 4; // crc32
        }

        log.error("Exited scan loop without finding offset. position={}, logPosition={}", position, logPosition);
        throw new IllegalStateException("Offset not found: " + targetOffset);
    }

    /**
     * Read a record at a specific file position
     */
    private MessageRecord readRecordAt(int position, long expectedOffset) throws IOException {
        MessageRecord record = new MessageRecord();

        // Read offset
        long offset = logBuffer.getLong(position);
        position += 8;

        if (offset != expectedOffset) {
            throw new IOException("Offset mismatch: expected " + expectedOffset + ", found " + offset);
        }

        record.setOffset(offset);

        // Read msg_key
        int keyLen = logBuffer.getInt(position);
        position += 4;
        byte[] keyBytes = new byte[keyLen];
        logBuffer.position(position);
        logBuffer.get(keyBytes);
        position += keyLen;
        record.setMsgKey(new String(keyBytes, StandardCharsets.UTF_8));

        // Read event_type
        byte eventTypeCode = logBuffer.get(position);
        position += 1;
        record.setEventType(EventType.fromCode((char) eventTypeCode));

        // Read data
        int dataLen = logBuffer.getInt(position);
        position += 4;
        if (dataLen > 0) {
            byte[] dataBytes = new byte[dataLen];
            logBuffer.position(position);
            logBuffer.get(dataBytes);
            position += dataLen;
            record.setData(new String(dataBytes, StandardCharsets.UTF_8));
        }

        // Read created_at
        long createdAtMillis = logBuffer.getLong(position);
        position += 8;
        record.setCreatedAt(Instant.ofEpochMilli(createdAtMillis));

        // Read and verify CRC32
        int storedCrc32 = logBuffer.getInt(position);
        record.setCrc32(storedCrc32);

        int calculatedCrc32 = calculateCRC32(record, offset);
        if (storedCrc32 != calculatedCrc32) {
            throw new IOException("CRC32 mismatch for offset " + offset);
        }

        return record;
    }

    /**
     * Calculate CRC32 checksum for a record
     */
    private int calculateCRC32(MessageRecord record, long offset) {
        CRC32 crc = new CRC32();

        // Include all fields except CRC32 itself
        // Use the provided offset (not record.getOffset() which may be the parent's offset)
        crc.update(ByteBuffer.allocate(8).putLong(offset).array());
        crc.update(record.getMsgKey().getBytes(StandardCharsets.UTF_8));
        crc.update((byte) record.getEventType().getCode());
        if (record.getData() != null) {
            crc.update(record.getData().getBytes(StandardCharsets.UTF_8));
        }
        crc.update(ByteBuffer.allocate(8).putLong(record.getCreatedAt().toEpochMilli()).array());

        return (int) crc.getValue();
    }

    /**
     * Seal the segment (make it read-only)
     */
    public synchronized void seal() {
        if (!active) {
            return;
        }

        active = false;

        // Force buffers to disk
        logBuffer.force();
        indexBuffer.force();

        log.info("Sealed segment with baseOffset={}, records={}", baseOffset, nextOffset - baseOffset);
    }

    /**
     * Close the segment and release resources
     */
    public void close() throws IOException {
        seal();

        logChannel.close();
        indexChannel.close();
    }

    // Getters
    public long getBaseOffset() {
        return baseOffset;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public int getSize() {
        return logPosition;
    }

    public boolean isActive() {
        return active;
    }

    public boolean isFull(long maxSize) {
        return logPosition >= maxSize;
    }

    public Path getLogPath() {
        return logPath;
    }

    public Path getIndexPath() {
        return indexPath;
    }
}
