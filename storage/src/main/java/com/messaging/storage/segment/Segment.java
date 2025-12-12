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
    private static final int INDEX_ENTRY_SIZE = 12; // 8 bytes actual offset (long) + 4 bytes position
    private static final long INITIAL_MMAP_SIZE = 64 * 1024 * 1024; // Start with 64MB
    private static final long MMAP_GROWTH_SIZE = 64 * 1024 * 1024; // Grow by 64MB chunks
    private static final long INDEX_INITIAL_SIZE = 1 * 1024 * 1024; // 1MB for index

    private  long baseOffset;
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

        // If segment has existing data, recover nextOffset by scanning the log
        if (existingLogSize > 0) {
            recoverNextOffset();
        }
    }

    /**
     * Recover nextOffset using index file (Kafka-style) - MUCH faster than scanning log
     */
    private void recoverNextOffset() {
        log.info("Recovering nextOffset for segment at baseOffset={}, logPosition={}, indexPosition={}",
                baseOffset, logPosition, indexPosition);

        try {
            // If index is empty, scan the log as fallback
            if (indexPosition == 0) {
                recoverFromLogScan();
                return;
            }

            // Read the last index entry (each entry is 8 bytes: 4 for offset + 4 for position)
            int lastIndexEntryPos = indexPosition - INDEX_ENTRY_SIZE;

            if (lastIndexEntryPos < 0) {
                log.warn("Invalid index position, falling back to log scan");
                recoverFromLogScan();
                return;
            }

            // Read last index entry
            int relativeOffset = indexBuffer.getInt(lastIndexEntryPos);
            int logPos = indexBuffer.getInt(lastIndexEntryPos + 4);

            long lastIndexedOffset = baseOffset + relativeOffset;

            log.info("Last index entry: relativeOffset={}, logPosition={}, absoluteOffset={}",
                    relativeOffset, logPos, lastIndexedOffset);

            // Now scan from the last indexed position to find the actual highest offset
            // (there may be records after the last index entry)
            long highestOffset = lastIndexedOffset;
            int position = logPos;
            long recordCount = 0;

            while (position < logPosition) {
                // Check if we have enough bytes to read offset (8 bytes)
                if (position + 8 > logPosition) {
                    break;
                }

                // Read offset at this position
                long recordOffset = logBuffer.getLong(position);
                position += 8;
                recordCount++;

                // Track the highest offset
                if (recordOffset > highestOffset) {
                    highestOffset = recordOffset;
                }

                // Skip the rest of the record
                if (position + 4 > logPosition) break;
                int keyLen = logBuffer.getInt(position);
                position += 4;

                if (position + keyLen > logPosition) break;
                position += keyLen;

                if (position + 1 > logPosition) break;
                position += 1; // event type

                if (position + 4 > logPosition) break;
                int dataLen = logBuffer.getInt(position);
                position += 4;

                if (position + dataLen > logPosition) break;
                position += dataLen;

                if (position + 12 > logPosition) break;
                position += 12; // timestamp (8) + crc32 (4)
            }

            // nextOffset is the highest offset + 1
            this.nextOffset = highestOffset + 1;

            log.info("Recovered segment using index: baseOffset={}, lastIndexedOffset={}, scannedRecords={}, highestOffset={}, nextOffset={}",
                    baseOffset, lastIndexedOffset, recordCount, highestOffset, nextOffset);

        } catch (Exception e) {
            log.error("Error recovering from index, falling back to log scan", e);
            recoverFromLogScan();
        }
    }

    /**
     * Fallback: Recover by scanning the entire log file (slow, used only when index is unavailable)
     */
    private void recoverFromLogScan() {
        log.warn("Using slow log scan for recovery at baseOffset={}", baseOffset);

        int position = 0;
        int recordStartPosition = 0;
        long recordCount = 0;
        long highestOffset = baseOffset - 1;
        int consecutiveZeroRecords = 0;
        final int MAX_CONSECUTIVE_ZEROS = 10; // Stop if we hit 10 consecutive zero-offset records

        try {
            // Reset index position to rebuild it
            indexPosition = 0;

            while (position < logPosition) {
                recordStartPosition = position; // Track where this record starts

                if (position + 8 > logPosition) break;
                long recordOffset = logBuffer.getLong(position);
                position += 8;

                // Detect pre-allocated zero region: if we hit multiple consecutive records with offset=0
                // AFTER we've seen higher offsets, we've reached the pre-allocated zero region
                // Note: The first record can legitimately be offset 0 (equals baseOffset)
                if (recordOffset == 0 && highestOffset > 10) { // Only detect zeros after we've seen real data
                    consecutiveZeroRecords++;
                    if (consecutiveZeroRecords >= MAX_CONSECUTIVE_ZEROS) {
                        log.info("Detected pre-allocated zero region at position {}, stopping scan", position - 8);
                        break;
                    }
                } else {
                    consecutiveZeroRecords = 0;
                }

                if (recordOffset > highestOffset) {
                    highestOffset = recordOffset;
                }

                if (position + 4 > logPosition) break;
                int keyLen = logBuffer.getInt(position);
                position += 4;

                // Validate keyLen: should be reasonable (UUIDs are 36 bytes, typically 10-100 bytes)
                if (keyLen < 0 || keyLen > 1000) {
                    log.warn("Invalid keyLen={} at position {}, stopping scan", keyLen, position - 4);
                    break;
                }

                if (position + keyLen > logPosition) break;
                position += keyLen;

                if (position + 1 > logPosition) break;
                position += 1;

                if (position + 4 > logPosition) break;
                int dataLen = logBuffer.getInt(position);
                position += 4;

                // Validate dataLen: should be reasonable (typically < 100KB for messages)
                if (dataLen < 0 || dataLen > 10 * 1024 * 1024) {
                    log.warn("Invalid dataLen={} at position {}, stopping scan", dataLen, position - 4);
                    break;
                }

                if (position + dataLen > logPosition) break;
                position += dataLen;

                if (position + 12 > logPosition) break;
                position += 12;

                recordCount++;

                // Rebuild index: add entry every 4KB (similar to append logic)
                if (recordStartPosition % 4096 < (position - recordStartPosition)) {
                    addIndexEntry(recordOffset, recordStartPosition);
                }
            }

            // Update logPosition to reflect actual data end
            this.logPosition = position;
            this.nextOffset = highestOffset + 1;
            log.info("Recovered segment (log scan): baseOffset={}, recordCount={}, highestOffset={}, nextOffset={}, actualLogPosition={}, rebuiltIndexEntries={}",
                    baseOffset, recordCount, highestOffset, nextOffset, logPosition, indexPosition / INDEX_ENTRY_SIZE);

            // Force index buffer to disk
            indexBuffer.force();

        } catch (Exception e) {
            log.error("Error in log scan recovery, using baseOffset as fallback", e);
            this.nextOffset = baseOffset;
        }
    }

    /**
     * Append a message record to the segment
     * @return The assigned offset
     */
    public synchronized long append(MessageRecord record) throws IOException {
        if (!active) {
            throw new IllegalStateException("Segment is not active");
        }


        this.nextOffset=record.getOffset()+1;
        long offset = record.getOffset();

        // Calculate CRC32 for the record with the new offset
        int crc32 = calculateCRC32(record, offset);

        // Write record to log buffer with assigned offset and CRC32
        // NOTE: This does NOT modify the input record object
        int recordSize = writeRecord(record, offset, crc32);

        // Update index every 4KB
        if (logPosition % 4096 < recordSize) {
            addIndexEntry(offset, logPosition - recordSize);
        }

        if(this.baseOffset==0){
            this.baseOffset=offset;
        }
        return record.getOffset();
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

        // Store actual cloud offset as 8-byte long (not relative offset)
        indexBuffer.putLong(indexPosition, offset);
        indexPosition += 8;
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
     * Read a record at the given offset or next available offset
     * Returns null if no record exists at or after the requested offset
     */
    public MessageRecord read(long offset) throws IOException {
        // Remove strict range validation - allow reading from next available offset
        // This handles offset gaps gracefully

        // Find position using index (returns exact match or next available)
        //log.info("Calling findPosition() for offset={}", offset);
        int position = findPosition(offset);
        //log.info("findPosition() returned position={}", position);

        // Handle case where no offset >= target exists in this segment
        if (position == -1) {
            log.debug("No record found at or after offset {} in segment", offset);
            return null;
        }

        //log.info("Calling readRecordAt() at position={}", position);
        MessageRecord record = readRecordAt(position);
        //log.info("Successfully read record: key={}", record.getMsgKey());

        return record;
    }

    /**
     * Find the file position for a given offset using the index
     */
    private int findPosition(long offset) {
        // No conversion needed - use actual offset directly

        //log.info("findPosition(): offset={}, indexPosition={}, logPosition={}, active={}",
            //     offset, indexPosition, logPosition, active);

        // For active segments with no index or small index, use sequential scan from start
        // This is similar to how Kafka handles active segment reads
        if (active && indexPosition < INDEX_ENTRY_SIZE) {
           // log.info("Active segment with no index entries, scanning from position 0");
            return scanToOffset(0, offset);
        }

        // Binary search in index for sealed segments or active segments with index
        int low = 0;
        int high = (indexPosition / INDEX_ENTRY_SIZE) - 1;
        int resultPosition = 0;

       // log.info("Binary search range: low={}, high={}", low, high);

        while (low <= high) {
            int mid = (low + high) / 2;
            long midOffset = indexBuffer.getLong(mid * INDEX_ENTRY_SIZE);  // Read actual offset as long

            if (midOffset == offset) {
                return indexBuffer.getInt(mid * INDEX_ENTRY_SIZE + 8);  // Position is at offset+8 (long=8 bytes)
            } else if (midOffset < offset) {
                resultPosition = indexBuffer.getInt(mid * INDEX_ENTRY_SIZE + 8);  // Position is at offset+8
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

       // log.info("Binary search complete. Calling scanToOffset(resultPosition={}, offset={})",
             //    resultPosition, offset);

        // Scan forward from the nearest index entry
        return scanToOffset(resultPosition, offset);
    }

    /**
     * Scan forward from a position to find the exact offset or next available offset
     * Implements "OrNext" behavior - returns position of exact match or first offset > target
     */
    private int scanToOffset(int startPosition, long targetOffset) {
        int position = startPosition;

       // log.info("scanToOffset(): startPosition={}, targetOffset={}, logPosition={}",
        //         startPosition, targetOffset, logPosition);

        int iterations = 0;
        while (position < logPosition) {
            iterations++;
            if (iterations > Long.MAX_VALUE) {
                log.error("scanToOffset() exceeded 1000 iterations! position={}, logPosition={}, startPosition={}",
                         position, logPosition, startPosition);
                throw new IllegalStateException("Scan loop exceeded maximum iterations");
            }

            log.debug("scanToOffset iteration {}: position={}", iterations, position);

            long recordOffset = logBuffer.getLong(position);
            log.debug("Read recordOffset={} at position={}", recordOffset, position);

            if (recordOffset == targetOffset) {
                //log.info("Found exact target offset at position={}", position);
                return position;
            }

            if (recordOffset > targetOffset) {
                // Found first offset > target - return this position (handles offset gaps)
                log.debug("Auto-advancing: Found offset {} (requested >= {})", recordOffset, targetOffset);
                return position;
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

        // No offset >= target found in this segment
        log.debug("No offset >= {} found in segment (scanned to end)", targetOffset);
        return -1;
    }

    /**
     * Read a record at a specific file position
     * No offset validation - reads whatever record exists at the position
     */
    private MessageRecord readRecordAt(int position) throws IOException {
        MessageRecord record = new MessageRecord();

        // Read offset (no validation - accept whatever offset is stored)
        long offset = logBuffer.getLong(position);
        position += 8;

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
