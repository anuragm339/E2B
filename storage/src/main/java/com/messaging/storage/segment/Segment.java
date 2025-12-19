package com.messaging.storage.segment;

import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.CRC32;

/**
 * Represents a single segment file using FileChannel (no memory mapping)
 * Enables true zero-copy via Netty FileRegion
 */
public class Segment {
    private static final Logger log = LoggerFactory.getLogger(Segment.class);
    private static final int INDEX_ENTRY_SIZE = 12; // 8 bytes offset + 4 bytes position
    private static final int WRITE_BUFFER_SIZE = 16 * 1024; // 16KB reusable buffer

    private long baseOffset;
    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;
    private final long maxSize;

    // In-memory index: offset -> file position
    private final ConcurrentSkipListMap<Long, Integer> index;

    // Thread-local buffers for reads (to avoid allocation)
    private final ThreadLocal<ByteBuffer> readBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(WRITE_BUFFER_SIZE));

    private long nextOffset;
    private long logPosition; // Current write position in log file
    private boolean active;

    public Segment(Path logPath, Path indexPath, long baseOffset, long maxSize) throws IOException {
        this.baseOffset = baseOffset;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.nextOffset = baseOffset;
        this.logPosition = 0;
        this.active = true;
        this.maxSize = maxSize;
        this.index = new ConcurrentSkipListMap<>();

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

        // Initialize write position from file size
        this.logPosition = logChannel.size();

        log.info("Created segment with baseOffset={} at {}, logSize={}MB",
                baseOffset, logPath, logPosition / (1024 * 1024));

        // If segment has existing data, recover index and nextOffset
        if (logPosition > 0) {
            recoverIndex();
        }
    }

    /**
     * Recover in-memory index by reading index file or scanning log
     */
    private void recoverIndex() {
        log.info("Recovering index for segment at baseOffset={}, logPosition={}", baseOffset, logPosition);

        try {
            long indexSize = indexChannel.size();

            if (indexSize > 0) {
                // Read index file into memory
                recoverFromIndexFile(indexSize);
            } else {
                // No index file, scan log
                recoverFromLogScan();
            }
        } catch (Exception e) {
            log.error("Error recovering index, falling back to log scan", e);
            recoverFromLogScan();
        }
    }

    /**
     * Recover index from index file
     */
    private void recoverFromIndexFile(long indexSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        long position = 0;

        while (position < indexSize) {
            buffer.clear();
            int bytesRead = indexChannel.read(buffer, position);
            if (bytesRead < INDEX_ENTRY_SIZE) break;

            buffer.flip();
            long offset = buffer.getLong();
            int logPos = buffer.getInt();

            index.put(offset, logPos);
            position += INDEX_ENTRY_SIZE;
        }

        // Find highest offset by scanning from last indexed position
        if (!index.isEmpty()) {
            long lastIndexedOffset = index.lastKey();
            int lastIndexedPosition = index.get(lastIndexedOffset);

            long highestOffset = scanForHighestOffset(lastIndexedPosition);
            this.nextOffset = highestOffset + 1;

            log.info("Recovered index with {} entries, nextOffset={}", index.size(), nextOffset);
        } else {
            recoverFromLogScan();
        }
    }

    /**
     * Scan from position to find highest offset
     */
    private long scanForHighestOffset(int startPosition) throws IOException {
        long highestOffset = baseOffset - 1;
        long position = startPosition;
        ByteBuffer buffer = ByteBuffer.allocate(20); // Header buffer

        while (position < logPosition) {
            buffer.clear();
            buffer.limit(8); // Read offset only
            int bytesRead = logChannel.read(buffer, position);
            if (bytesRead < 8) break;

            buffer.flip();
            long recordOffset = buffer.getLong();

            if (recordOffset > highestOffset) {
                highestOffset = recordOffset;
            }

            // Skip rest of record
            position += 8;

            // Read key length
            buffer.clear();
            buffer.limit(4);
            bytesRead = logChannel.read(buffer, position);
            if (bytesRead < 4) break;
            buffer.flip();
            int keyLen = buffer.getInt();
            position += 4 + keyLen;

            // Skip event type (1 byte)
            position += 1;

            // Read data length
            buffer.clear();
            buffer.limit(4);
            bytesRead = logChannel.read(buffer, position);
            if (bytesRead < 4) break;
            buffer.flip();
            int dataLen = buffer.getInt();
            position += 4 + dataLen;

            // Skip timestamp (8) + crc32 (4)
            position += 12;
        }

        return highestOffset;
    }

    /**
     * Fallback: Recover by scanning the entire log file
     */
    private void recoverFromLogScan() {
        log.warn("Using log scan for recovery at baseOffset={}", baseOffset);

        try {
            index.clear();
            long position = 0;
            long highestOffset = baseOffset - 1;
            long recordCount = 0;
            ByteBuffer buffer = ByteBuffer.allocate(20);

            while (position < logPosition) {
                int recordStart = (int) position;

                // Read offset
                buffer.clear();
                buffer.limit(8);
                int bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 8) break;
                buffer.flip();
                long recordOffset = buffer.getLong();
                position += 8;

                if (recordOffset > highestOffset) {
                    highestOffset = recordOffset;
                }

                // Read key length
                buffer.clear();
                buffer.limit(4);
                bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 4) break;
                buffer.flip();
                int keyLen = buffer.getInt();
                position += 4;

                if (keyLen < 0 || keyLen > 1000) {
                    log.warn("Invalid keyLen={}, stopping scan", keyLen);
                    break;
                }
                position += keyLen;

                // Skip event type
                position += 1;

                // Read data length
                buffer.clear();
                buffer.limit(4);
                bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 4) break;
                buffer.flip();
                int dataLen = buffer.getInt();
                position += 4;

                if (dataLen < 0 || dataLen > 10 * 1024 * 1024) {
                    log.warn("Invalid dataLen={}, stopping scan", dataLen);
                    break;
                }
                position += dataLen;

                // Skip timestamp + crc32
                position += 12;

                recordCount++;

                // Add to index every 4KB
                if (recordStart % 4096 < 100) { // Within 100 bytes of 4KB boundary
                    index.put(recordOffset, recordStart);
                    writeIndexEntry(recordOffset, recordStart);
                }
            }

            this.logPosition = position;
            this.nextOffset = highestOffset + 1;

            log.info("Recovered segment (log scan): baseOffset={}, recordCount={}, nextOffset={}, indexEntries={}",
                    baseOffset, recordCount, nextOffset, index.size());

        } catch (Exception e) {
            log.error("Error in log scan recovery", e);
            this.nextOffset = baseOffset;
        }
    }

    /**
     * Append a message record to the segment
     */
    public synchronized long append(MessageRecord record) throws IOException {
        if (!active) {
            throw new IllegalStateException("Segment is not active");
        }

        this.nextOffset = record.getOffset() + 1;
        long offset = record.getOffset();

        // Calculate CRC32
        int crc32 = calculateCRC32(record, offset);

        // Write record to log file
        int recordSize = writeRecord(record, offset, crc32);

        // Update index every 4KB
        if ((logPosition - recordSize) % 4096 < recordSize) {
            int recordStart = (int) (logPosition - recordSize);
            index.put(offset, recordStart);
            writeIndexEntry(offset, recordStart);
        }

        if (this.baseOffset == 0) {
            this.baseOffset = offset;
        }

        return record.getOffset();
    }

    /**
     * Write record to log file using FileChannel
     */
    private int writeRecord(MessageRecord record, long offset, int crc32) throws IOException {
        // Prepare data
        byte[] keyBytes = record.getMsgKey().getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = record.getData() != null ?
                record.getData().getBytes(StandardCharsets.UTF_8) : new byte[0];

        int recordSize = 8 + 4 + keyBytes.length + 1 + 4 + dataBytes.length + 8 + 4;

        // Check segment size limit
        if (logPosition + recordSize > maxSize) {
            throw new IOException("Segment full");
        }

        // Build record in buffer
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putLong(offset);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.put((byte) record.getEventType().getCode());
        buffer.putInt(dataBytes.length);
        if (dataBytes.length > 0) {
            buffer.put(dataBytes);
        }
        buffer.putLong(record.getCreatedAt().toEpochMilli());
        buffer.putInt(crc32);
        buffer.flip();

        // Write to file
        int written = 0;
        while (buffer.hasRemaining()) {
            written += logChannel.write(buffer, logPosition + written);
        }

        logPosition += written;

        // Force to disk for durability
        logChannel.force(false);

        return recordSize;
    }

    /**
     * Write index entry to index file
     */
    private void writeIndexEntry(long offset, int position) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            buffer.putLong(offset);
            buffer.putInt(position);
            buffer.flip();

            long indexPos = index.headMap(offset, true).size() * INDEX_ENTRY_SIZE;
            indexChannel.write(buffer, indexPos);
            indexChannel.force(false);
        } catch (IOException e) {
            log.error("Failed to write index entry", e);
        }
    }

    /**
     * Read a record at the given offset or next available offset
     */
    public MessageRecord read(long offset) throws IOException {
        int position = findPosition(offset);

        if (position == -1) {
            log.debug("No record found at or after offset {} in segment", offset);
            return null;
        }

        return readRecordAt(position);
    }

    /**
     * Find file position for given offset using in-memory index
     */
    private int findPosition(long offset) {
        // Use in-memory index for fast lookup
        Long floorKey = index.floorKey(offset);

        if (floorKey == null) {
            // No index entry, scan from start
            return scanToOffset(0, offset);
        }

        int startPosition = index.get(floorKey);
        return scanToOffset(startPosition, offset);
    }

    /**
     * Scan forward from position to find exact offset or next available
     */
    private int scanToOffset(int startPosition, long targetOffset) {
        try {
            long position = startPosition;
            ByteBuffer buffer = ByteBuffer.allocate(20);

            while (position < logPosition) {
                // Read offset
                buffer.clear();
                buffer.limit(8);
                int bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 8) break;
                buffer.flip();
                long recordOffset = buffer.getLong();

                if (recordOffset == targetOffset) {
                    return (int) position;
                }

                if (recordOffset > targetOffset) {
                    log.debug("Auto-advancing: Found offset {} (requested >= {})", recordOffset, targetOffset);
                    return (int) position;
                }

                // Skip to next record
                position += 8;

                // Read key length
                buffer.clear();
                buffer.limit(4);
                bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 4) break;
                buffer.flip();
                int keyLen = buffer.getInt();
                position += 4 + keyLen;

                // Skip event type
                position += 1;

                // Read data length
                buffer.clear();
                buffer.limit(4);
                bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 4) break;
                buffer.flip();
                int dataLen = buffer.getInt();
                position += 4 + dataLen;

                // Skip timestamp + crc32
                position += 12;
            }

            return -1;
        } catch (IOException e) {
            log.error("Error scanning to offset", e);
            return -1;
        }
    }

    /**
     * Read record at specific file position using FileChannel
     */
    private MessageRecord readRecordAt(int position) throws IOException {
        MessageRecord record = new MessageRecord();
        long pos = position;
        ByteBuffer buffer = readBuffer.get();

        // Read offset
        buffer.clear();
        buffer.limit(8);
        logChannel.read(buffer, pos);
        buffer.flip();
        long offset = buffer.getLong();
        record.setOffset(offset);
        pos += 8;

        // Read key
        buffer.clear();
        buffer.limit(4);
        logChannel.read(buffer, pos);
        buffer.flip();
        int keyLen = buffer.getInt();
        pos += 4;

        byte[] keyBytes = new byte[keyLen];
        buffer.clear();
        buffer.limit(keyLen);
        logChannel.read(buffer, pos);
        buffer.flip();
        buffer.get(keyBytes);
        record.setMsgKey(new String(keyBytes, StandardCharsets.UTF_8));
        pos += keyLen;

        // Read event type
        buffer.clear();
        buffer.limit(1);
        logChannel.read(buffer, pos);
        buffer.flip();
        byte eventTypeCode = buffer.get();
        record.setEventType(EventType.fromCode((char) eventTypeCode));
        pos += 1;

        // Read data
        buffer.clear();
        buffer.limit(4);
        logChannel.read(buffer, pos);
        buffer.flip();
        int dataLen = buffer.getInt();
        pos += 4;

        if (dataLen > 0) {
            byte[] dataBytes = new byte[dataLen];
            ByteBuffer dataBuffer = ByteBuffer.wrap(dataBytes);
            logChannel.read(dataBuffer, pos);
            record.setData(new String(dataBytes, StandardCharsets.UTF_8));
            pos += dataLen;
        }

        // Read timestamp
        buffer.clear();
        buffer.limit(8);
        logChannel.read(buffer, pos);
        buffer.flip();
        long createdAtMillis = buffer.getLong();
        record.setCreatedAt(Instant.ofEpochMilli(createdAtMillis));
        pos += 8;

        // Read and verify CRC32
        buffer.clear();
        buffer.limit(4);
        logChannel.read(buffer, pos);
        buffer.flip();
        int storedCrc32 = buffer.getInt();
        record.setCrc32(storedCrc32);

        int calculatedCrc32 = calculateCRC32(record, offset);
        if (storedCrc32 != calculatedCrc32) {
            throw new IOException("CRC32 mismatch for offset " + offset);
        }

        return record;
    }

    /**
     * Calculate CRC32 checksum
     */
    private int calculateCRC32(MessageRecord record, long offset) {
        CRC32 crc = new CRC32();
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

        try {
            logChannel.force(true);
            indexChannel.force(true);
        } catch (IOException e) {
            log.error("Error forcing channels to disk", e);
        }

        log.info("Sealed segment with baseOffset={}, records={}", baseOffset, nextOffset - baseOffset);
    }

    /**
     * Zero-copy batch read: Get FileRegion for direct file-to-network transfer
     * This is the KEY method for Kafka-style zero-copy using sendfile() syscall
     */
    public BatchFileRegion getBatchFileRegion(long startOffset, int maxRecords, long maxBytes) throws IOException {
        // Find starting position
        int startPosition = findPosition(startOffset);

        if (startPosition == -1) {
            return new BatchFileRegion(null, null, 0, 0, 0, startOffset);
        }

        // Scan forward to determine batch size
        BatchInfo batchInfo = scanBatch(startPosition, startOffset, maxRecords, maxBytes);

        if (batchInfo.recordCount == 0) {
            return new BatchFileRegion(null, null, 0, 0, 0, startOffset);
        }

        // Open a separate READ-ONLY FileChannel for zero-copy transfer
        // This prevents interference with the write channel (Kafka-style)
        // The FileRegion will close this channel when transfer completes
        FileChannel readChannel = FileChannel.open(logPath, StandardOpenOption.READ);

        // Create FileRegion for zero-copy transfer
        // This is what enables sendfile() syscall - NO heap allocations!
        FileRegion fileRegion = new DefaultFileRegion(readChannel, startPosition, batchInfo.totalBytes);

        return new BatchFileRegion(
            fileRegion,
            readChannel,  // Pass the read channel (will be closed by FileRegion)
            batchInfo.recordCount,
            batchInfo.totalBytes,
            batchInfo.lastOffset,
            startPosition
        );
    }

    /**
     * Scan forward to determine batch boundaries using FileChannel
     */
    private BatchInfo scanBatch(int startPosition, long targetOffset, int maxRecords, long maxBytes) {
        try {
            long position = startPosition;
            int recordCount = 0;
            long cumulativeBytes = 0;
            long lastOffset = targetOffset - 1;
            ByteBuffer buffer = ByteBuffer.allocate(20);

            while (position < logPosition && recordCount < maxRecords) {
                long recordStart = position;

                // Read offset
                buffer.clear();
                buffer.limit(8);
                int bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 8) break;
                buffer.flip();
                long recordOffset = buffer.getLong();
                position += 8;

                // Read key length
                buffer.clear();
                buffer.limit(4);
                bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 4) break;
                buffer.flip();
                int keyLen = buffer.getInt();
                position += 4 + keyLen;

                // Skip event type
                position += 1;

                // Read data length
                buffer.clear();
                buffer.limit(4);
                bytesRead = logChannel.read(buffer, position);
                if (bytesRead < 4) break;
                buffer.flip();
                int dataLen = buffer.getInt();
                position += 4 + dataLen;

                // Skip timestamp + crc32
                position += 12;

                // Calculate record size
                long recordSize = position - recordStart;

                // Check cumulative size limit
                if (cumulativeBytes + recordSize > maxBytes && recordCount > 0) {
                    break;
                }

                cumulativeBytes += recordSize;
                recordCount++;
                lastOffset = recordOffset;
            }

            return new BatchInfo(recordCount, cumulativeBytes, lastOffset);
        } catch (IOException e) {
            log.error("Error scanning batch", e);
            return new BatchInfo(0, 0, targetOffset - 1);
        }
    }

    /**
     * Close the segment and release resources
     */
    public void close() throws IOException {
        seal();
        logChannel.close();
        indexChannel.close();
    }

    /**
     * Metadata about a batch for zero-copy transfer
     */
    public static class BatchFileRegion {
        public final FileRegion fileRegion;  // null if no records
        public final FileChannel fileChannel;  // For reading batch data
        public final int recordCount;
        public final long totalBytes;
        public final long lastOffset;
        public final long filePosition;

        public BatchFileRegion(FileRegion fileRegion, FileChannel fileChannel, int recordCount, long totalBytes,
                              long lastOffset, long filePosition) {
            this.fileRegion = fileRegion;
            this.fileChannel = fileChannel;
            this.recordCount = recordCount;
            this.totalBytes = totalBytes;
            this.lastOffset = lastOffset;
            this.filePosition = filePosition;
        }
    }

    /**
     * Internal class for batch scanning
     */
    private static class BatchInfo {
        final int recordCount;
        final long totalBytes;
        final long lastOffset;

        BatchInfo(int recordCount, long totalBytes, long lastOffset) {
            this.recordCount = recordCount;
            this.totalBytes = totalBytes;
            this.lastOffset = lastOffset;
        }
    }

    // Getters
    public long getBaseOffset() {
        return baseOffset;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public int getSize() {
        return (int) logPosition;
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
