package com.messaging.storage.segment;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.exception.StorageException;
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

    // Unified format constants
    private static final int INDEX_ENTRY_SIZE = 20; // offset:8 + logPosition:4 + recordSize:4 + crc32:4
    private static final int WRITE_BUFFER_SIZE = 16 * 1024; // 16KB reusable buffer
    private static final int FILE_HEADER_SIZE = 6; // magic:4 + version:2
    private static final byte[] LOG_MAGIC = "MLOG".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INDEX_MAGIC = "MIDX".getBytes(StandardCharsets.UTF_8);
    private static final short FORMAT_VERSION = 1;

    private long baseOffset;
    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;
    private final long maxSize;

    // Topic and partition context for rich exception logging
    private final String topic;
    private final int partition;

    /**
     * In-memory index: offset -> file position
     *
     * @deprecated This in-memory index is kept only for backward compatibility during segment recovery.
     * New reads use file-based binary search via findIndexEntryForOffset(), which provides:
     * - O(log n) lookup time (similar to this ConcurrentSkipListMap)
     * - O(1) memory usage (vs O(n) for this map)
     * - Thread-safe positioned reads without synchronization
     *
     * Memory savings: ~50MB per 1M records (8 bytes key + 4 bytes value + ~40 bytes overhead per entry).
     * With binary search: ~400 bytes per lookup (20 iterations × 20 bytes per index entry).
     *
     * Will be completely removed in next major version after successful rollout.
     */
    @Deprecated
    private final ConcurrentSkipListMap<Long, Integer> index;

    // Thread-local buffers for reads (to avoid allocation)
    private final ThreadLocal<ByteBuffer> readBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(WRITE_BUFFER_SIZE));

    private long nextOffset;
    private long logPosition; // Current write position in log file
    private long recordCount; // Number of records actually in this segment (for dense indexing)
    private boolean active;

    public Segment(Path logPath, Path indexPath, long baseOffset, long maxSize, String topic, int partition) throws StorageException {
        this.baseOffset = baseOffset;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.nextOffset = baseOffset;
        this.logPosition = 0;
        this.recordCount = 0;
        this.active = true;
        this.maxSize = maxSize;
        this.topic = topic;
        this.partition = partition;
        this.index = new ConcurrentSkipListMap<>();

        try {
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

            // Initialize files with headers if new, or validate existing headers
            initializeOrValidateHeaders();

            // Initialize write position from file size
            this.logPosition = logChannel.size();

            log.info("Created segment with baseOffset={} at {}, logSize={}MB",
                    baseOffset, logPath, logPosition / (1024 * 1024));

            // If segment has existing data, recover index and nextOffset
            if (logPosition > FILE_HEADER_SIZE) {
                recoverIndex();
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                "Failed to open segment files: " + logPath, e)
                .withTopic(topic)
                .withPartition(partition)
                .withSegmentPath(logPath.toString());
        }
    }

    /**
     * Initialize new files with headers or validate existing headers
     */
    private void initializeOrValidateHeaders() throws StorageException {
        try {
            long logSize = logChannel.size();
            long indexSize = indexChannel.size();

            if (logSize == 0 && indexSize == 0) {
                // New files - write headers
                writeFileHeaders();
            } else if (logSize >= FILE_HEADER_SIZE && indexSize >= FILE_HEADER_SIZE) {
                // Existing files - validate headers
                validateFileHeaders();
            } else {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.corruption("Corrupted segment files: log=" + logSize + "bytes, index=" + indexSize + "bytes")
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(logPath.toString())
                        .withContext("logSize", logSize)
                        .withContext("indexSize", indexSize));
            }
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to initialize or validate segment headers", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString()));
        }
    }

    /**
     * Write file headers to new segment files
     */
    private void writeFileHeaders() throws MessagingException {
        try {
            // Write log header: [MLOG][version]
            ByteBuffer logHeader = ByteBuffer.allocate(FILE_HEADER_SIZE);
            logHeader.put(LOG_MAGIC);
            logHeader.putShort(FORMAT_VERSION);
            logHeader.flip();
            logChannel.write(logHeader, 0);

            // Write index header: [MIDX][version]
            ByteBuffer indexHeader = ByteBuffer.allocate(FILE_HEADER_SIZE);
            indexHeader.put(INDEX_MAGIC);
            indexHeader.putShort(FORMAT_VERSION);
            indexHeader.flip();
            indexChannel.write(indexHeader, 0);

            logChannel.force(false);
            indexChannel.force(false);

            log.info("Initialized new segment files with unified format headers");
        } catch (IOException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.writeFailed(topic, partition, e)
                    .withSegmentPath(logPath.toString())
                    .withContext("operation", "writeFileHeaders"));
        }
    }

    /**
     * Validate file headers for existing segment files
     */
    private void validateFileHeaders() throws StorageException {
        try {
            // Validate log header
            ByteBuffer logHeader = ByteBuffer.allocate(FILE_HEADER_SIZE);
            logChannel.read(logHeader, 0);
            logHeader.flip();

            byte[] logMagic = new byte[4];
            logHeader.get(logMagic);
            short logVersion = logHeader.getShort();

            if (!java.util.Arrays.equals(logMagic, LOG_MAGIC)) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.corruption("Invalid log file magic bytes")
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(logPath.toString())
                        .withContext("expectedMagic", "MLOG")
                        .withContext("actualMagic", new String(logMagic, StandardCharsets.UTF_8)));
            }

            if (logVersion != FORMAT_VERSION) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.corruption("Unsupported log file version")
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(logPath.toString())
                        .withContext("expectedVersion", FORMAT_VERSION)
                        .withContext("actualVersion", logVersion));
            }

            // Validate index header
            ByteBuffer indexHeader = ByteBuffer.allocate(FILE_HEADER_SIZE);
            indexChannel.read(indexHeader, 0);
            indexHeader.flip();

            byte[] indexMagic = new byte[4];
            indexHeader.get(indexMagic);
            short indexVersion = indexHeader.getShort();

            if (!java.util.Arrays.equals(indexMagic, INDEX_MAGIC)) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.corruption("Invalid index file magic bytes")
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(indexPath.toString())
                        .withContext("expectedMagic", "MIDX")
                        .withContext("actualMagic", new String(indexMagic, StandardCharsets.UTF_8)));
            }

            if (indexVersion != FORMAT_VERSION) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.corruption("Unsupported index file version")
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(indexPath.toString())
                        .withContext("expectedVersion", FORMAT_VERSION)
                        .withContext("actualVersion", indexVersion));
            }

            log.debug("Validated segment file headers: version {}", FORMAT_VERSION);
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to validate segment headers", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString()));
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
     * Recover index from index file (unified format).
     * Index entry: [offset:8][logPosition:4][recordSize:4][crc32:4]
     *
     * NOTE: This method still populates the deprecated in-memory index for backward compatibility.
     * However, new reads no longer use this index - they use file-based binary search instead.
     * This ensures segment recovery works during transition period.
     * Will be simplified in future version to only recover metadata (nextOffset, recordCount).
     */
    private void recoverFromIndexFile(long indexSize) throws MessagingException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            long position = FILE_HEADER_SIZE;  // Skip file header
            long highestOffset = baseOffset - 1;
            // B7-2 fix: track where valid log data ends according to the index.
            // If the log file is larger, the extra bytes are a partial write from a crash
            // between log-write and index-write — they must be truncated.
            long expectedLogEnd = FILE_HEADER_SIZE;

            while (position < indexSize) {
                buffer.clear();
                int bytesRead = indexChannel.read(buffer, position);
                if (bytesRead < INDEX_ENTRY_SIZE) break;

                buffer.flip();
                long offset = buffer.getLong();
                int logPos = buffer.getInt();
                int recordSize = buffer.getInt();
                int crc = buffer.getInt();

                index.put(offset, logPos);

                if (offset > highestOffset) {
                    highestOffset = offset;
                }
                // Track the end of the last fully-indexed record in the log file
                long entryEnd = logPos + (long) recordSize;
                if (entryEnd > expectedLogEnd) {
                    expectedLogEnd = entryEnd;
                }

                position += INDEX_ENTRY_SIZE;
            }

            this.nextOffset = highestOffset + 1;
            this.recordCount = index.size(); // Set recordCount for dense indexing

            // B7-2 fix: truncate log file if it has bytes beyond what the index knows about.
            // This removes any partial record written before a crash that prevented the
            // corresponding index entry from being written and fsynced.
            long actualLogSize = logChannel.size();
            if (actualLogSize > expectedLogEnd) {
                log.warn("B7-2 crash recovery: log file has {} orphaned bytes beyond last indexed record " +
                         "(logSize={}, expectedLogEnd={}). Truncating to remove partial write.",
                         actualLogSize - expectedLogEnd, actualLogSize, expectedLogEnd);
                logChannel.truncate(expectedLogEnd);
                this.logPosition = expectedLogEnd;
            }

            log.info("Recovered index with {} entries, nextOffset={}, recordCount={}", index.size(), nextOffset, recordCount);
        } catch ( IOException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to recover index from index file", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(indexPath.toString())
                    .withContext("indexSize", indexSize));
        }
    }

    /**
     * Fallback: Recover by scanning the entire log file (UNIFIED FORMAT - NOT SUPPORTED)
     *
     * In the unified format, log files don't contain offsets, so we can't scan them
     * to rebuild the index. The index file is mandatory for recovery.
     */
    private void recoverFromLogScan() {
        log.error("Log scan recovery not supported in unified format - index file is mandatory!");
        log.error("Segment at baseOffset={} has corrupted or missing index file", baseOffset);
        log.error("Solution: Delete this segment and re-ingest data from source");

        // Use RuntimeException for fatal unrecoverable error (unchecked exception appropriate here)
        // This will propagate up to caller and cause broker initialization failure
        throw new RuntimeException("Cannot recover unified format segment without index file. " +
                "topic=" + topic + " partition=" + partition + " baseOffset=" + baseOffset +
                " segmentPath=" + logPath + ". Delete segment files and re-ingest data.");
    }

    /**
     * Append a message record to the segment
     */
    public synchronized long append(MessageRecord record) throws MessagingException {
        if (!active) {
            throw ExceptionLogger.logAndThrow(log,
                new StorageException(ErrorCode.STORAGE_WRITE_FAILED, "Segment is not active")
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString())
                    .withContext("baseOffset", baseOffset));
        }

        long offset = record.getOffset();

        // baseOffset is IMMUTABLE - set only during construction
        // DO NOT update it here, as it would corrupt loaded segments
        // The baseOffset is set in the constructor based on:
        // 1. Filename offset (for loaded segments)
        // 2. Explicit parameter (for new segments)

        this.nextOffset = offset + 1;

        // Write record using unified format (split log/index)
        writeRecord(record, offset);

        return offset;
    }

    /**
     * Write record using unified format: split log/index writes
     * Log: [keyLen:4][key][eventType:1][dataLen:4][data][timestamp:8]
     * Index: [offset:8][logPosition:4][recordSize:4][crc32:4]
     */
    private void writeRecord(MessageRecord record, long offset) throws MessagingException {
        try {
            // Prepare data
            byte[] keyBytes = record.getMsgKey().getBytes(StandardCharsets.UTF_8);
            byte[] dataBytes = record.getData() != null ?
                    record.getData().getBytes(StandardCharsets.UTF_8) : new byte[0];

            // Calculate log record size (NO offset, NO CRC in log)
            int logRecordSize = 4 + keyBytes.length + 1 + 4 + dataBytes.length + 8;

            // Check segment size limit
            if (logPosition + logRecordSize > maxSize) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.writeFailed(topic, partition, new IOException("Segment full"))
                        .withSegmentPath(logPath.toString())
                        .withContext("logPosition", logPosition)
                        .withContext("recordSize", logRecordSize)
                        .withContext("maxSize", maxSize));
            }

            // 1. Write to LOG file (message data only)
            ByteBuffer logBuffer = ByteBuffer.allocate(logRecordSize);
            logBuffer.putInt(keyBytes.length);          // keyLen: 4 bytes
            logBuffer.put(keyBytes);                    // key: variable
            logBuffer.put((byte) record.getEventType().getCode());  // eventType: 1 byte
            logBuffer.putInt(dataBytes.length);         // dataLen: 4 bytes
            if (dataBytes.length > 0) {
                logBuffer.put(dataBytes);               // data: variable
            }
            logBuffer.putLong(record.getCreatedAt().toEpochMilli());  // timestamp: 8 bytes
            logBuffer.flip();

            // Save log position BEFORE writing
            long recordLogPosition = logPosition;

            log.info("DEBUG writeRecord: path={}, offset={}, logPosition={}, recordSize={}, baseOffset={}",
                     logPath, offset, logPosition, logRecordSize, baseOffset);

            // Write log data
            int written = 0;
            while (logBuffer.hasRemaining()) {
                written += logChannel.write(logBuffer, logPosition + written);
            }
            logPosition += written;

            log.info("DEBUG writeRecord AFTER write: newLogPosition={}, written={}", logPosition, written);

            // 2. Calculate CRC32 from log data
            logBuffer.rewind();
            CRC32 crc = new CRC32();
            crc.update(logBuffer);
            int crc32Value = (int) crc.getValue();

            // 3. Write to INDEX file (metadata: offset, position, size, CRC)
            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            indexBuffer.putLong(offset);                        // offset: 8 bytes
            indexBuffer.putInt((int) recordLogPosition);        // logPosition: 4 bytes
            indexBuffer.putInt(logRecordSize);                  // recordSize: 4 bytes
            indexBuffer.putInt(crc32Value);                     // crc32: 4 bytes
            indexBuffer.flip();

            // Write index entry using DENSE indexing based on recordCount
            // This creates sequential index entries regardless of actual offset values
            long indexPosition = FILE_HEADER_SIZE + (recordCount * INDEX_ENTRY_SIZE);
            indexChannel.write(indexBuffer, indexPosition);

            // Increment record count for next write
            recordCount++;

            // Update in-memory index (offset -> log position)
            // DEPRECATED: Commented out - new reads use file-based binary search
            // index.put(offset, (int) recordLogPosition);

            // Force to disk for durability
            logChannel.force(false);
            indexChannel.force(false);
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.writeFailed(topic, partition, e)
                    .withSegmentPath(logPath.toString())
                    .withContext("offset", offset)
                    .withContext("operation", "writeRecord"));
        }
    }

    /**
     * Read a record at the given offset or next available offset
     */
    public MessageRecord read(long offset) throws MessagingException {
        if (offset < baseOffset || offset >= nextOffset) {
            log.debug("Offset {} out of range [{}, {})", offset, baseOffset, nextOffset);
            return null;
        }

        return readRecordAtOffset(offset);
    }


    /**
     * Read record at specific file position using index-based lookup (unified format)
     * Index entry: [offset:8][logPosition:4][recordSize:4][crc32:4]
     * Log record: [keyLen:4][key][eventType:1][dataLen:4][data][timestamp:8]
     */
    private MessageRecord readRecordAt(int position) throws MessagingException {
        // For unified format, 'position' is the offset, not the file position
        // We need to read from the index first
        // This is a fallback for the old scan-based lookup
        // In the new format, we should use readRecordAtOffset() directly
        throw ExceptionLogger.logAndThrow(log,
            new StorageException(ErrorCode.STORAGE_READ_FAILED, "readRecordAt(position) deprecated - use readRecordAtOffset(offset)")
                .withTopic(topic)
                .withPartition(partition)
                .withSegmentPath(logPath.toString())
                .withContext("deprecatedMethod", "readRecordAt"));
    }

    /**
     * Binary search through index file to find entry for targetOffset.
     * Finds first entry where offset >= targetOffset, gracefully handling offset gaps.
     *
     * This method implements Kafka-style file-based index lookup with O(log n) complexity
     * and O(1) memory usage. It uses positioned reads which are thread-safe without
     * synchronization, allowing concurrent reads from multiple consumers.
     *
     * @param targetOffset The offset to search for
     * @return IndexEntry for first offset >= targetOffset, or null if not found
     * @throws StorageException If index file read fails
     */
    private IndexEntry findIndexEntryForOffset(long targetOffset) throws MessagingException {
        try {
            long indexSize = indexChannel.size();
            long numEntries = (indexSize - FILE_HEADER_SIZE) / INDEX_ENTRY_SIZE;

            // Edge cases: empty segment or offset before segment start
            if (numEntries == 0) {
                log.debug("Empty segment: baseOffset={}, no entries", baseOffset);
                return null;
            }

            if (targetOffset < baseOffset) {
                log.debug("Offset {} before segment baseOffset={}", targetOffset, baseOffset);
                return null;
            }

            // Binary search variables
            long left = 0;                    // First entry index (0-based)
            long right = numEntries - 1;      // Last entry index
            IndexEntry result = null;         // Best match so far (first offset >= target)

            // Allocate buffer for reading index entries (20 bytes per entry)
            ByteBuffer searchBuffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);

            while (left <= right) {
                long mid = left + (right - left) / 2;

                // Calculate file position for this entry (DENSE indexing)
                long filePosition = FILE_HEADER_SIZE + (mid * INDEX_ENTRY_SIZE);

                // Positioned read - thread-safe, doesn't modify channel position
                searchBuffer.clear();
                int bytesRead = indexChannel.read(searchBuffer, filePosition);

                if (bytesRead < INDEX_ENTRY_SIZE) {
                    // Incomplete entry (shouldn't happen unless file is corrupted)
                    log.warn("Incomplete index entry at position {}, bytes read: {}",
                             filePosition, bytesRead);
                    break;
                }

                searchBuffer.flip();

                // Parse index entry: [offset:8][logPos:4][size:4][crc32:4]
                long entryOffset = searchBuffer.getLong();
                int logPosition = searchBuffer.getInt();
                int recordSize = searchBuffer.getInt();
                int crc32 = searchBuffer.getInt();

                IndexEntry entry = new IndexEntry(entryOffset, logPosition, recordSize, crc32);

                if (entryOffset == targetOffset) {
                    // Exact match found
                    log.debug("Binary search: exact match for offset={}, logPos={}",
                              targetOffset, logPosition);
                    return entry;
                } else if (entryOffset < targetOffset) {
                    // Target is in right half
                    left = mid + 1;
                } else {
                    // entryOffset > targetOffset
                    // This could be our answer (first offset >= target) if gap exists
                    result = entry;
                    right = mid - 1; // Continue looking for closer match in left half
                }
            }

            if (result != null) {
                log.debug("Binary search: offset gap detected, requested={}, found={}, segment={}",
                          targetOffset, result.offset, baseOffset);
            } else {
                log.debug("Binary search: offset {} beyond segment end (nextOffset={})",
                          targetOffset, nextOffset);
            }

            return result;
        } catch (IOException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.readFailed(topic, partition, targetOffset, e)
                    .withSegmentPath(indexPath.toString())
                    .withContext("operation", "findIndexEntryForOffset"));
        }
    }

    /**
     * Read record by offset using binary search-based lookup.
     * Handles offset gaps gracefully by finding exact match or returning null.
     *
     * NEW IMPLEMENTATION: Uses file-based binary search instead of arithmetic calculation.
     * This fixes the bug where SPARSE indexing assumption (offset - baseOffset) failed
     * when offsets had gaps (e.g., 100202, 110000, 150000).
     */
    private MessageRecord readRecordAtOffset(long offset) throws MessagingException {
        try {
            // 1. Binary search for index entry
            IndexEntry entry = findIndexEntryForOffset(offset);

            if (entry == null) {
                log.debug("No record found at or after offset {} in segment baseOffset={}",
                          offset, baseOffset);
                return null; // No data at or after this offset
            }

            // 2. Handle offset gaps - if exact match not found, return null
            // This triggers SegmentManager to continue traversal to next segment
            if (entry.offset != offset) {
                log.debug("Offset gap detected: requested={}, found={}, segment baseOffset={}",
                          offset, entry.offset, baseOffset);
                return null; // Trigger SegmentManager traversal
            }

            // 3. Read from log file using entry metadata
            ByteBuffer logBuffer = ByteBuffer.allocate(entry.recordSize);
            int bytesRead = logChannel.read(logBuffer, entry.logPosition);

            if (bytesRead < entry.recordSize) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.readFailed(topic, partition, offset,
                        new IOException("Incomplete read: expected " + entry.recordSize + ", got " + bytesRead))
                        .withSegmentPath(logPath.toString())
                        .withContext("logPosition", entry.logPosition)
                        .withContext("expectedBytes", entry.recordSize)
                        .withContext("actualBytes", bytesRead));
            }
            logBuffer.flip();

            // 4. Validate CRC32
            logBuffer.mark();
            CRC32 crc = new CRC32();
            crc.update(logBuffer);
            int calculatedCrc = (int) crc.getValue();

            if (calculatedCrc != entry.crc32) {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.crcMismatch("CRC32 validation failed", offset)
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(logPath.toString())
                        .withContext("expectedCrc", entry.crc32)
                        .withContext("actualCrc", calculatedCrc));
            }

            logBuffer.reset();

            // 5. Parse message data from log (same parsing logic as before)
            MessageRecord record = new MessageRecord();
            record.setOffset(entry.offset); // Use actual offset from index

            // Read key
            int keyLen = logBuffer.getInt();
            byte[] keyBytes = new byte[keyLen];
            logBuffer.get(keyBytes);
            record.setMsgKey(new String(keyBytes, StandardCharsets.UTF_8));

            // Read event type
            byte eventTypeCode = logBuffer.get();
            record.setEventType(EventType.fromCode((char) eventTypeCode));

            // Read data
            int dataLen = logBuffer.getInt();
            if (dataLen > 0) {
                byte[] dataBytes = new byte[dataLen];
                logBuffer.get(dataBytes);
                record.setData(new String(dataBytes, StandardCharsets.UTF_8));
            }

            // Read timestamp
            long createdAtMillis = logBuffer.getLong();
            record.setCreatedAt(Instant.ofEpochMilli(createdAtMillis));

            return record;
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.readFailed(topic, partition, offset, e)
                    .withSegmentPath(logPath.toString())
                    .withContext("operation", "readRecordAtOffset"));
        }
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
     * Zero-copy batch read using binary search for first entry, then sequential scan.
     * Handles offset gaps gracefully by scanning actual index entries.
     *
     * NEW IMPLEMENTATION: Uses file-based binary search to find starting point,
     * then sequential scan through index entries to accumulate batch.
     * This fixes the bug where SPARSE indexing (currentOffset++) failed with offset gaps.
     *
     * Index entry format: [offset:8][logPosition:4][recordSize:4][crc32:4] = 20 bytes
     */
    public BatchFileRegion getBatchFileRegion(long startOffset, long maxBytes) throws MessagingException {
        if (startOffset < baseOffset || startOffset >= nextOffset) {
            log.debug("Offset {} outside segment range [{}, {})", startOffset, baseOffset, nextOffset);
            return new BatchFileRegion(null, null, 0, 0, 0, startOffset);
        }

        try {
            // 1. Binary search to find first entry >= startOffset
            IndexEntry firstEntry = findIndexEntryForOffset(startOffset);

            if (firstEntry == null) {
                log.debug("No entries found at or after offset {} in segment baseOffset={}",
                          startOffset, baseOffset);
                return new BatchFileRegion(null, null, 0, 0, 0, startOffset);
            }

            // 2. Sequential scan from first entry to accumulate batch
            // We already have the first entry from binary search, so we can start accumulating
            long indexSize = indexChannel.size();
            int recordCount = 0;
            long firstLogPosition = firstEntry.logPosition;
            long lastLogPosition = firstEntry.logPosition;  // Track last record's position
            int lastRecordSize = firstEntry.recordSize;      // Track last record's size
            long lastOffset = firstEntry.offset;

            log.debug("Starting batch accumulation: startOffset={}, firstEntry.offset={}, firstEntry.logPosition={}, maxBytes={}",
                      startOffset, firstEntry.offset, firstEntry.logPosition, maxBytes);

            // Start by adding the first entry we found via binary search
            recordCount++;

            log.debug("Added first entry to batch: offset={}, recordSize={}, logPosition={}, recordCount={}",
                      firstEntry.offset, firstEntry.recordSize, firstEntry.logPosition, recordCount);

            // Now scan forward from the next index entry to accumulate more records
            // We need to find where firstEntry is in the index, then continue from there
            long currentIndexPos = FILE_HEADER_SIZE;
            ByteBuffer scanBuffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            boolean foundFirstEntry = false;

            // Sequential scan through index file
            while (currentIndexPos < indexSize) {
                scanBuffer.clear();
                int bytesRead = indexChannel.read(scanBuffer, currentIndexPos);

                if (bytesRead < INDEX_ENTRY_SIZE) {
                    log.debug("Reached end of index file at position {}", currentIndexPos);
                    break; // End of index
                }

                scanBuffer.flip();
                long offset = scanBuffer.getLong();
                int logPosition = scanBuffer.getInt();
                int recordSize = scanBuffer.getInt();
                int crc32 = scanBuffer.getInt();

                // Skip until we find our first entry
                if (!foundFirstEntry) {
                    if (offset == firstEntry.offset) {
                        foundFirstEntry = true;
                        // Skip this entry as we already added it above
                    }
                    currentIndexPos += INDEX_ENTRY_SIZE;
                    continue;
                }

                // Calculate what the batch size would be if we add this record
                // totalBytes = (endOfLastRecord) - firstLogPosition
                long batchSizeWithThisRecord = (logPosition + recordSize) - firstLogPosition;

                // Check if adding this record would exceed maxBytes
                if (batchSizeWithThisRecord > maxBytes && recordCount > 0) {
                    log.debug("Batch size limit reached: batchSize={}, maxBytes={}", batchSizeWithThisRecord, maxBytes);
                    break; // Would exceed size limit
                }

                lastLogPosition = logPosition;
                lastRecordSize = recordSize;
                lastOffset = offset;
                recordCount++;
                currentIndexPos += INDEX_ENTRY_SIZE;

                log.debug("Added to batch: offset={}, recordSize={}, logPosition={}, recordCount={}",
                          offset, recordSize, logPosition, recordCount);
            }

            if (recordCount == 0) {
                log.debug("No records accumulated for batch starting at offset {}", startOffset);
                return new BatchFileRegion(null, null, 0, 0, 0, startOffset);
            }

            // 3. Create FileRegion for zero-copy transfer (Kafka-style sendfile)
            // Calculate actual batch size: from first record start to last record end
            long totalBytes = (lastLogPosition + lastRecordSize) - firstLogPosition;

            // Open separate READ-ONLY channel to avoid interference with writes
            FileChannel readChannel = FileChannel.open(logPath, StandardOpenOption.READ);
            FileRegion fileRegion = new DefaultFileRegion(readChannel, firstLogPosition, totalBytes);

            log.debug("Created zero-copy batch: offset={}-{}, bytes={}, records={}, firstLogPos={}",
                      startOffset, lastOffset, totalBytes, recordCount, firstLogPosition);

            return new BatchFileRegion(
                fileRegion,
                readChannel,  // Will be closed by FileRegion
                recordCount,
                totalBytes,
                lastOffset,
                firstLogPosition
            );
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.readFailed(topic, partition, startOffset, e)
                    .withSegmentPath(logPath.toString())
                    .withContext("operation", "getBatchFileRegion")
                    .withContext("maxBytes", maxBytes));
        }
    }


    /**
     * Close the segment and release resources
     */
    public void close() throws MessagingException {
        try {
            seal();
            logChannel.close();
            indexChannel.close();
        } catch (IOException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to close segment", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString())
                    .withContext("operation", "close"));
        }
    }

    /**
     * Helper class representing a parsed index entry.
     * Used during binary search through index file for offset lookups.
     */
    private static class IndexEntry {
        final long offset;      // Message offset (from cloud-server, can have gaps)
        final int logPosition;  // Position in log file where record starts
        final int recordSize;   // Size of log record in bytes
        final int crc32;        // CRC32 checksum for validation

        IndexEntry(long offset, int logPosition, int recordSize, int crc32) {
            this.offset = offset;
            this.logPosition = logPosition;
            this.recordSize = recordSize;
            this.crc32 = crc32;
        }
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
