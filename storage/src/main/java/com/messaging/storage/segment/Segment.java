package com.messaging.storage.segment;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.DeliveryBatch;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
/**
 * Represents a single segment file using FileChannel (no memory mapping)
 * Enables true zero-copy via FileChannel.transferTo() (sendfile).
 * BatchFileRegion implements DeliveryBatch — carries both routing metadata and file bytes.
 */
public class Segment {
    private static final Logger log = LoggerFactory.getLogger(Segment.class);

    // Segment format constants (index v2 = current 16-byte, index v1 = legacy 20-byte with CRC)
    private static final int INDEX_ENTRY_SIZE = 16;        // v2: offset:8 + logPosition:4 + recordSize:4
    private static final int INDEX_ENTRY_SIZE_LEGACY = 20; // v1 (legacy): same as v2 + crc32:4
    private static final int WRITE_BUFFER_SIZE = 16 * 1024; // 16KB reusable buffer
    private static final int FILE_HEADER_SIZE = 6; // magic:4 + version:2
    private static final byte[] LOG_MAGIC = "MLOG".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INDEX_MAGIC = "MIDX".getBytes(StandardCharsets.UTF_8);
    private static final short FORMAT_VERSION = 1;          // Log file version (unchanged)
    private static final short INDEX_FORMAT_VERSION = 2;    // New index format (16-byte entries)
    private static final short INDEX_FORMAT_VERSION_LEGACY = 1; // Old index format (20-byte entries with CRC)

    private long baseOffset;
    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;
    private final long maxSize;

    // Topic and partition context for rich exception logging
    private final String topic;
    private final int partition;

    // Thread-local buffers for reads (to avoid allocation)
    private final ThreadLocal<ByteBuffer> readBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(WRITE_BUFFER_SIZE));

    // volatile: written under synchronized append/seal but read lock-free by delivery threads
    // (watermark gates, getBatchFileRegion range checks). Plain fields had no JMM visibility
    // guarantee and risk torn 64-bit reads on 32-bit JVMs.
    private volatile long nextOffset;
    private volatile long logPosition; // Current write position in log file
    private volatile long recordCount; // Number of records actually in this segment (for dense indexing)
    private volatile boolean active;
    private final int effectiveIndexEntrySize; // INDEX_ENTRY_SIZE (v2) or INDEX_ENTRY_SIZE_LEGACY (v1)

    // Lease counting for zero-copy delivery: getBatchFileRegion() hands the segment's OWN
    // logChannel to Netty, and the transfer happens long after SegmentManager's read lock is
    // released. Compaction closing the channel mid-sendfile caused ClosedChannelException →
    // consumer connection resets on every compaction under load. close() now defers the
    // actual channel close until all outstanding batch leases are released (Netty always
    // calls BatchFileRegion.close() via deallocate()).
    private final java.util.concurrent.atomic.AtomicInteger batchLeases =
            new java.util.concurrent.atomic.AtomicInteger();
    private volatile boolean closeRequested;

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

            // Determine index entry size from file headers (v1=20 bytes legacy, v2=16 bytes current)
            this.effectiveIndexEntrySize = initializeOrValidateHeaders();

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
    private int initializeOrValidateHeaders() throws StorageException {
        try {
            long logSize = logChannel.size();
            long indexSize = indexChannel.size();

            if (logSize == 0 && indexSize == 0) {
                writeFileHeaders();
                return INDEX_ENTRY_SIZE; // new files always use v2 (16-byte entries)
            } else if (logSize >= FILE_HEADER_SIZE && indexSize == 0) {
                // Crash window: log header (and possibly a partial record) was written but the
                // index header never made it to disk. Validate the log header FIRST — a file
                // with garbage magic is corruption, not a crash artifact, and must still be
                // rejected. Then re-initialise the index header; recoverIndex() truncates any
                // unindexed log bytes back to the header (B7-3).
                validateLogHeader();
                log.warn("B7-3 crash recovery: index file is empty but log has {} bytes for {} — " +
                         "re-initialising index header; orphaned log bytes will be truncated",
                         logSize, indexPath);
                ByteBuffer indexHeader = ByteBuffer.allocate(FILE_HEADER_SIZE);
                indexHeader.put(INDEX_MAGIC);
                indexHeader.putShort(INDEX_FORMAT_VERSION);
                indexHeader.flip();
                indexChannel.write(indexHeader, 0);
                indexChannel.force(false);
                return INDEX_ENTRY_SIZE;
            } else if (logSize >= FILE_HEADER_SIZE && indexSize >= FILE_HEADER_SIZE) {
                return validateFileHeaders();
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
            indexHeader.putShort(INDEX_FORMAT_VERSION); // v2 = 16-byte entries (no CRC)
            indexHeader.flip();
            indexChannel.write(indexHeader, 0);

            logChannel.force(false);
            indexChannel.force(false);

            log.info("Initialized new segment files (index format v2, 16-byte entries)");
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
    /**
     * @return the effective index entry size for this segment (16 for v2, 20 for legacy v1)
     */
    private int validateFileHeaders() throws StorageException {
        try {
            validateLogHeader();

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

            if (indexVersion == INDEX_FORMAT_VERSION_LEGACY) {
                log.warn("Legacy index format (v1, 20-byte entries) detected for {}, reading with backward compatibility", indexPath);
                return INDEX_ENTRY_SIZE_LEGACY;
            } else if (indexVersion == INDEX_FORMAT_VERSION) {
                return INDEX_ENTRY_SIZE;
            } else {
                throw ExceptionLogger.logAndThrow(log,
                    StorageException.corruption("Unsupported index file version")
                        .withTopic(topic)
                        .withPartition(partition)
                        .withSegmentPath(indexPath.toString())
                        .withContext("supportedVersions", "1 (legacy), 2 (current)")
                        .withContext("actualVersion", indexVersion));
            }
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to validate segment headers", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString()));
        }
    }

    /**
     * Validate the log file's magic bytes and format version.
     */
    private void validateLogHeader() throws StorageException {
        try {
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
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to validate log header", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString()));
        }
    }

    /**
     * Recover in-memory state (nextOffset, recordCount) from the index file.
     * @throws StorageException if the index file is missing, empty, or unreadable
     */
    private void recoverIndex() throws StorageException {
        log.info("Recovering index for segment at baseOffset={}, logPosition={}", baseOffset, logPosition);

        try {
            long indexSize = indexChannel.size();

            if (indexSize > FILE_HEADER_SIZE) {
                recoverFromIndexFile(indexSize);
            } else {
                // Empty index with non-empty log: a crash landed between the log write and the
                // first index write. Log records do not store offsets, so the orphaned bytes are
                // unrecoverable — truncate them rather than refusing to start the broker. The
                // parent will re-send anything lost (pipe offset only advances after append).
                log.warn("B7-3 crash recovery: index is empty but log has {} bytes beyond header " +
                         "for segment baseOffset={} — truncating log to header; orphaned records " +
                         "will be re-fetched from the parent",
                         logPosition - FILE_HEADER_SIZE, baseOffset);
                logChannel.truncate(FILE_HEADER_SIZE);
                this.logPosition = FILE_HEADER_SIZE;
                this.nextOffset = baseOffset;
                this.recordCount = 0;
            }
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Unexpected error recovering index", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(indexPath.toString()));
        }
    }

    /**
     * Recover segment metadata from the index file.
     * Supports both current format (v2, 16-byte entries) and legacy format (v1, 20-byte entries with CRC32).
     * Index entry v2: [offset:8][logPosition:4][recordSize:4]
     * Index entry v1: [offset:8][logPosition:4][recordSize:4][crc32:4]
     *
     * Recovers:
     * - Sets nextOffset to highest offset + 1
     * - Sets recordCount based on index entries
     * - Validates log file integrity (B7-2 crash recovery)
     * - Truncates orphaned bytes if present
     *
     * All reads use file-based binary search (findIndexEntryForOffset) - O(1) memory.
     */
    private void recoverFromIndexFile(long indexSize) throws MessagingException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(effectiveIndexEntrySize);
            long position = FILE_HEADER_SIZE;  // Skip file header
            long highestOffset = baseOffset - 1;
            // B7-2 fix: track where valid log data ends according to the index.
            // If the log file is larger, the extra bytes are a partial write from a crash
            // between log-write and index-write — they must be truncated.
            long expectedLogEnd = FILE_HEADER_SIZE;
            long actualLogSize = logChannel.size();

            while (position < indexSize) {
                buffer.clear();
                int bytesRead = indexChannel.read(buffer, position);
                if (bytesRead < effectiveIndexEntrySize) break;

                buffer.flip();
                long offset = buffer.getLong();
                int logPos = buffer.getInt();
                int recordSize = buffer.getInt();
                // CRC field (4 bytes) is present in legacy format but ignored

                // B7-4 fix (inverse of B7-2): the index entry points past the end of the log
                // file — the index write became durable while the log bytes did not. Entries
                // are written in ascending log position, so this and all later entries are
                // invalid. Stop here; the index is truncated below.
                long entryEnd = logPos + (long) recordSize;
                if (entryEnd > actualLogSize) {
                    log.warn("B7-4 crash recovery: index entry at position {} references log bytes " +
                             "[{}, {}) beyond log size {} — truncating index to last valid entry",
                             position, logPos, entryEnd, actualLogSize);
                    break;
                }

                if (offset > highestOffset) {
                    highestOffset = offset;
                }
                // Track the end of the last fully-indexed record in the log file
                if (entryEnd > expectedLogEnd) {
                    expectedLogEnd = entryEnd;
                }

                position += effectiveIndexEntrySize;
            }

            this.nextOffset = highestOffset + 1;
            // Calculate recordCount from number of index entries accepted
            this.recordCount = (int) ((position - FILE_HEADER_SIZE) / effectiveIndexEntrySize);

            // B7-4: drop index entries that reference log bytes that never became durable
            // (or a torn partial entry at the tail).
            if (position < indexSize) {
                indexChannel.truncate(position);
                indexChannel.force(false);
            }

            // B7-2 fix: truncate log file if it has bytes beyond what the index knows about.
            // This removes any partial record written before a crash that prevented the
            // corresponding index entry from being written and fsynced.
            if (actualLogSize > expectedLogEnd) {
                log.warn("B7-2 crash recovery: log file has {} orphaned bytes beyond last indexed record " +
                         "(logSize={}, expectedLogEnd={}). Truncating to remove partial write.",
                         actualLogSize - expectedLogEnd, actualLogSize, expectedLogEnd);
                logChannel.truncate(expectedLogEnd);
                this.logPosition = expectedLogEnd;
            }

            log.info("Recovered index with {} entries, nextOffset={}, recordCount={}", recordCount, nextOffset, recordCount);
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

        // Pipe offsets must be monotonically increasing within a topic. A lower offset here
        // means a duplicate or out-of-order record slipped past the ingest guard — appending
        // it would regress nextOffset and break the sorted-index invariant binary search needs.
        if (offset < nextOffset && recordCount > 0) {
            log.warn("event=segment.non_monotonic_append topic={} partition={} offset={} nextOffset={} " +
                     "— index ordering invariant at risk", topic, partition, offset, nextOffset);
        }

        // baseOffset is IMMUTABLE - set only during construction
        // DO NOT update it here, as it would corrupt loaded segments
        // The baseOffset is set in the constructor based on:
        // 1. Filename offset (for loaded segments)
        // 2. Explicit parameter (for new segments)

        // Write record using unified format (split log/index)
        writeRecord(record, offset);

        // Only advance nextOffset after a successful write — no speculative advance
        this.nextOffset = offset + 1;

        return offset;
    }

    /**
     * Write record using unified format: split log/index writes
     * Log: [keyLen:4][key][eventType:1][dataLen:4][data][timestamp:8]
     * Index: [offset:8][logPosition:4][recordSize:4]
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

            // Write log data
            int written = 0;
            while (logBuffer.hasRemaining()) {
                written += logChannel.write(logBuffer, logPosition + written);
            }
            logPosition += written;

            // 2. Write to INDEX file (metadata: offset, position, size)
            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            indexBuffer.putLong(offset);                        // offset: 8 bytes
            indexBuffer.putInt((int) recordLogPosition);        // logPosition: 4 bytes
            indexBuffer.putInt(logRecordSize);                  // recordSize: 4 bytes
            indexBuffer.flip();

            // Write index entry using DENSE indexing based on recordCount
            // This creates sequential index entries regardless of actual offset values
            long indexPosition = FILE_HEADER_SIZE + (recordCount * INDEX_ENTRY_SIZE);
            indexChannel.write(indexBuffer, indexPosition);

            // Increment record count for next write
            recordCount++;

            // No per-record fsync: forcing both channels on every append costs two fsyncs
            // per message and destroys flash (eMMC/SD) endurance on POS hardware. Durability
            // is provided by the periodic flush (SegmentManager.flushActiveSegment), the
            // force(true) on seal/close, and crash recovery (B7-2/B7-3/B7-4 truncation).
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
            long numEntries = (indexSize - FILE_HEADER_SIZE) / effectiveIndexEntrySize;

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

            // Allocate buffer for reading index entries (size depends on format)
            ByteBuffer searchBuffer = ByteBuffer.allocate(effectiveIndexEntrySize);

            while (left <= right) {
                long mid = left + (right - left) / 2;

                // Calculate file position for this entry (DENSE indexing)
                long filePosition = FILE_HEADER_SIZE + (mid * effectiveIndexEntrySize);

                // Positioned read - thread-safe, doesn't modify channel position
                searchBuffer.clear();
                int bytesRead = indexChannel.read(searchBuffer, filePosition);

                if (bytesRead < effectiveIndexEntrySize) {
                    // Incomplete entry (shouldn't happen unless file is corrupted)
                    log.warn("Incomplete index entry at position {}, bytes read: {}",
                             filePosition, bytesRead);
                    break;
                }

                searchBuffer.flip();

                // Parse index entry: [offset:8][logPos:4][size:4]
                long entryOffset = searchBuffer.getLong();
                int logPosition = searchBuffer.getInt();
                int recordSize = searchBuffer.getInt();

                IndexEntry entry = new IndexEntry(entryOffset, logPosition, recordSize, filePosition);

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

            // 2. Gap handling: findIndexEntryForOffset returns the first entry >= targetOffset.
            // If entry.offset > offset there is a gap in this segment.  We do NOT return null here
            // because null causes SegmentManager.readWithSizeLimit() to abandon this segment and
            // move to the next one — silently skipping every record that sits after the gap but
            // within this same segment.  Instead we fall through and read the record at entry.offset;
            // SegmentManager already advances currentOffset to record.getOffset()+1, so subsequent
            // iterations will skip over the gap automatically.
            if (entry.offset != offset) {
                log.debug("Offset gap: requested={}, reading nearest available={}, segment baseOffset={}",
                          offset, entry.offset, baseOffset);
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

            // 4. Parse message data from log
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

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    /**
     * Force buffered log + index bytes to disk. Called periodically by the storage engine's
     * flush scheduler (group commit) instead of fsyncing on every append.
     */
    public synchronized void flush() {
        flush(-1L);
    }

    /**
     * Force buffered bytes to disk and optionally evict already-flushed cold pages from the
     * page cache, keeping the last {@code cacheTailKeepBytes} hot (consumers deliver from
     * the tail; older pages are only re-read on catch-up or compaction).
     *
     * <p>Docker counts page cache against the container's memory limit, so on a constrained
     * POS device an active segment ingesting steadily can push the container's reported
     * memory to its cgroup limit between segment seals. The pages dropped here are clean
     * (just fsynced) — the kernel re-faults them from disk if needed.
     *
     * @param cacheTailKeepBytes hot tail to keep cached; negative disables cache eviction
     */
    public synchronized void flush(long cacheTailKeepBytes) {
        if (!active) {
            return;
        }
        try {
            logChannel.force(false);
            indexChannel.force(false);
        } catch (IOException e) {
            log.error("Periodic flush failed for segment baseOffset={} at {}", baseOffset, logPath, e);
            return;
        }

        if (cacheTailKeepBytes >= 0) {
            long coldEnd = logPosition - cacheTailKeepBytes;
            if (coldEnd > FILE_HEADER_SIZE) {
                // Index pages are deliberately kept cached — every delivery binary-searches
                // the index, and the whole index is small (16 B per record).
                NativePageCache.dropCacheRange(logChannel, 0, coldEnd);
            }
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
            NativePageCache.dropCache(logChannel);
            NativePageCache.dropCache(indexChannel);
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
     * Index entry format: [offset:8][logPosition:4][recordSize:4] = 16 bytes
     */
    public BatchFileRegion getBatchFileRegion(long startOffset, long maxBytes) throws MessagingException {
        if (startOffset < baseOffset || startOffset >= nextOffset) {
            log.debug("Offset {} outside segment range [{}, {})", startOffset, baseOffset, nextOffset);
            return new BatchFileRegion(topic, null, 0, 0L, -1L, 0L, startOffset);
        }

        try {
            // 1. Binary search to find first entry >= startOffset
            IndexEntry firstEntry = findIndexEntryForOffset(startOffset);

            if (firstEntry == null) {
                log.debug("No entries found at or after offset {} in segment baseOffset={}",
                          startOffset, baseOffset);
                return new BatchFileRegion(topic, null, 0, 0L, -1L, 0L, startOffset);
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

            // Continue the scan from the entry AFTER the one binary search located.
            // (Previously this rescanned the whole index from byte 0 to re-find firstEntry —
            // O(n) 16-byte positioned reads per batch call on large segments.)
            long currentIndexPos = firstEntry.indexFilePosition + effectiveIndexEntrySize;
            ByteBuffer scanBuffer = ByteBuffer.allocate(effectiveIndexEntrySize);

            // Sequential scan through index file
            while (currentIndexPos < indexSize) {
                scanBuffer.clear();
                int bytesRead = indexChannel.read(scanBuffer, currentIndexPos);

                if (bytesRead < effectiveIndexEntrySize) {
                    log.debug("Reached end of index file at position {}", currentIndexPos);
                    break; // End of index
                }

                scanBuffer.flip();
                long offset = scanBuffer.getLong();
                int logPosition = scanBuffer.getInt();
                int recordSize = scanBuffer.getInt();
                // CRC field (4 bytes) present in legacy format is ignored

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
                currentIndexPos += effectiveIndexEntrySize;

                log.debug("Added to batch: offset={}, recordSize={}, logPosition={}, recordCount={}",
                          offset, recordSize, logPosition, recordCount);
            }

            if (recordCount == 0) {
                log.debug("No records accumulated for batch starting at offset {}", startOffset);
                return new BatchFileRegion(topic, null, 0, 0L, -1L, 0L, startOffset);
            }

            // 3. Use the segment's own logChannel for zero-copy transfer (Kafka-style sendfile).
            // FileChannel.transferTo() uses the explicit position argument and is safe for
            // concurrent positioned reads even while the channel may still be open for writes
            // (active segment). Opening a fresh FileChannel per batch was leaking one FD per
            // batch delivery — with small segment sizes this exhausts the process FD table.
            long totalBytes = (lastLogPosition + lastRecordSize) - firstLogPosition;

            log.debug("Created zero-copy batch: offset={}-{}, bytes={}, records={}, firstLogPos={}",
                      startOffset, lastOffset, totalBytes, recordCount, firstLogPosition);

            // Lease the channel for the duration of the network transfer (released by
            // BatchFileRegion.close()); keeps a concurrent compaction close from yanking
            // the channel mid-sendfile.
            batchLeases.incrementAndGet();
            return new BatchFileRegion(topic, logChannel, recordCount, totalBytes,
                    firstEntry.offset, lastOffset, firstLogPosition, this::releaseBatchLease);
        } catch (IOException | MessagingException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.readFailed(topic, partition, startOffset, e)
                    .withSegmentPath(logPath.toString())
                    .withContext("operation", "getBatchFileRegion")
                    .withContext("maxBytes", maxBytes));
        }
    }


    /**
     * Close the segment and release resources.
     *
     * <p>When zero-copy batch transfers are still in flight (outstanding leases), the channel
     * close is DEFERRED until the last lease is released — the file may already be unlinked by
     * compaction, which is safe on POSIX (the open FD keeps serving reads).
     */
    public void close() throws MessagingException {
        seal();
        closeRequested = true;
        if (batchLeases.get() == 0) {
            closeChannels();
        } else {
            log.info("Deferring channel close for segment baseOffset={} — {} in-flight batch transfer(s)",
                    baseOffset, batchLeases.get());
        }
    }

    private synchronized void closeChannels() throws MessagingException {
        try {
            if (logChannel.isOpen()) logChannel.close();
            if (indexChannel.isOpen()) indexChannel.close();
        } catch (IOException e) {
            throw ExceptionLogger.logAndThrow(log,
                StorageException.ioError("Failed to close segment", e)
                    .withTopic(topic)
                    .withPartition(partition)
                    .withSegmentPath(logPath.toString())
                    .withContext("operation", "close"));
        }
    }

    private void releaseBatchLease() {
        if (batchLeases.decrementAndGet() == 0 && closeRequested) {
            try {
                closeChannels();
            } catch (MessagingException e) {
                log.warn("Deferred segment close failed for baseOffset={}", baseOffset, e);
            }
        }
    }

    /**
     * Helper class representing a parsed index entry.
     * Used during binary search through index file for offset lookups.
     */
    private static class IndexEntry {
        final long offset;          // Message offset (from cloud-server, can have gaps)
        final int logPosition;      // Position in log file where record starts
        final int recordSize;       // Size of log record in bytes
        final long indexFilePosition; // Byte position of this entry in the index file

        IndexEntry(long offset, int logPosition, int recordSize, long indexFilePosition) {
            this.offset = offset;
            this.logPosition = logPosition;
            this.recordSize = recordSize;
            this.indexFilePosition = indexFilePosition;
        }
    }

    /**
     * File-backed DeliveryBatch for zero-copy transfer via FileChannel.transferTo() (sendfile).
     *
     * Carries storage metadata (topic) and byte payload. Consumer group is intentionally absent
     * — it is a delivery-routing concept passed separately at the NetworkServer call site.
     */
    public static class BatchFileRegion implements DeliveryBatch {
        private final String topic;
        private final FileChannel fileChannel;  // null if empty batch
        private final int recordCount;
        private final long totalBytes;
        private final long firstOffset; // actual offset of first record in batch (-1 if empty)
        private final long lastOffset;
        private final long filePosition;  // position in log file where payload starts
        private final Runnable onClose;   // releases the segment's batch lease (null for empty batches)
        private final java.util.concurrent.atomic.AtomicBoolean closed =
                new java.util.concurrent.atomic.AtomicBoolean();

        public BatchFileRegion(String topic, FileChannel fileChannel,
                               int recordCount, long totalBytes, long firstOffset, long lastOffset, long filePosition) {
            this(topic, fileChannel, recordCount, totalBytes, firstOffset, lastOffset, filePosition, null);
        }

        public BatchFileRegion(String topic, FileChannel fileChannel,
                               int recordCount, long totalBytes, long firstOffset, long lastOffset,
                               long filePosition, Runnable onClose) {
            this.topic = topic;
            this.fileChannel = fileChannel;
            this.recordCount = recordCount;
            this.totalBytes = totalBytes;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.filePosition = filePosition;
            this.onClose = onClose;
        }

        @Override public String getTopic()      { return topic; }
        @Override public int getRecordCount()   { return recordCount; }
        @Override public long getTotalBytes()   { return totalBytes; }
        @Override public long getFirstOffset()  { return firstOffset; }
        @Override public long getLastOffset()   { return lastOffset; }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            if (fileChannel == null) {
                return -1;
            }
            long remaining = totalBytes - position;
            if (remaining <= 0) {
                return -1;
            }
            return fileChannel.transferTo(filePosition + position, remaining, target);
        }

        @Override
        public void close() throws IOException {
            // The FileChannel belongs to the Segment that created this batch — never close it
            // here. Releasing the lease lets a DEFERRED Segment.close() (compaction replaced
            // this segment mid-transfer) finally close the channel once the last in-flight
            // transfer completes. Idempotent: Netty's deallocate() and error paths may both call.
            if (closed.compareAndSet(false, true) && onClose != null) {
                onClose.run();
            }
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

    public boolean hasSpaceFor(MessageRecord record) {
        byte[] key = record.getMsgKey() != null
                ? record.getMsgKey().getBytes(StandardCharsets.UTF_8) : new byte[0];
        int dataLen = record.getData() != null
                ? record.getData().getBytes(StandardCharsets.UTF_8).length : 0;
        int logRecordSize = 4 + key.length + 1 + 4 + dataLen + 8;
        return logPosition + logRecordSize <= maxSize;
    }

    public Path getLogPath() {
        return logPath;
    }

    public Path getIndexPath() {
        return indexPath;
    }

    public long getRecordCount() {
        return recordCount;
    }
}
