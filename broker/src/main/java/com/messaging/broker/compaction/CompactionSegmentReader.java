package com.messaging.broker.compaction;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.NetworkException;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Sequential streaming reader for compaction.
 *
 * <p>Reads a segment log file using a single fixed-size {@link ByteBuffer} (256 KB), refilling
 * it from a dedicated read-only {@link FileChannel} as records are consumed.  This approach:
 *
 * <ul>
 *   <li>Coalesces thousands of individual {@code FileChannel.read()} calls into large sequential
 *       I/O operations — dramatically reducing syscall overhead on a 1 GB segment.</li>
 *   <li>Never holds more than one decoded {@link MessageRecord} in heap at a time, so memory
 *       consumption is bounded by the 256 KB read buffer regardless of segment size.</li>
 *   <li>Uses a separate {@link FileChannel} opened {@code READ}-only — it does NOT touch the
 *       {@link com.messaging.storage.segment.Segment}'s own channels and therefore cannot
 *       interfere with concurrent normal reads or writes.</li>
 * </ul>
 *
 * <p>The log file format (written by {@code Segment.writeRecord}) is:
 * <pre>
 *   File header: [magic:4B "MLOG"][version:2B]
 *   Per record:  [keyLen:4B][key:keyLen B][eventType:1B][dataLen:4B][data:dataLen B][timestamp:8B]
 * </pre>
 *
 * <p>The index file is NOT consulted; offsets are read from the index entries in a separate
 * index-scan pass and correlated by sequential record position.  Specifically: this reader
 * scans the index file to build an in-order array of {@code (logPosition, offset)} pairs, then
 * reads the log file at each logPosition in order.  This is O(1) memory per record and avoids
 * random-access I/O on the log file.
 */
final class CompactionSegmentReader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(CompactionSegmentReader.class);

    // Must match Segment constants
    private static final int FILE_HEADER_SIZE    = 6;  // magic:4 + version:2
    private static final int READ_BUFFER_SIZE    = 256 * 1024;  // 256 KB streaming window

    // Index entry sizes — must match Segment.INDEX_ENTRY_SIZE / INDEX_ENTRY_SIZE_LEGACY
    private static final int INDEX_ENTRY_V2      = 16; // offset:8 + logPos:4 + recordSize:4
    private static final int INDEX_ENTRY_LEGACY  = 20; // same + crc32:4

    private static final byte[] LOG_MAGIC   = "MLOG".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INDEX_MAGIC = "MIDX".getBytes(StandardCharsets.UTF_8);

    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;

    // Fixed-size streaming buffer — never grows beyond READ_BUFFER_SIZE
    private final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUFFER_SIZE);

    // Current file position of the data already consumed from readBuf
    private long logFilePos = 0;

    // Pre-loaded index entries (offset per sequential record position)
    private long[] indexOffsets;     // logical offset of each record, indexed by record seq number
    private int[]  indexLogPositions; // logPosition for each record (for validation / skipping)
    private int    totalRecords = 0;
    private int    currentRecord = 0; // next record index to deliver

    private boolean closed = false;

    /**
     * Open a compaction reader for the given segment log and index files.
     *
     * @throws StorageException if the files cannot be opened or their headers are invalid
     */
    CompactionSegmentReader(Path logPath, Path indexPath) throws StorageException {
        this.logPath   = logPath;
        this.indexPath = indexPath;

        try {
            this.logChannel   = FileChannel.open(logPath,   StandardOpenOption.READ);
            this.indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentReader: failed to open segment files: " + logPath, e);
        }

        try {
            validateHeaders();
            loadIndexOffsets();
            // Position log file past the header — streaming reads start here
            logFilePos = FILE_HEADER_SIZE;
            refillBuffer();
        } catch (IOException e) {
            closeQuietly();
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentReader: failed to initialise reader for " + logPath, e);
        }
    }

    /** Returns true if there is at least one more record to read. */
    boolean hasNext() {
        return !closed && currentRecord < totalRecords;
    }

    /**
     * Read and decode the next record.  Returns {@code null} when there are no more records.
     *
     * <p>The returned {@link MessageRecord} is populated with:
     * <ul>
     *   <li>offset   — from the index file (not stored in the log record itself)</li>
     *   <li>msgKey   — decoded from log bytes</li>
     *   <li>eventType — decoded from log bytes</li>
     *   <li>data      — decoded from log bytes (may be null for zero-length payloads)</li>
     *   <li>createdAt — decoded from log bytes</li>
     * </ul>
     */
    MessageRecord next() throws StorageException {
        if (!hasNext()) return null;

        try {
            // -- keyLen (4 bytes) --
            ensureAvailable(4);
            int keyLen = readBuf.getInt();

            // -- key (keyLen bytes) --
            byte[] keyBytes = readBytes(keyLen);

            // -- eventType (1 byte) --
            ensureAvailable(1);
            byte eventCode = readBuf.get();

            // -- dataLen (4 bytes) --
            ensureAvailable(4);
            int dataLen = readBuf.getInt();

            // -- data (dataLen bytes) --
            byte[] dataBytes = dataLen > 0 ? readBytes(dataLen) : null;

            // -- timestamp (8 bytes) --
            ensureAvailable(8);
            long timestampMs = readBuf.getLong();

            EventType eventType;
            try {
                eventType = EventType.fromCode((char) eventCode);
            } catch (NetworkException e) {
                throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                        "CompactionSegmentReader: unknown event type code " + eventCode +
                        " at record " + currentRecord + " in " + logPath, e);
            }

            MessageRecord rec = new MessageRecord();
            rec.setOffset(indexOffsets[currentRecord]);
            rec.setMsgKey(new String(keyBytes, StandardCharsets.UTF_8));
            rec.setEventType(eventType);
            if (dataBytes != null && dataBytes.length > 0) {
                rec.setData(new String(dataBytes, StandardCharsets.UTF_8));
            }
            rec.setCreatedAt(Instant.ofEpochMilli(timestampMs));

            currentRecord++;
            return rec;

        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentReader: failed to read record " + currentRecord +
                    " from " + logPath, e);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        closeQuietly();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Validate file magic bytes and determine the index entry size.
     * Returns the effective index entry size (16 for v2, 20 for legacy v1).
     */
    private int validateHeaders() throws IOException, StorageException {
        ByteBuffer hdr = ByteBuffer.allocate(FILE_HEADER_SIZE);

        // Validate log header
        hdr.clear();
        logChannel.read(hdr, 0);
        hdr.flip();
        byte[] magic = new byte[4];
        hdr.get(magic);
        if (!java.util.Arrays.equals(magic, LOG_MAGIC)) {
            throw new StorageException(ErrorCode.STORAGE_CORRUPTION,
                    "CompactionSegmentReader: invalid log magic in " + logPath);
        }
        // version byte (ignored, just validated present)
        hdr.getShort();

        // Validate index header and detect v1 vs v2 format
        hdr.clear();
        indexChannel.read(hdr, 0);
        hdr.flip();
        byte[] idxMagic = new byte[4];
        hdr.get(idxMagic);
        if (!java.util.Arrays.equals(idxMagic, INDEX_MAGIC)) {
            throw new StorageException(ErrorCode.STORAGE_CORRUPTION,
                    "CompactionSegmentReader: invalid index magic in " + indexPath);
        }
        short indexVersion = hdr.getShort();
        if (indexVersion == 1) {
            log.debug("CompactionSegmentReader: legacy index format (v1, 20-byte entries) for {}", indexPath);
            return INDEX_ENTRY_LEGACY;
        }
        return INDEX_ENTRY_V2;
    }

    /**
     * Load all index entries into two compact primitive arrays.
     * Memory cost: 12 bytes per record (8B offset + 4B logPosition).
     * A 1 GB segment with 10 KB average record size → ~100 K records → ~1.2 MB — negligible.
     */
    private void loadIndexOffsets() throws IOException, StorageException {
        // Re-determine entry size (validateHeaders was called before but didn't store result)
        int entrySize = detectIndexEntrySize();

        long indexSize = indexChannel.size();
        long dataBytes = indexSize - FILE_HEADER_SIZE;
        if (dataBytes <= 0) {
            totalRecords  = 0;
            indexOffsets  = new long[0];
            indexLogPositions = new int[0];
            return;
        }

        totalRecords = (int) (dataBytes / entrySize);
        indexOffsets      = new long[totalRecords];
        indexLogPositions = new int[totalRecords];

        ByteBuffer entryBuf = ByteBuffer.allocate(entrySize);
        long pos = FILE_HEADER_SIZE;

        for (int i = 0; i < totalRecords; i++) {
            entryBuf.clear();
            int read = indexChannel.read(entryBuf, pos);
            if (read < entrySize) {
                // Truncated index — fewer records than expected
                totalRecords = i;
                break;
            }
            entryBuf.flip();
            indexOffsets[i]      = entryBuf.getLong();   // offset:8
            indexLogPositions[i] = entryBuf.getInt();    // logPosition:4
            // recordSize:4 is present but not needed here (we scan sequentially)
            pos += entrySize;
        }
    }

    private int detectIndexEntrySize() throws IOException, StorageException {
        ByteBuffer hdr = ByteBuffer.allocate(FILE_HEADER_SIZE);
        hdr.clear();
        indexChannel.read(hdr, 0);
        hdr.flip();
        hdr.position(4); // skip magic
        short v = hdr.getShort();
        return (v == 1) ? INDEX_ENTRY_LEGACY : INDEX_ENTRY_V2;
    }

    /**
     * Ensure at least {@code needed} bytes are available in {@code readBuf}.
     * If not, compacts the buffer and refills from the file channel.
     */
    private void ensureAvailable(int needed) throws IOException, StorageException {
        if (readBuf.remaining() >= needed) return;

        // Compact: move unread bytes to front, then fill remainder from file
        readBuf.compact();
        int bytesRead = logChannel.read(readBuf, logFilePos);
        if (bytesRead > 0) {
            logFilePos += bytesRead;
        }
        readBuf.flip();

        if (readBuf.remaining() < needed) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentReader: unexpected end of log file at position " +
                    logFilePos + " in " + logPath +
                    " (need " + needed + " bytes, have " + readBuf.remaining() + ")");
        }
    }

    /**
     * Read exactly {@code len} bytes from the streaming buffer, refilling as necessary.
     * Handles the case where {@code len} exceeds {@code READ_BUFFER_SIZE} (large records)
     * by falling back to a direct positioned read.
     */
    private byte[] readBytes(int len) throws IOException, StorageException {
        if (len == 0) return new byte[0];

        if (len <= READ_BUFFER_SIZE) {
            ensureAvailable(len);
            byte[] out = new byte[len];
            readBuf.get(out);
            return out;
        }

        // Large record: read directly from file to avoid buffer overflow
        // First drain what's in the buffer so logFilePos stays consistent
        int inBuf = readBuf.remaining();
        byte[] out = new byte[len];
        readBuf.get(out, 0, inBuf);

        // Read remainder directly; logFilePos already advanced past what we consumed
        ByteBuffer tmp = ByteBuffer.wrap(out, inBuf, len - inBuf);
        while (tmp.hasRemaining()) {
            int r = logChannel.read(tmp, logFilePos);
            if (r <= 0) {
                throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                        "CompactionSegmentReader: unexpected EOF reading large record (" +
                        len + " bytes) from " + logPath);
            }
            logFilePos += r;
        }

        // Reset buffer to empty so ensureAvailable will refill on next call
        readBuf.clear();
        readBuf.flip();  // empty, position=0, limit=0

        return out;
    }

    /** Initial buffer fill after positioning past the file header. */
    private void refillBuffer() throws IOException {
        readBuf.clear();
        int bytesRead = logChannel.read(readBuf, logFilePos);
        if (bytesRead > 0) {
            logFilePos += bytesRead;
        }
        readBuf.flip();
    }

    private void closeQuietly() {
        try { if (logChannel   != null && logChannel.isOpen())   logChannel.close();   } catch (IOException ignored) {}
        try { if (indexChannel != null && indexChannel.isOpen()) indexChannel.close(); } catch (IOException ignored) {}
    }
}
