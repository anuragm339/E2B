package com.messaging.broker.compaction;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.MessageRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Streaming output writer for compaction.
 *
 * <p>Writes compaction survivors to a staging log+index pair using coalesced {@link FileChannel}
 * writes.  Key memory-reduction properties:
 *
 * <ul>
 *   <li>A single reusable {@code ByteBuffer} of at most {@link #WRITE_BUFFER_SIZE} bytes is used
 *       for all writes.  Records larger than the buffer are written directly.</li>
 *   <li>{@code FileChannel.force()} is called <em>once</em> at close, not per-record — this
 *       eliminates thousands of fsync syscalls for a 1 GB segment and dramatically reduces
 *       the time (and therefore JVM heap live-time) of a compaction run.</li>
 *   <li>Log and index files are written to {@code .compacting} staging paths and atomically
 *       renamed by the caller via {@link #finalise(Path, Path)} only after successful
 *       completion.</li>
 * </ul>
 *
 * <p>Log record format (must match {@code Segment.writeRecord}):
 * <pre>
 *   File header : [magic:4B "MLOG"][version:2B 0x01]
 *   Per record  : [keyLen:4B][key:keyLen B][eventType:1B][dataLen:4B][data:dataLen B][timestamp:8B]
 * </pre>
 *
 * <p>Index entry format v2 (must match {@code Segment} INDEX_ENTRY_SIZE = 16):
 * <pre>
 *   File header : [magic:4B "MIDX"][version:2B 0x02]
 *   Per entry   : [offset:8B][logPosition:4B][recordSize:4B]
 * </pre>
 */
final class CompactionSegmentWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(CompactionSegmentWriter.class);

    // Must match Segment constants exactly
    private static final int    FILE_HEADER_SIZE = 6;   // magic:4 + version:2
    private static final int    INDEX_ENTRY_SIZE = 16;  // offset:8 + logPos:4 + size:4
    private static final byte[] LOG_MAGIC        = "MLOG".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INDEX_MAGIC      = "MIDX".getBytes(StandardCharsets.UTF_8);
    private static final short  LOG_VERSION      = 1;
    private static final short  INDEX_VERSION    = 2;

    /** Write buffer size: 256 KB for log, 64 KB for index (index entries are small). */
    private static final int WRITE_BUFFER_SIZE       = 256 * 1024;
    private static final int INDEX_WRITE_BUFFER_SIZE =  64 * 1024;

    private final Path stagingLogPath;
    private final Path stagingIndexPath;

    private final FileChannel logChannel;
    private final FileChannel indexChannel;

    // Write-coalescing buffers — flushed when full or at close
    private final ByteBuffer logWriteBuf   = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
    private final ByteBuffer indexWriteBuf = ByteBuffer.allocate(INDEX_WRITE_BUFFER_SIZE);

    // Running positions in log and index files
    private long logFilePos   = 0;
    private long indexFilePos = 0;

    // Stats
    private int  recordsWritten = 0;
    private long bytesWritten   = 0;

    private boolean closed = false;

    /**
     * Create a compaction output writer, opening staging files at the given paths.
     * Any pre-existing files at those paths are truncated (caller should delete them first).
     */
    CompactionSegmentWriter(Path stagingLogPath, Path stagingIndexPath) throws StorageException {
        this.stagingLogPath   = stagingLogPath;
        this.stagingIndexPath = stagingIndexPath;

        try {
            logChannel = FileChannel.open(stagingLogPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);

            indexChannel = FileChannel.open(stagingIndexPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            // logChannel may have opened successfully before indexChannel failed — close it to
            // avoid leaking an FD. Under EMFILE conditions each leaked FD makes the next attempt
            // more likely to fail, creating a self-reinforcing exhaustion loop.
            closeQuietly();
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentWriter: failed to open staging files: " + stagingLogPath, e);
        }

        try {
            writeFileHeaders();
        } catch (IOException e) {
            closeQuietly();
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentWriter: failed to write file headers: " + stagingLogPath, e);
        }
    }

    /**
     * Append a survivor record to the compacted output.
     * Writes are coalesced in the write buffers; call {@link #close()} to flush and fsync.
     */
    void append(MessageRecord record) throws StorageException {
        if (closed) {
            throw new StorageException(ErrorCode.STORAGE_WRITE_FAILED,
                    "CompactionSegmentWriter: writer is already closed: " + stagingLogPath);
        }
        try {
            writeRecord(record);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentWriter: failed to write record offset=" +
                    record.getOffset() + " to " + stagingLogPath, e);
        }
    }

    /** Total bytes written to the log file (excluding file header). */
    long getBytesWritten() {
        // logFilePos includes the header; subtract header size for payload-only count
        return Math.max(0, logFilePos - FILE_HEADER_SIZE);
    }

    int getRecordsWritten() { return recordsWritten; }

    /**
     * Flush all write buffers, fsync both log and index files, and close channels.
     * After this returns the staging files are durable and ready to be renamed.
     */
    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            flushLogBuf();
            flushIndexBuf();
            logChannel.force(true);
            indexChannel.force(true);
        } finally {
            closeQuietly();
        }
    }

    /**
     * Atomically rename staging files to their final target paths.
     * Must be called AFTER {@link #close()} has returned successfully.
     *
     * @param finalLogPath   target log path (e.g. {@code 00000000000000000000.compacted.log})
     * @param finalIndexPath target index path
     */
    void finalise(Path finalLogPath, Path finalIndexPath) throws StorageException {
        try {
            Files.move(stagingLogPath, finalLogPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentWriter: atomic rename failed: " + stagingLogPath + " -> " + finalLogPath, e);
        }
        try {
            Files.move(stagingIndexPath, finalIndexPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // Roll back the log rename so the broker does not end up with a .compacted.log
            // that has no matching .compacted.index — that would crash segment recovery on restart.
            try {
                Files.move(finalLogPath, stagingLogPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException rollbackEx) {
                log.error("CompactionSegmentWriter: log rollback failed after index rename error — " +
                          "orphaned compacted log at {}; manual cleanup required", finalLogPath, rollbackEx);
            }
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "CompactionSegmentWriter: atomic rename failed: " + stagingIndexPath + " -> " + finalIndexPath, e);
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void writeFileHeaders() throws IOException {
        // Log header
        ByteBuffer hdr = ByteBuffer.allocate(FILE_HEADER_SIZE);
        hdr.put(LOG_MAGIC);
        hdr.putShort(LOG_VERSION);
        hdr.flip();
        writeToLog(hdr);

        // Index header
        hdr.clear();
        hdr.put(INDEX_MAGIC);
        hdr.putShort(INDEX_VERSION);
        hdr.flip();
        writeToIndex(hdr);
    }

    /**
     * Encode and buffer a record.
     * Log layout: [keyLen:4][key][eventType:1][dataLen:4][data][timestamp:8]
     * Index layout: [offset:8][logPos:4][recordSize:4]
     */
    private void writeRecord(MessageRecord record) throws IOException {
        byte[] keyBytes  = record.getMsgKey() != null
                ? record.getMsgKey().getBytes(StandardCharsets.UTF_8)
                : new byte[0];
        byte[] dataBytes = record.getData() != null
                ? record.getData().getBytes(StandardCharsets.UTF_8)
                : new byte[0];

        int logRecordSize = 4 + keyBytes.length + 1 + 4 + dataBytes.length + 8;

        long recordLogPosition;

        // Encode log record
        if (logRecordSize <= logWriteBuf.capacity()) {
            // Fits in the write buffer — coalesce.
            // Capture position AFTER ensureLogSpace so any buffer flush is already reflected
            // in logFilePos; the record lands at logFilePos + current buffer position.
            ensureLogSpace(logRecordSize);
            recordLogPosition = logFilePos + logWriteBuf.position();

            logWriteBuf.putInt(keyBytes.length);
            logWriteBuf.put(keyBytes);
            logWriteBuf.put((byte) record.getEventType().getCode());
            logWriteBuf.putInt(dataBytes.length);
            if (dataBytes.length > 0) logWriteBuf.put(dataBytes);
            logWriteBuf.putLong(record.getCreatedAt().toEpochMilli());
        } else {
            // Large record: flush current buffer then write directly.
            // Capture position AFTER flush so logFilePos is current file end.
            flushLogBuf();
            recordLogPosition = logFilePos;

            ByteBuffer direct = ByteBuffer.allocate(logRecordSize);
            direct.putInt(keyBytes.length);
            direct.put(keyBytes);
            direct.put((byte) record.getEventType().getCode());
            direct.putInt(dataBytes.length);
            if (dataBytes.length > 0) direct.put(dataBytes);
            direct.putLong(record.getCreatedAt().toEpochMilli());
            direct.flip();
            writeToLog(direct);
        }

        // Encode index entry (always fixed 16 bytes — always fits in buffer)
        ensureIndexSpace(INDEX_ENTRY_SIZE);
        indexWriteBuf.putLong(record.getOffset());
        indexWriteBuf.putInt((int) recordLogPosition);
        indexWriteBuf.putInt(logRecordSize);

        recordsWritten++;
        bytesWritten += logRecordSize;
    }

    /**
     * Ensure at least {@code needed} bytes are available in the log write buffer.
     * Flushes the buffer to disk first if necessary.
     */
    private void ensureLogSpace(int needed) throws IOException {
        if (logWriteBuf.remaining() < needed) {
            flushLogBuf();
        }
    }

    private void ensureIndexSpace(int needed) throws IOException {
        if (indexWriteBuf.remaining() < needed) {
            flushIndexBuf();
        }
    }

    private void flushLogBuf() throws IOException {
        logWriteBuf.flip();
        if (logWriteBuf.hasRemaining()) {
            writeToLog(logWriteBuf);
        }
        logWriteBuf.clear();
    }

    private void flushIndexBuf() throws IOException {
        indexWriteBuf.flip();
        if (indexWriteBuf.hasRemaining()) {
            writeToIndex(indexWriteBuf);
        }
        indexWriteBuf.clear();
    }

    /** Write all bytes in {@code buf} to the log channel at the current log position. */
    private void writeToLog(ByteBuffer buf) throws IOException {
        int toWrite = buf.remaining();
        while (buf.hasRemaining()) {
            logChannel.write(buf, logFilePos + (toWrite - buf.remaining()));
        }
        logFilePos += toWrite;
    }

    /** Write all bytes in {@code buf} to the index channel at the current index position. */
    private void writeToIndex(ByteBuffer buf) throws IOException {
        int toWrite = buf.remaining();
        while (buf.hasRemaining()) {
            indexChannel.write(buf, indexFilePos + (toWrite - buf.remaining()));
        }
        indexFilePos += toWrite;
    }

    private void closeQuietly() {
        try { if (logChannel   != null && logChannel.isOpen())   logChannel.close();   } catch (IOException ignored) {}
        try { if (indexChannel != null && indexChannel.isOpen()) indexChannel.close(); } catch (IOException ignored) {}
    }
}
