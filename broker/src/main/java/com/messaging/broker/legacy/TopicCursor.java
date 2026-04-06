package com.messaging.broker.legacy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Streaming cursor for reading index entries from a single topic.
 * Implements peek/advance pattern for efficient k-way merge.
 *
 * Memory: O(1) - only one IndexEntry cached at a time (plus 1KB bulk buffer)
 * Time: O(log n) seek + amortized O(1/64) FileChannel.read() per entry
 */
public class TopicCursor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TopicCursor.class);

    private static final int FILE_HEADER_SIZE = 6;         // magic:4 + version:2
    private static final int INDEX_ENTRY_SIZE_V1 = 20;     // v1 (legacy): offset:8 + logPos:4 + size:4 + crc32:4
    private static final int INDEX_ENTRY_SIZE_V2 = 16;     // v2 (current): offset:8 + logPos:4 + size:4
    private static final short INDEX_FORMAT_VERSION_V2 = 2;

    // Bulk index read: 64 entries per FileChannel.read() call (~1KB for v2)
    private static final int READ_CHUNK = 64;

    private final String topic;
    private final FileChannel indexChannel;
    private final long indexFileSize;
    private final long startOffset;
    private final int indexEntrySize;    // 16 (v2) or 20 (v1), detected from file header

    private long indexPosition;          // File position for next refill
    private IndexEntry peeked;           // Next entry (cached by peek())
    private long lastOffsetDelivered;    // Track for ACK

    // Bulk index buffer — reused across refills, never reallocated
    private final ByteBuffer indexBuffer;
    private int bufferEntryCount = 0;   // valid entries in current buffer fill
    private int bufferEntryPos   = 0;   // next entry to consume from buffer

    public TopicCursor(String topic, Path indexPath, long startOffset) throws IOException {
        this.topic = topic;
        this.startOffset = startOffset;
        this.lastOffsetDelivered = startOffset - 1;

        this.indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
        this.indexFileSize = indexChannel.size();
        this.indexEntrySize = detectIndexEntrySize();

        // Single allocation for the bulk read buffer — reused for the lifetime of this cursor
        this.indexBuffer = ByteBuffer.allocate(READ_CHUNK * indexEntrySize);

        // Skip header; binary seek will adjust indexPosition
        this.indexPosition = FILE_HEADER_SIZE;

        log.debug("TopicCursor created: topic={}, indexPath={}, startOffset={}, fileSize={}",
                topic, indexPath, startOffset, indexFileSize);

        // Binary seek to first entry >= startOffset  (O(log n) reads)
        seekToOffset(startOffset);
    }

    /**
     * Read the 2-byte version field from the file header and return the correct entry size.
     * Header format: magic:4 + version:2 (matches Segment.java FILE_HEADER_SIZE=6)
     */
    private int detectIndexEntrySize() throws IOException {
        if (indexFileSize < FILE_HEADER_SIZE) {
            log.warn("TopicCursor: index file too small to read header for topic={}, defaulting to v2 (16-byte)", topic);
            return INDEX_ENTRY_SIZE_V2;
        }
        ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_SIZE);
        indexChannel.read(header, 0);
        header.flip();
        header.getInt(); // skip magic bytes
        short version = header.getShort();
        int size = (version >= INDEX_FORMAT_VERSION_V2) ? INDEX_ENTRY_SIZE_V2 : INDEX_ENTRY_SIZE_V1;
        log.debug("TopicCursor: topic={}, index version={}, entry size={}", topic, version, size);
        return size;
    }

    /**
     * Binary search to position cursor at first entry with offset >= targetOffset.
     * Complexity: O(log n) reads of indexEntrySize bytes each (~20 reads for 1M-record topic).
     */
    private void seekToOffset(long targetOffset) throws IOException {
        long totalEntries = (indexFileSize - FILE_HEADER_SIZE) / indexEntrySize;
        if (totalEntries <= 0) {
            indexPosition = indexFileSize; // mark exhausted
            log.debug("TopicCursor: empty index for topic={}", topic);
            return;
        }

        long lo = 0, hi = totalEntries - 1;
        long resultPos = -1;

        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long filePos = FILE_HEADER_SIZE + mid * indexEntrySize;
            IndexEntry entry = readEntryAt(filePos);
            if (entry == null) break;

            if (entry.offset >= targetOffset) {
                resultPos = filePos;  // candidate — keep searching left for earlier match
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }

        if (resultPos >= 0) {
            indexPosition = resultPos;
            bufferEntryCount = 0;
            bufferEntryPos   = 0;
            log.debug("TopicCursor positioned via binary search: topic={}, targetOffset={}, filePos={}",
                    topic, targetOffset, resultPos);
        } else {
            // All entries have offset < targetOffset — cursor exhausted
            indexPosition = indexFileSize;
            log.debug("TopicCursor exhausted during seek: topic={}, targetOffset={}", topic, targetOffset);
        }
    }

    /**
     * Read a single index entry at an absolute file position without disturbing
     * indexPosition or the bulk buffer. Used only during binary search.
     */
    private IndexEntry readEntryAt(long filePos) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(indexEntrySize);
        int bytesRead = indexChannel.read(buf, filePos);
        if (bytesRead < indexEntrySize) return null;
        buf.flip();
        long offset      = buf.getLong();
        int logPosition  = buf.getInt();
        int recordSize   = buf.getInt();
        int crc32        = (indexEntrySize == INDEX_ENTRY_SIZE_V1) ? buf.getInt() : 0;
        return new IndexEntry(offset, logPosition, recordSize, crc32);
    }

    /**
     * Peek at the next entry without advancing the cursor.
     * Returns the same entry on repeated calls.
     */
    public IndexEntry peek() throws IOException {
        if (peeked == null && hasMoreEntries()) {
            peeked = readNextEntry();
        }
        return peeked;
    }

    /**
     * Advance the cursor and return the current entry.
     * Subsequent peek() will return the next entry.
     */
    public IndexEntry advance() throws IOException {
        IndexEntry result = peek();
        if (result != null) {
            lastOffsetDelivered = result.offset;
            peeked = null;  // Clear cached entry
        }
        return result;
    }

    /**
     * Check if there are more entries to read
     */
    public boolean hasMore() throws IOException {
        return peek() != null;
    }

    /**
     * Check if there are more entries available — either buffered or still in the file.
     */
    private boolean hasMoreEntries() {
        return bufferEntryPos < bufferEntryCount
                || indexPosition + indexEntrySize <= indexFileSize;
    }

    /**
     * Read the next index entry using the bulk buffer.
     * Refills the buffer (READ_CHUNK entries at once) when exhausted — amortised O(1/64)
     * FileChannel.read() calls and zero ByteBuffer allocations per entry.
     */
    private IndexEntry readNextEntry() throws IOException {
        // Refill bulk buffer if all buffered entries have been consumed
        if (bufferEntryPos >= bufferEntryCount) {
            indexBuffer.clear();
            int bytesRead = indexChannel.read(indexBuffer, indexPosition);
            if (bytesRead <= 0) return null;
            indexBuffer.flip();
            bufferEntryCount = bytesRead / indexEntrySize;
            bufferEntryPos   = 0;
            indexPosition   += bytesRead;  // advance file cursor by full chunk
        }

        if (bufferEntryPos >= bufferEntryCount) return null;

        long offset     = indexBuffer.getLong();
        int logPosition = indexBuffer.getInt();
        int recordSize  = indexBuffer.getInt();
        int crc32       = (indexEntrySize == INDEX_ENTRY_SIZE_V1) ? indexBuffer.getInt() : 0;
        bufferEntryPos++;

        return new IndexEntry(offset, logPosition, recordSize, crc32);
    }

    // Getters
    public String getTopic() {
        return topic;
    }

    public long getLastOffsetDelivered() {
        return lastOffsetDelivered;
    }

    @Override
    public void close() throws IOException {
        if (indexChannel != null) {
            indexChannel.close();
        }
    }

    @Override
    public String toString() {
        return String.format("TopicCursor{topic=%s, lastOffset=%d, position=%d}",
                topic, lastOffsetDelivered, indexPosition);
    }
}
