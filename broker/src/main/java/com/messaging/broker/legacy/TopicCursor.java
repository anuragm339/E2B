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
 * Memory: O(1) - only one IndexEntry cached at a time
 * Time: O(n) - sequential scan through index file
 */
public class TopicCursor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TopicCursor.class);

    private static final int FILE_HEADER_SIZE = 6;  // magic:4 + version:2
    private static final int INDEX_ENTRY_SIZE = 20; // offset:8 + logPos:4 + size:4 + crc32:4

    private final String topic;
    private final FileChannel indexChannel;
    private final long indexFileSize;
    private final long startOffset;

    private long indexPosition;          // Current position in index file
    private IndexEntry peeked;           // Next entry (cached)
    private long lastOffsetDelivered;    // Track for ACK

    public TopicCursor(String topic, Path indexPath, long startOffset) throws IOException {
        this.topic = topic;
        this.startOffset = startOffset;
        this.lastOffsetDelivered = startOffset - 1;

        // Open index file for reading
        this.indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
        this.indexFileSize = indexChannel.size();

        // Skip header
        this.indexPosition = FILE_HEADER_SIZE;

        log.debug("TopicCursor created: topic={}, indexPath={}, startOffset={}, fileSize={}",
                topic, indexPath, startOffset, indexFileSize);

        // Position to the first entry >= startOffset
        seekToOffset(startOffset);
    }

    /**
     * Seek to the first entry with offset >= targetOffset
     */
    private void seekToOffset(long targetOffset) throws IOException {
        // For now, do linear scan (could optimize with binary search later)
        while (hasMoreEntries()) {
            long position = indexPosition;
            IndexEntry entry = readNextEntry();

            if (entry.offset >= targetOffset) {
                // Found it! Put back and return
                peeked = entry;
                indexPosition = position;
                log.debug("TopicCursor positioned: topic={}, targetOffset={}, foundOffset={}",
                        topic, targetOffset, entry.offset);
                return;
            }
        }

        // Reached end without finding - cursor exhausted
        log.debug("TopicCursor exhausted during seek: topic={}, targetOffset={}",
                topic, targetOffset);
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
     * Check if there are more entries in the index file
     */
    private boolean hasMoreEntries() {
        return indexPosition + INDEX_ENTRY_SIZE <= indexFileSize;
    }

    /**
     * Read the next index entry from the file
     */
    private IndexEntry readNextEntry() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        int bytesRead = indexChannel.read(buffer, indexPosition);

        if (bytesRead < INDEX_ENTRY_SIZE) {
            return null;
        }

        buffer.flip();
        long offset = buffer.getLong();
        int logPosition = buffer.getInt();
        int recordSize = buffer.getInt();
        int crc32 = buffer.getInt();

        indexPosition += INDEX_ENTRY_SIZE;

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
