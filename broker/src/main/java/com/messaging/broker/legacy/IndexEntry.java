package com.messaging.broker.legacy;

/**
 * Represents a single entry from the segment index file.
 * Format: [offset:8][logPosition:4][recordSize:4][crc32:4] = 20 bytes
 */
public class IndexEntry {
    public final long offset;
    public final int logPosition;
    public final int recordSize;
    public final int crc32;

    public IndexEntry(long offset, int logPosition, int recordSize, int crc32) {
        this.offset = offset;
        this.logPosition = logPosition;
        this.recordSize = recordSize;
        this.crc32 = crc32;
    }

    @Override
    public String toString() {
        return String.format("IndexEntry{offset=%d, logPos=%d, size=%d, crc=%08x}",
                offset, logPosition, recordSize, crc32);
    }
}
