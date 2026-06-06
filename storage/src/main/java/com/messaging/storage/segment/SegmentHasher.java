package com.messaging.storage.segment;

import com.messaging.common.exception.MessagingException;
import com.messaging.common.hash.RecordHasher;
import com.messaging.common.model.MessageRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * One-time backfill of the rolling PipeConsistency hash for segments that didn't
 * accumulate it incrementally (pre-feature segments, or active segments whose
 * in-memory hash was lost in a crash before seal).
 *
 * The fast path on the write side is Segment.append() folding each record into
 * the rolling hash as it arrives — never use this method for fresh writes.
 */
public final class SegmentHasher {
    private static final Logger log = LoggerFactory.getLogger(SegmentHasher.class);

    private static final int FILE_HEADER_SIZE = 6;
    private static final int INDEX_ENTRY_SIZE_V2 = 16;        // [offset:8][logPos:4][recordSize:4]
    private static final int INDEX_ENTRY_SIZE_LEGACY = 20;    // v1 = same as v2 + crc32:4

    private SegmentHasher() {}

    /**
     * Walk the segment's index file in offset order, hash each record, and install
     * the resulting rolling hash + record count on the segment. After this call,
     * snapshotRollingHash().trustworthy is true and reflects every record currently
     * indexed.
     */
    public static void reconstructFromScratch(Segment segment) throws MessagingException {
        Path indexPath = segment.getIndexPath();
        int entrySize = detectEntrySize(indexPath);

        byte[] rolling = RecordHasher.EMPTY_HASH.clone();
        long count = 0;

        try (FileChannel idx = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            long size = idx.size();
            ByteBuffer entry = ByteBuffer.allocate(entrySize);
            long pos = FILE_HEADER_SIZE;
            while (pos + entrySize <= size) {
                entry.clear();
                int read = idx.read(entry, pos);
                if (read < entrySize) {
                    break;
                }
                entry.flip();
                long offset = entry.getLong();
                // logPos and recordSize follow but we don't need them when reading via Segment.read()
                pos += entrySize;

                MessageRecord rec = segment.read(offset);
                if (rec == null) {
                    log.warn("Index pointed at offset {} but record read returned null; skipping. topic={}, partition={}, baseOffset={}",
                            offset, segment.getTopic(), segment.getPartition(), segment.getBaseOffset());
                    continue;
                }
                int crc = RecordHasher.recordCrc(
                        offset,
                        rec.getMsgKey(),
                        (char) rec.getEventType().getCode(),
                        rec.getData());
                rolling = RecordHasher.combine(rolling, crc);
                count++;
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to reconstruct rolling hash from " + indexPath, e);
        }

        segment.installRollingHash(rolling, count);
        log.info("Reconstructed rolling hash: topic={}, partition={}, baseOffset={}, records={}",
                segment.getTopic(), segment.getPartition(), segment.getBaseOffset(), count);
    }

    private static int detectEntrySize(Path indexPath) {
        try (FileChannel idx = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            ByteBuffer hdr = ByteBuffer.allocate(FILE_HEADER_SIZE);
            int read = idx.read(hdr, 0);
            if (read < FILE_HEADER_SIZE) {
                return INDEX_ENTRY_SIZE_V2;
            }
            hdr.flip();
            // Skip 4 magic bytes ("MIDX") then read version short
            hdr.position(4);
            short version = hdr.getShort();
            return (version == 1) ? INDEX_ENTRY_SIZE_LEGACY : INDEX_ENTRY_SIZE_V2;
        } catch (java.io.IOException e) {
            log.warn("Failed to detect index entry size for {}; assuming v2", indexPath, e);
            return INDEX_ENTRY_SIZE_V2;
        }
    }
}
