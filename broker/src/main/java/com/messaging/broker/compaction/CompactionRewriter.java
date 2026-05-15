package com.messaging.broker.compaction;

import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.segment.Segment;
import com.messaging.storage.segment.SegmentManager;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Rewrites a window of sealed segments, removing records eligible for physical deletion:
 * <ul>
 *   <li><b>Superseded records</b>: a newer record for the same (topic, msgKey) exists.</li>
 *   <li><b>Expired tombstones</b>: the record is a DELETE that is the latest for its key,
 *       but its timestamp is older than {@code tombstoneRetentionDays}.</li>
 * </ul>
 *
 * <p>Surviving records retain their original offsets. All candidate segments are merged into a
 * single compacted segment and atomically swapped via
 * {@link SegmentManager#replaceSegments(List, Segment)}.
 *
 * <p>The new segment is written to a {@code .compacted.log} path so a crash mid-rewrite
 * does not corrupt live data. The recovery service recognises {@code .compacted.log} as a
 * valid segment on restart.
 */
@Singleton
public class CompactionRewriter {

    private static final Logger log = LoggerFactory.getLogger(CompactionRewriter.class);
    private static final long MS_PER_DAY = 86_400_000L;

    public static class CompactionResult {
        public final int recordsRemoved;
        public final long bytesReclaimed;
        /** True when at least one live DELETE tombstone was kept because it has not yet aged out.
         *  The scheduler must NOT advance the checkpoint in this case so the segment is revisited
         *  on the next run, when the tombstone may have finally expired. */
        public final boolean hadUnexpiredTombstones;

        public CompactionResult(int recordsRemoved, long bytesReclaimed, boolean hadUnexpiredTombstones) {
            this.recordsRemoved = recordsRemoved;
            this.bytesReclaimed = bytesReclaimed;
            this.hadUnexpiredTombstones = hadUnexpiredTombstones;
        }
    }

    /**
     * Rewrite {@code candidates} into a single compacted segment.
     * {@code partitionDataDir} and {@code maxSegmentSize} are read from {@code segmentManager}.
     *
     * @param candidates             sealed segments to compact (sorted ascending by base offset)
     * @param topic                  topic name
     * @param partition              partition number
     * @param segmentManager         provides data dir, segment size, and the atomic swap
     * @param compactionIndex        source of truth for latest offset per (topic, key)
     * @param tombstoneRetentionDays days after which an expired DELETE tombstone can be removed
     */
    public CompactionResult rewrite(
            List<Segment> candidates,
            String topic,
            int partition,
            SegmentManager segmentManager,
            RocksDbCompactionIndex compactionIndex,
            int tombstoneRetentionDays) throws MessagingException {

        if (candidates.isEmpty()) {
            return new CompactionResult(0, 0L, false);
        }

        long firstBaseOffset = candidates.stream()
                .mapToLong(Segment::getBaseOffset)
                .min()
                .orElseThrow();

        Path partitionDataDir = segmentManager.getDataDir();

        // The compacted segment must hold all survivors from potentially many source segments.
        // Use the combined on-disk size of all candidates as the ceiling — survivors can never
        // exceed the total input bytes, so this is always sufficient.
        long compactedMaxSize = candidates.stream()
                .mapToLong(Segment::getSize)
                .sum();
        // Guard against empty segments (size=0) — ensure at least the normal segment size.
        if (compactedMaxSize <= 0) {
            compactedMaxSize = segmentManager.getMaxSegmentSize();
        }

        Path compactedLogPath   = partitionDataDir.resolve(String.format("%020d.compacted.log",   firstBaseOffset));
        Path compactedIndexPath = partitionDataDir.resolve(String.format("%020d.compacted.index", firstBaseOffset));

        // Remove any leftover file from a previous crashed run at the same path
        try {
            Files.deleteIfExists(compactedLogPath);
            Files.deleteIfExists(compactedIndexPath);
        } catch (IOException e) {
            log.warn("Could not delete stale compacted segment files: {}", compactedLogPath, e);
        }

        Segment compactedSegment = new Segment(compactedLogPath, compactedIndexPath,
                firstBaseOffset, compactedMaxSize, topic, partition);

        int removedCount             = 0;
        long reclaimedBytes          = 0L;
        int survivorCount            = 0;
        boolean hadUnexpiredTombstone = false;

        try {
            // Stream-write survivors directly — avoids holding an entire segment in heap.
            // Advance by actual record offset (not offset++) because sealed segments may have
            // gaps after a prior compaction; Segment.read() returns the record AT OR AFTER the
            // requested offset, so incrementing by 1 would re-read the same record repeatedly.
            for (Segment seg : candidates) {
                long offset = seg.getBaseOffset();
                while (offset < seg.getNextOffset()) {
                    MessageRecord record = seg.read(offset);
                    if (record == null) {
                        log.warn("Null record at offset={} in segment baseOffset={}, stopping scan",
                                offset, seg.getBaseOffset());
                        break;
                    }

                    long actualOffset = record.getOffset();
                    if (actualOffset >= seg.getNextOffset()) break;

                    if (isEligibleForDeletion(topic, record.getMsgKey(), actualOffset, record,
                            compactionIndex, tombstoneRetentionDays)) {
                        removedCount++;
                        reclaimedBytes += estimateRecordSize(record);
                    } else {
                        compactedSegment.append(record);
                        survivorCount++;
                        // Track whether a live DELETE tombstone was kept within its retention window
                        // so the scheduler knows to revisit this window later.
                        if (record.getEventType() == EventType.DELETE) {
                            long[] latestInfo = compactionIndex.getLatestOffsetAndTimestamp(topic, record.getMsgKey());
                            if (latestInfo != null && latestInfo[0] == actualOffset) {
                                hadUnexpiredTombstone = true;
                            }
                        }
                    }

                    offset = actualOffset + 1;
                }
            }
            // All records eligible for deletion — installing an empty segment would stall consumers
            // whose read offset equals the base offset (fromOffset > storageHead with 0 records).
            // Remove the input segments from the manager without installing any replacement.
            if (survivorCount == 0) {
                try { compactedSegment.close(); } catch (Exception ignored) {}
                try {
                    Files.deleteIfExists(compactedLogPath);
                    Files.deleteIfExists(compactedIndexPath);
                } catch (IOException ioEx) {
                    log.warn("Could not delete empty compacted segment files: {}", compactedLogPath, ioEx);
                }
                segmentManager.removeSegments(candidates);
                log.info("Compacted topic={} partition={}: all {} input records were eligible for deletion, " +
                         "removed {} source segment(s) without installing a replacement",
                         topic, partition, removedCount, candidates.size());
                return new CompactionResult(removedCount, reclaimedBytes, false);
            }

            compactedSegment.seal();
        } catch (MessagingException e) {
            try { compactedSegment.close(); } catch (Exception ignored) {}
            throw e;
        }

        segmentManager.replaceSegments(candidates, compactedSegment);

        log.info("Compacted topic={} partition={}: removed={} records, reclaimed={}B, survivors={}, unexpiredTombstones={}",
                topic, partition, removedCount, reclaimedBytes, survivorCount, hadUnexpiredTombstone);

        return new CompactionResult(removedCount, reclaimedBytes, hadUnexpiredTombstone);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private boolean isEligibleForDeletion(
            String topic,
            String msgKey,
            long recordOffset,
            MessageRecord record,
            RocksDbCompactionIndex compactionIndex,
            int tombstoneRetentionDays) {

        // Null-keyed records are never indexed, so they are never eligible for deletion
        if (msgKey == null) return false;

        // Single RocksDB lookup covers both supersession check and tombstone expiry check
        long[] latestInfo = compactionIndex.getLatestOffsetAndTimestamp(topic, msgKey);
        if (latestInfo == null) return false;

        long latestOffset = latestInfo[0];

        // Superseded: a newer record for this key exists
        if (latestOffset > recordOffset) return true;

        // Latest DELETE tombstone past its retention window
        if (record.getEventType() == EventType.DELETE) {
            long ageMs = System.currentTimeMillis() - latestInfo[1];
            return ageMs > (long) tombstoneRetentionDays * MS_PER_DAY;
        }

        return false;
    }

    private int estimateRecordSize(MessageRecord record) {
        int keyBytes  = record.getMsgKey() != null ? record.getMsgKey().length() : 0;
        int dataBytes = record.getData()   != null ? record.getData().length()   : 0;
        return 4 + keyBytes + 1 + 4 + dataBytes + 8 + 4;
    }
}
