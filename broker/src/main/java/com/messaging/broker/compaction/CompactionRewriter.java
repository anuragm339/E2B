package com.messaging.broker.compaction;

import com.messaging.common.exception.MessagingException;
import com.messaging.common.exception.StorageException;
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
 * <h2>Memory-efficient I/O path</h2>
 * <p>Source segments are read through {@link CompactionSegmentReader}: a single fixed-size 256 KB
 * {@link java.nio.ByteBuffer} streams through each log file sequentially. No {@code MappedByteBuffer}
 * is touched and no per-record heap allocation occurs beyond the one {@link MessageRecord} being
 * evaluated at any instant.
 *
 * <p>The compacted output is written through {@link CompactionSegmentWriter}: a 256 KB write-coalescing
 * buffer drains to the staging {@link java.nio.channels.FileChannel}; {@code FileChannel.force()} is
 * called <em>once</em> at the end of the run (not per-record). The staging file is atomically renamed
 * into its final position only after a successful write.
 *
 * <p>As a result, peak compaction memory is bounded by:
 * <ul>
 *   <li>256 KB read buffer</li>
 *   <li>256 KB + 64 KB write buffers</li>
 *   <li>One decoded {@link MessageRecord} at a time</li>
 *   <li>Index offset arrays: ~12 B per record (O(1 MB) for a 1 GB segment with 10 KB avg records)</li>
 * </ul>
 *
 * <p>The new segment is written to a {@code .compacting} staging path so a crash mid-rewrite
 * does not corrupt live data. The recovery service recognises {@code .compacted.log} as a
 * valid segment on restart.
 */
@Singleton
public class CompactionRewriter {

    private static final Logger log = LoggerFactory.getLogger(CompactionRewriter.class);
    private static final long MS_PER_DAY = 86_400_000L;

    // Optional — Micronaut injects when PipeConsistency wiring is present. Allowing null keeps
    // this class testable in isolation (Spock mocks pass nothing).
    @jakarta.inject.Inject
    com.messaging.broker.consistency.HashCache pipeConsistencyHashCache;

    public static class CompactionResult {
        public final int recordsRemoved;
        public final int tombstonesRemoved;
        public final long bytesRead;
        public final long bytesWritten;
        public final long bytesReclaimed;
        public final int segmentsReplaced;
        /** True when at least one live DELETE tombstone was kept because it has not yet aged out.
         *  The scheduler must NOT advance the checkpoint in this case so the segment is revisited
         *  on the next run, when the tombstone may have finally expired. */
        public final boolean hadUnexpiredTombstones;

        public CompactionResult(
                int recordsRemoved,
                int tombstonesRemoved,
                long bytesRead,
                long bytesWritten,
                long bytesReclaimed,
                int segmentsReplaced,
                boolean hadUnexpiredTombstones) {
            this.recordsRemoved       = recordsRemoved;
            this.tombstonesRemoved    = tombstonesRemoved;
            this.bytesRead            = bytesRead;
            this.bytesWritten         = bytesWritten;
            this.bytesReclaimed       = bytesReclaimed;
            this.segmentsReplaced     = segmentsReplaced;
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
            return new CompactionResult(0, 0, 0L, 0L, 0L, 0, false);
        }

        Runtime rt = Runtime.getRuntime();
        long heapBefore = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        log.info("event=compaction_rewrite_start topic={} partition={} candidates={} heap={}MB/{}MB",
                 topic, partition, candidates.size(), heapBefore, rt.maxMemory() / (1024 * 1024));

        long firstBaseOffset = candidates.stream()
                .mapToLong(Segment::getBaseOffset)
                .min()
                .orElseThrow();

        Path partitionDataDir = segmentManager.getDataDir();

        // Total on-disk bytes across all source segments — used only for metrics/logging
        long totalInputBytes = candidates.stream()
                .mapToLong(Segment::getSize)
                .sum();

        // Staging paths: written first, renamed to final paths only on success.
        // The .compacting suffix distinguishes these from the live .compacted.log files
        // recognised by DefaultStorageRecoveryService.
        Path stagingLogPath   = partitionDataDir.resolve(
                String.format("%020d.compacting.log",   firstBaseOffset));
        Path stagingIndexPath = partitionDataDir.resolve(
                String.format("%020d.compacting.index", firstBaseOffset));

        // Final paths: recognised by DefaultStorageRecoveryService on restart
        Path finalLogPath   = partitionDataDir.resolve(
                String.format("%020d.compacted.log",   firstBaseOffset));
        Path finalIndexPath = partitionDataDir.resolve(
                String.format("%020d.compacted.index", firstBaseOffset));

        // Remove any leftover STAGING files from a previous crashed run.
        // Do NOT delete the final (.compacted.*) files here — they may still be live in the
        // SegmentManager and readable by concurrent delivery threads via open FileChannels.
        // finalise() uses ATOMIC_MOVE (rename(2)) which atomically replaces the destination,
        // so pre-deleting the final paths is unnecessary and creates a race window where
        // findIndexPath() sees no index file covering the consumer's current offset.
        deleteIfExists(stagingLogPath);
        deleteIfExists(stagingIndexPath);

        int  removedCount            = 0;
        int  tombstonesRemovedCount  = 0;
        long reclaimedBytes          = 0L;
        int  survivorCount           = 0;
        long bytesStreamedIn         = 0L;
        boolean hadUnexpiredTombstone = false;

        // PipeConsistency: accumulate rolling hash over survivors in append order so the merged
        // segment's hash matches what cloud-server's `projection=compacted` will produce over the
        // same offset range.
        byte[] rollingHash = com.messaging.common.hash.RecordHasher.EMPTY_HASH.clone();

        // The compacted segment's maxSize must accommodate all survivors.
        // Use the combined on-disk size of all candidates as the ceiling — survivors can never
        // exceed total input bytes. Guard against empty/zero-size inputs.
        long compactedMaxSize = Math.max(totalInputBytes, segmentManager.getMaxSegmentSize());

        // Guard against the partition directory disappearing after SegmentManager construction
        // (container volume remount, manual cleanup, etc.). CREATE uses StandardOpenOption.CREATE
        // which does not create parents — if the dir is missing we get NoSuchFileException.
        try {
            Files.createDirectories(partitionDataDir);
        } catch (IOException e) {
            throw new StorageException(com.messaging.common.exception.ErrorCode.STORAGE_IO_ERROR,
                    "CompactionRewriter: cannot create partition directory: " + partitionDataDir, e);
        }

        CompactionSegmentWriter writer;
        try {
            writer = new CompactionSegmentWriter(stagingLogPath, stagingIndexPath);
        } catch (StorageException e) {
            throw e;
        }

        try {
            // ── Stream through each source segment ──────────────────────────
            for (Segment seg : candidates) {
                Path logPath   = seg.getLogPath();
                Path indexPath = seg.getIndexPath();

                bytesStreamedIn += seg.getSize();

                try (CompactionSegmentReader reader =
                             new CompactionSegmentReader(logPath, indexPath)) {

                    while (reader.hasNext()) {
                        MessageRecord record = reader.next();
                        if (record == null) break;

                        long actualOffset = record.getOffset();

                        if (isEligibleForDeletion(topic, record.getMsgKey(), actualOffset, record,
                                compactionIndex, tombstoneRetentionDays)) {
                            removedCount++;
                            if (record.getEventType() == EventType.DELETE) {
                                tombstonesRemovedCount++;
                            }
                            reclaimedBytes += estimateRecordSize(record);
                        } else {
                            writer.append(record);
                            survivorCount++;
                            // Fold this survivor into the rolling hash in append order.
                            int recCrc = com.messaging.common.hash.RecordHasher.recordCrc(
                                    actualOffset,
                                    record.getMsgKey(),
                                    (char) record.getEventType().getCode(),
                                    record.getData());
                            rollingHash = com.messaging.common.hash.RecordHasher.combine(rollingHash, recCrc);
                            // Track whether a live DELETE tombstone was kept within retention window
                            if (record.getEventType() == EventType.DELETE) {
                                long[] latestInfo = compactionIndex.getLatestOffsetAndTimestamp(
                                        topic, record.getMsgKey());
                                if (latestInfo != null && latestInfo[0] == actualOffset) {
                                    hadUnexpiredTombstone = true;
                                }
                            }
                        }
                    }
                } catch (StorageException e) {
                    // Surface as MessagingException for caller handling
                    throw e;
                }
            }

            // ── All records eligible for deletion ───────────────────────────
            // Installing an empty segment would stall consumers whose read offset equals the base
            // offset (fromOffset > storageHead with 0 records). Remove the input segments from the
            // manager without installing any replacement.
            if (survivorCount == 0) {
                try { writer.close(); } catch (Exception ignored) {}
                deleteIfExists(stagingLogPath);
                deleteIfExists(stagingIndexPath);

                segmentManager.removeSegments(candidates);

                String removedOffsets = candidates.stream()
                        .map(s -> String.valueOf(s.getBaseOffset()))
                        .reduce((a, b) -> a + "," + b).orElse("none");
                log.warn("event=compaction_all_deleted topic={} partition={} " +
                         "deletedOffsets=[{}] recordsRemoved={} segments={} freed=~{}MB " +
                         "— no replacement segment installed; any consumer committed inside " +
                         "this offset range will stall until offset reset",
                         topic, partition, removedOffsets, removedCount, candidates.size(),
                         totalInputBytes / (1024 * 1024));

                return new CompactionResult(
                        removedCount, tombstonesRemovedCount,
                        bytesStreamedIn, 0L, reclaimedBytes,
                        candidates.size(), false);
            }

            // ── Flush, fsync, and rename staging files ───────────────────────
            try {
                writer.close();  // flush + fsync
            } catch (IOException e) {
                throw new StorageException(com.messaging.common.exception.ErrorCode.STORAGE_IO_ERROR,
                        "CompactionRewriter: failed to fsync compacted output for topic=" + topic, e);
            }

            writer.finalise(finalLogPath, finalIndexPath);

        } catch (MessagingException e) {
            try { writer.close(); } catch (Exception ignored) {}
            deleteIfExists(stagingLogPath);
            deleteIfExists(stagingIndexPath);
            throw e;
        }

        // ── Open the finalised segment and install it via replaceSegments ───
        // The Segment constructor reads file headers and recovers the index.
        Segment compactedSegment = new Segment(
                finalLogPath, finalIndexPath,
                firstBaseOffset, compactedMaxSize, topic, partition);

        compactedSegment.seal();

        // PipeConsistency: install the rolling hash we accumulated over survivors,
        // and bump the compaction epoch (max input epoch + 1) so HashCache invalidation
        // and projection routing work correctly downstream.
        compactedSegment.installRollingHash(rollingHash, survivorCount);
        int maxInputEpoch = 0;
        for (Segment in : candidates) {
            int ep = segmentManager.getSegmentCompactionEpoch(in.getBaseOffset());
            if (ep > maxInputEpoch) {
                maxInputEpoch = ep;
            }
        }
        compactedSegment.setCompactionEpoch(maxInputEpoch + 1);

        segmentManager.replaceSegments(candidates, compactedSegment);

        // PipeConsistency: invalidate any cached hashes whose range overlaps the compacted window
        // so child brokers don't see stale pre-compaction hashes on their next call.
        if (pipeConsistencyHashCache != null) {
            long compactedLo = candidates.get(0).getBaseOffset();
            long compactedHi = compactedSegment.getNextOffset() - 1;
            pipeConsistencyHashCache.invalidateOverlapping(topic, compactedLo, compactedHi);
        }

        long bytesWritten = writer.getBytesWritten();
        long mmapFreedMB  = (totalInputBytes - bytesWritten) / (1024 * 1024);
        long heapAfter = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);

        log.info("event=compaction_rewrite_finish topic={} partition={} removed={} (tombstones={}) " +
                 "bytesRead={} bytesWritten={} reclaimed={}B survivors={} " +
                 "unexpiredTombstones={} freed=~{}MB heapBefore={}MB heapAfter={}MB",
                topic, partition, removedCount, tombstonesRemovedCount,
                bytesStreamedIn, bytesWritten, reclaimedBytes, survivorCount,
                hadUnexpiredTombstone, mmapFreedMB, heapBefore, heapAfter);

        return new CompactionResult(
                removedCount, tombstonesRemovedCount,
                bytesStreamedIn, bytesWritten, reclaimedBytes,
                candidates.size(), hadUnexpiredTombstone);
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

    private void deleteIfExists(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warn("CompactionRewriter: could not delete file: {}", path, e);
        }
    }
}
