package com.messaging.storage.segment;

import com.messaging.common.exception.StorageException;
import com.messaging.storage.segment.Segment;

import java.nio.file.Path;
import java.util.List;

/**
 * Service for recovering storage state from disk (Phase 10).
 *
 * Responsibilities:
 * - Load existing segments from directory
 * - Validate segment integrity
 * - Determine which segments to seal
 * - Extract metadata from segment files
 */
public interface StorageRecoveryService {
    /**
     * Load all segments from a data directory.
     *
     * @param dataDir Directory containing segment files
     * @param topic Topic name
     * @param partition Partition number
     * @param maxSegmentSize Maximum segment size (for determining active segment)
     * @return Recovery result containing loaded segments
     * @throws StorageException if recovery fails
     */
    RecoveryResult recoverSegments(Path dataDir, String topic, int partition, long maxSegmentSize)
            throws StorageException;

    /**
     * Extract base offset from segment filename.
     *
     * @param filename Segment filename (e.g., "00000000000000000100.log")
     * @return Base offset
     * @throws StorageException if filename format is invalid
     */
    long extractOffsetFromFilename(String filename) throws StorageException;

    /**
     * Result of storage recovery operation.
     */
    class RecoveryResult {
        private final List<Segment> segments;
        private final Segment activeSegment;
        private final int totalSegments;

        public RecoveryResult(List<Segment> segments, Segment activeSegment) {
            this.segments = segments;
            this.activeSegment = activeSegment;
            this.totalSegments = segments.size();
        }

        public List<Segment> getSegments() {
            return segments;
        }

        public Segment getActiveSegment() {
            return activeSegment;
        }

        public int getTotalSegments() {
            return totalSegments;
        }

        public boolean hasActiveSegment() {
            return activeSegment != null;
        }
    }
}
