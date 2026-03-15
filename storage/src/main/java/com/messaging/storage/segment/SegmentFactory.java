package com.messaging.storage.segment;

import com.messaging.common.exception.StorageException;
import com.messaging.storage.metadata.SegmentMetadataStore;
import com.messaging.storage.segment.Segment;

import java.nio.file.Path;

/**
 * Factory for creating storage segments (Phase 10).
 *
 * Responsibilities:
 * - Create new segments with proper naming
 * - Initialize segment files
 * - Set up metadata tracking
 */
public interface SegmentFactory {
    /**
     * Create a new segment.
     *
     * @param dataDir Directory for segment files
     * @param topic Topic name
     * @param partition Partition number
     * @param baseOffset Starting offset for this segment
     * @param metadataStore Metadata store for tracking segment metadata
     * @return Newly created segment
     * @throws StorageException if creation fails
     */
    Segment createSegment(
            Path dataDir,
            String topic,
            int partition,
            long baseOffset,
            SegmentMetadataStore metadataStore) throws StorageException;

    /**
     * Generate segment filename from base offset.
     *
     * @param baseOffset Starting offset
     * @return Filename (e.g., "00000000000000000100.log")
     */
    String generateSegmentFilename(long baseOffset);
}
