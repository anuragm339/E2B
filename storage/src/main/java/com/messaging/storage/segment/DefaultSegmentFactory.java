package com.messaging.storage.segment;

import com.messaging.common.exception.StorageException;
import com.messaging.storage.segment.SegmentFactory;
import com.messaging.storage.metadata.SegmentMetadataStore;
import com.messaging.storage.segment.Segment;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Default implementation of SegmentFactory (Phase 10).
 *
 * Responsibilities:
 * - Create new segments with proper naming
 * - Initialize segment files
 * - Set up metadata tracking
 */
@Singleton
public class DefaultSegmentFactory implements SegmentFactory {
    private static final Logger log = LoggerFactory.getLogger(DefaultSegmentFactory.class);

    @Override
    public Segment createSegment(
            Path dataDir,
            String topic,
            int partition,
            long baseOffset,
            SegmentMetadataStore metadataStore) throws StorageException {

        String filename = generateSegmentFilename(baseOffset);
        Path logPath = dataDir.resolve(filename);
        Path indexPath = dataDir.resolve(filename.replace(".log", ".index"));

        log.info("Creating new segment: topic={}, partition={}, baseOffset={}, logPath={}",
                topic, partition, baseOffset, logPath);

        // Note: maxSegmentSize is passed to Segment constructor but not used during creation
        // It's used later to determine when to roll segments
        // We use a default value here since the factory doesn't need to know the max size
        long maxSegmentSize = 1073741824L; // 1GB default

        return new Segment(logPath, indexPath, baseOffset, maxSegmentSize, topic, partition);
    }

    @Override
    public String generateSegmentFilename(long baseOffset) {
        // Format: 20-digit zero-padded offset + ".log"
        // Example: 00000000000000000100.log
        return String.format("%020d.log", baseOffset);
    }
}
