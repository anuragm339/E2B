package com.messaging.storage.segment;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.StorageException;
import com.messaging.storage.segment.StorageRecoveryService;
import com.messaging.storage.metadata.SegmentMetadataStore;
import com.messaging.storage.segment.Segment;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Default implementation of StorageRecoveryService (Phase 10).
 *
 * Responsibilities:
 * - Load existing segments from directory
 * - Validate segment integrity
 * - Determine which segments to seal
 * - Extract metadata from segment files
 */
@Singleton
public class DefaultStorageRecoveryService implements StorageRecoveryService {
    private static final Logger log = LoggerFactory.getLogger(DefaultStorageRecoveryService.class);
    private static final Pattern SEGMENT_PATTERN = Pattern.compile("(\\d{20})\\.log");

    @Override
    public RecoveryResult recoverSegments(Path dataDir, String topic, int partition, long maxSegmentSize)
            throws StorageException {

        // Validate directory exists
        if (!Files.exists(dataDir)) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "Data directory does not exist: " + dataDir);
        }

        if (!Files.isDirectory(dataDir)) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "Path is not a directory: " + dataDir);
        }

        List<Path> logFiles = new ArrayList<>();

        // Collect all log files
        try (Stream<Path> paths = Files.list(dataDir)) {
            paths.filter(path -> path.toString().endsWith(".log"))
                    .forEach(logFiles::add);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "Failed to list segment files in " + dataDir, e);
        }

        // Sort by filename (which contains offset)
        logFiles.sort(Path::compareTo);

        List<Segment> segments = new ArrayList<>();
        Segment activeSegment = null;

        // Load segments - seal all except the last one
        for (int i = 0; i < logFiles.size(); i++) {
            Path logPath = logFiles.get(i);
            boolean isLastSegment = (i == logFiles.size() - 1);

            try {
                long baseOffset = extractOffsetFromFilename(logPath.getFileName().toString());
                Path indexPath = dataDir.resolve(String.format("%020d.index", baseOffset));

                // Note: SegmentMetadataStore is not injected here to avoid coupling
                // The caller (SegmentManager) should pass it when creating segments
                Segment segment = new Segment(logPath, indexPath, baseOffset, maxSegmentSize, topic, partition);

                // Only seal old segments, keep the last one active
                if (!isLastSegment) {
                    segment.seal();
                    log.info("Loaded and sealed segment: topic={}, partition={}, baseOffset={}",
                            topic, partition, baseOffset);
                } else {
                    activeSegment = segment;
                    log.info("Loaded active segment: topic={}, partition={}, baseOffset={}",
                            topic, partition, baseOffset);
                }

                segments.add(segment);

            } catch (StorageException e) {
                // Log error but continue with other segments (graceful degradation)
                log.error("Failed to load segment: {} - {}", logPath, e.getMessage());
                // Don't rethrow - allow recovery to continue with valid segments
            }
        }

        log.info("Recovery completed for topic={}, partition={}: loaded {} segments, active={}",
                topic, partition, segments.size(), (activeSegment != null ? activeSegment.getBaseOffset() : "none"));

        return new RecoveryResult(segments, activeSegment);
    }

    @Override
    public long extractOffsetFromFilename(String filename) throws StorageException {
        Matcher matcher = SEGMENT_PATTERN.matcher(filename);
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }

        // Invalid segment filename indicates corrupted filesystem state
        throw new StorageException(ErrorCode.STORAGE_CORRUPTION,
                "Invalid segment filename: " + filename + " (expected format: NNNNNNNNNNNNNNNNNNNN.log)");
    }
}
