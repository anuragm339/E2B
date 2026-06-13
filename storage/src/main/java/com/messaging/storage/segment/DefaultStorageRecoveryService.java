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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    // Matches both regular segments (00000000000000000000.log) and compacted segments
    // (00000000000000000000.compacted.log) produced by CompactionRewriter.
    private static final Pattern SEGMENT_PATTERN = Pattern.compile("(\\d{20})(?:\\.compacted)?\\.log");

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

        // Delete any leftover .compacting.{log,index} files from a previous crashed compaction run.
        // These are incomplete staging files — they are never valid segments.
        // CompactionRewriter also cleans them up at the start of the next run, but doing it here
        // ensures they never cause spurious "invalid segment filename" errors during startup.
        try (Stream<Path> paths = Files.list(dataDir)) {
            paths.filter(p -> p.getFileName().toString().contains(".compacting"))
                 .forEach(p -> {
                     try {
                         Files.deleteIfExists(p);
                         log.warn("Deleted leftover compaction staging file: {}", p);
                     } catch (IOException e) {
                         log.warn("Could not delete compaction staging file {}: {}", p, e.getMessage());
                     }
                 });
        } catch (IOException e) {
            log.warn("Could not scan for leftover compaction staging files in {}: {}", dataDir, e.getMessage());
        }

        List<Path> logFiles = new ArrayList<>();

        // Collect only files that match the segment filename pattern.
        // This naturally excludes .compacting.log (staging) files and any other non-segment files.
        try (Stream<Path> paths = Files.list(dataDir)) {
            paths.filter(p -> SEGMENT_PATTERN.matcher(p.getFileName().toString()).matches())
                    .forEach(logFiles::add);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.STORAGE_IO_ERROR,
                    "Failed to list segment files in " + dataDir, e);
        }

        // Sort by filename (which contains offset)
        logFiles.sort(Path::compareTo);

        // Deduplicate by base offset: a crash between compaction's finalise and cleanup can
        // leave BOTH X.log and X.compacted.log on disk. Loading both used to make the second
        // silently shadow the first in SegmentManager's map (nondeterministic data view) and
        // leak the loser's open channels. The compacted file is the post-swap source of truth.
        Map<Long, Path> byBase = new LinkedHashMap<>();
        for (Path logPath : logFiles) {
            String name = logPath.getFileName().toString();
            long base;
            try {
                base = extractOffsetFromFilename(name);
            } catch (StorageException e) {
                log.error("Skipping unparseable segment filename: {}", name);
                continue;
            }
            Path existing = byBase.get(base);
            if (existing == null) {
                byBase.put(base, logPath);
            } else {
                boolean preferNew = name.endsWith(".compacted.log");
                Path winner = preferNew ? logPath : existing;
                Path loser = preferNew ? existing : logPath;
                byBase.put(base, winner);
                log.warn("Duplicate segment files for baseOffset={}: keeping {} and IGNORING {} " +
                         "(crash between compaction finalise and cleanup) — delete the ignored " +
                         "file manually after verifying the kept one",
                        base, winner.getFileName(), loser.getFileName());
            }
        }
        logFiles = new ArrayList<>(byBase.values());
        logFiles.sort(Path::compareTo);

        List<Segment> segments = new ArrayList<>();
        Segment activeSegment = null;

        // Load segments - seal all except the last one
        for (int i = 0; i < logFiles.size(); i++) {
            Path logPath = logFiles.get(i);
            boolean isLastSegment = (i == logFiles.size() - 1);

            try {
                long baseOffset = extractOffsetFromFilename(logPath.getFileName().toString());
                Path indexPath = dataDir.resolve(logPath.getFileName().toString().replace(".log", ".index"));

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
