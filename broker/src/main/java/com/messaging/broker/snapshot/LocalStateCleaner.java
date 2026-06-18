package com.messaging.broker.snapshot;

import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Resets a node's local state ahead of a download-refresh bootstrap.
 *
 * <p>The download-refresh re-sources authoritative data from upstream, so the PARENT-specific
 * state this node holds must be cleared first (it is the parent's, not this node's):
 * <ul>
 *   <li>{@code ack-store/} — RocksDB ACK + compaction CF; rebuilt from segments (backfill path).</li>
 *   <li>{@code consumer-offsets.properties} — this node's consumers re-sync from 0.</li>
 *   <li>{@code pipe-offset.properties} — reset; the pipe re-syncs from 0 (idempotent ingestion).</li>
 *   <li>{@code data-refresh-state.properties}, {@code delivery-state.properties} — stale in-flight state.</li>
 * </ul>
 *
 * <p>NOT cleared: {@code topology.properties} (this node's own identity from the registry) and, for
 * the snapshot path, topic data (the {@link SnapshotRestorer} swaps topic folders atomically).
 * {@link #clearTopicData} is the explicit full wipe used by the cloud / no-snapshot path.
 */
@Singleton
public class LocalStateCleaner {
    private static final Logger log = LoggerFactory.getLogger(LocalStateCleaner.class);

    /** State files reset on bootstrap. topology.properties is intentionally absent. */
    static final Set<String> STATE_FILES = Set.of(
            "consumer-offsets.properties",
            "pipe-offset.properties",
            "data-refresh-state.properties",
            "delivery-state.properties");

    static final String ACK_STORE_DIR = "ack-store";

    /** Directories that are never topic folders and must be preserved by {@link #clearTopicData}. */
    static final Set<String> NON_TOPIC_DIRS = Set.of(ACK_STORE_DIR, "snapshots");

    /** Reset RocksDB + the parent-specific state files. Leaves topic data and topology in place. */
    public void clearState(String dataDirStr) {
        Path dataDir = Paths.get(dataDirStr);
        try {
            deleteRecursively(dataDir.resolve(ACK_STORE_DIR));
            for (String f : STATE_FILES) {
                Files.deleteIfExists(dataDir.resolve(f));
            }
            log.info("event=bootstrap.state_cleared dataDir={}", dataDir);
        } catch (IOException e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_RESTORE_FAILED,
                    "Failed to clear local state for bootstrap", e)
                    .withContext("dataDir", dataDirStr);
        }
    }

    /** Full wipe of all topic folders (cloud / no-snapshot path). Preserves ack-store/snapshots dirs. */
    public void clearTopicData(String dataDirStr) {
        Path dataDir = Paths.get(dataDirStr);
        if (!Files.isDirectory(dataDir)) {
            return;
        }
        try (var entries = Files.list(dataDir)) {
            List<Path> topicDirs = entries
                    .filter(Files::isDirectory)
                    .filter(p -> !NON_TOPIC_DIRS.contains(p.getFileName().toString()))
                    .toList();
            for (Path topic : topicDirs) {
                deleteRecursively(topic);
            }
            log.info("event=bootstrap.topic_data_cleared dataDir={} topics={}", dataDir, topicDirs.size());
        } catch (IOException e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_RESTORE_FAILED,
                    "Failed to clear topic data for bootstrap", e)
                    .withContext("dataDir", dataDirStr);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIo(e);
                }
            });
        } catch (UncheckedIo u) {
            throw u.io;
        }
    }

    private static final class UncheckedIo extends RuntimeException {
        final IOException io;
        UncheckedIo(IOException io) {
            this.io = io;
        }
    }
}
