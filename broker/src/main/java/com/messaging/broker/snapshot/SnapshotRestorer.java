package com.messaging.broker.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Restores a data snapshot ZIP (see {@link SnapshotBuilder}) into a child's data dir.
 *
 * <p>Crash-safe by construction: the ZIP is expanded into a sibling staging directory FIRST, the
 * manifest is validated, and only then is each topic folder atomically swapped into place (old
 * topic dir deleted, staged dir moved in). A failure mid-download/unzip therefore leaves the live
 * data dir untouched — never the "deleted old, no new" hole that a delete-then-fetch would create.
 *
 * <p>The restorer only lays down topic data. Resetting the parent-specific state files
 * (consumer-offsets, topology, pipe-offset, refresh/delivery state) and the {@code ack-store/} is
 * the orchestration's job — the snapshot does not contain them.
 */
@Singleton
public class SnapshotRestorer {
    private static final Logger log = LoggerFactory.getLogger(SnapshotRestorer.class);

    private static final String MANIFEST_ENTRY = "manifest.json";
    private static final String STAGING_DIR = ".snapshot-staging";

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    /**
     * Expand {@code snapshotZip} and swap its topic folders into {@code dataDir}.
     *
     * @return the snapshot manifest (watermarks etc.).
     */
    public SnapshotManifest restore(Path snapshotZip, Path dataDir) {
        Path staging = dataDir.resolve(STAGING_DIR);
        try {
            deleteRecursively(staging);
            Files.createDirectories(staging);

            // 1. Expand the whole ZIP into staging (no live data touched yet).
            SnapshotManifest manifest = unzipToStaging(snapshotZip, staging);
            if (manifest == null) {
                throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_RESTORE_FAILED,
                        "Snapshot is missing manifest.json").withContext("zip", snapshotZip.toString());
            }

            // 2. Atomic per-topic swap: delete the live topic dir, move the staged one in.
            try (var staged = Files.list(staging)) {
                List<Path> topicDirs = staged.filter(Files::isDirectory).toList();
                for (Path stagedTopic : topicDirs) {
                    Path target = dataDir.resolve(stagedTopic.getFileName());
                    deleteRecursively(target);
                    Files.move(stagedTopic, target);
                }
            }

            log.info("event=snapshot.restored zip={} topics={}", snapshotZip, manifest.getTopicHeads().size());
            return manifest;
        } catch (IOException e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_RESTORE_FAILED,
                    "Failed to restore snapshot " + snapshotZip, e)
                    .withContext("dataDir", dataDir.toString());
        } finally {
            try {
                deleteRecursively(staging);
            } catch (IOException ce) {
                log.warn("event=snapshot.staging_cleanup_failed staging={} err={}", staging, ce.toString());
            }
        }
    }

    private SnapshotManifest unzipToStaging(Path snapshotZip, Path staging) throws IOException {
        SnapshotManifest manifest = null;
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(snapshotZip))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                if (MANIFEST_ENTRY.equals(entry.getName())) {
                    manifest = objectMapper.readValue(readAll(zis), SnapshotManifest.class);
                    continue;
                }
                Path out = safeResolve(staging, entry.getName());
                Files.createDirectories(out.getParent());
                try (var os = Files.newOutputStream(out)) {
                    zis.transferTo(os);
                }
            }
        }
        return manifest;
    }

    /** Guard against Zip-Slip: the resolved path must stay within {@code base}. */
    private Path safeResolve(Path base, String entryName) throws IOException {
        Path resolved = base.resolve(entryName).normalize();
        if (!resolved.startsWith(base.normalize())) {
            throw new IOException("Illegal snapshot entry escapes staging dir: " + entryName);
        }
        return resolved;
    }

    private static byte[] readAll(InputStream is) throws IOException {
        return is.readAllBytes();
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
                    throw new RuntimeIoException(e);
                }
            });
        } catch (RuntimeIoException re) {
            throw re.io;
        }
    }

    /** Bridges IOException through the delete forEach lambda. */
    private static final class RuntimeIoException extends RuntimeException {
        final IOException io;
        RuntimeIoException(IOException io) {
            this.io = io;
        }
    }
}
