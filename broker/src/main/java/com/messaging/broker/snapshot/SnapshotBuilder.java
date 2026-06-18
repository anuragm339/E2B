package com.messaging.broker.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Builds a data snapshot ZIP of "pure topic data" for POS→POS bootstrap.
 *
 * <p>INCLUDES: every per-topic subfolder (its {@code segment_metadata.db} + {@code partition-0/}
 * segment + index files) plus a {@code manifest.json} with per-topic head watermarks.
 *
 * <p>EXCLUDES (agreed design — these are the PARENT's state, not the child's, and the child
 * rebuilds or re-derives them): {@code ack-store/} (RocksDB ACK + compaction CF), all top-level
 * state files ({@code consumer-offsets/topology/data-refresh-state/delivery-state/pipe-offset
 * .properties}), {@code events.db} (cloud-resident), and logs. The simple, robust rule is:
 * include a file only if it lives inside a topic subfolder (relative depth ≥ 2) whose top-level
 * directory is not excluded; every top-level file is dropped.
 */
@Singleton
public class SnapshotBuilder {
    private static final Logger log = LoggerFactory.getLogger(SnapshotBuilder.class);

    /** Top-level directories never included in a snapshot. */
    private static final Set<String> EXCLUDED_TOP_DIRS = Set.of("ack-store", "snapshots");

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    /**
     * Build a snapshot ZIP of {@code dataDir} into {@code outputZip}.
     *
     * @param topicHeads topic -> head offset captured by the caller (which has the storage engine),
     *                   embedded in the manifest as the bootstrap watermark.
     * @return the manifest written into the ZIP.
     */
    public SnapshotManifest build(Path dataDir, Path outputZip, Map<String, Long> topicHeads) {
        SnapshotManifest manifest = new SnapshotManifest(System.currentTimeMillis(), topicHeads);
        try {
            if (outputZip.getParent() != null) {
                Files.createDirectories(outputZip.getParent());
            }
            try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(outputZip))) {
                // Manifest first so a reader can validate before streaming the (large) data entries.
                zos.putNextEntry(new ZipEntry("manifest.json"));
                zos.write(objectMapper.writeValueAsBytes(manifest));
                zos.closeEntry();

                try (Stream<Path> walk = Files.walk(dataDir)) {
                    walk.filter(Files::isRegularFile)
                            .filter(p -> includeInSnapshot(dataDir, p))
                            .forEach(p -> addEntry(zos, dataDir, p));
                }
            }
            log.info("event=snapshot.built outputZip={} topics={} sizeBytes={}",
                    outputZip, manifest.getTopicHeads().size(), sizeOf(outputZip));
            return manifest;
        } catch (IOException e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_BUILD_FAILED,
                    "Failed to build snapshot ZIP at " + outputZip, e)
                    .withContext("dataDir", dataDir.toString());
        }
    }

    /** Include only files inside a non-excluded topic subfolder (depth ≥ 2). Top-level files dropped. */
    boolean includeInSnapshot(Path dataDir, Path file) {
        Path rel = dataDir.relativize(file);
        if (rel.getNameCount() < 2) {
            return false; // top-level file (state properties, events.db, logs, .DS_Store, the zip)
        }
        String top = rel.getName(0).toString();
        return !EXCLUDED_TOP_DIRS.contains(top);
    }

    private void addEntry(ZipOutputStream zos, Path dataDir, Path file) {
        String entryName = dataDir.relativize(file).toString().replace('\\', '/');
        try {
            zos.putNextEntry(new ZipEntry(entryName));
            try (var in = Files.newInputStream(file)) {
                in.transferTo(zos);
            }
            zos.closeEntry();
        } catch (IOException e) {
            throw new DataRefreshException(ErrorCode.DATA_REFRESH_SNAPSHOT_BUILD_FAILED,
                    "Failed to add snapshot entry " + entryName, e);
        }
    }

    private static long sizeOf(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            return -1;
        }
    }
}
