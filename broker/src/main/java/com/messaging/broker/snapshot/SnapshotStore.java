package com.messaging.broker.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Owns the on-disk location of the latest data snapshot ({@code <dataDir>/snapshots/latest.zip})
 * and the most-recently published manifest. Shared by {@link SnapshotScheduler} (writer) and the
 * snapshot serve endpoint (reader). Publishing is an atomic rename so a reader never observes a
 * half-written ZIP.
 */
@Singleton
public class SnapshotStore {

    private static final String MANIFEST_ENTRY = "manifest.json";

    private final Path snapshotDir;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private volatile SnapshotManifest latestManifest;

    public SnapshotStore(@Value("${broker.storage.dataDir:./data}") String dataDir) {
        this.snapshotDir = Paths.get(dataDir, "snapshots");
    }

    public Path latestZip() {
        return snapshotDir.resolve("latest.zip");
    }

    /** Temp target a build writes to before the atomic publish. */
    public Path tempZip() {
        return snapshotDir.resolve("latest.zip.tmp");
    }

    public boolean exists() {
        return Files.exists(latestZip());
    }

    /** Atomically replace the live snapshot with a freshly-built temp ZIP and cache its manifest. */
    public void publish(Path builtTempZip, SnapshotManifest manifest) throws IOException {
        Files.createDirectories(snapshotDir);
        Files.move(builtTempZip, latestZip(), StandardCopyOption.REPLACE_EXISTING);
        this.latestManifest = manifest;
    }

    /**
     * The current snapshot's manifest, or {@code null} if no snapshot exists. Uses the cached
     * manifest when available (set by the scheduler this run); otherwise reads it from the ZIP —
     * which is the case after a restart before the next scheduled build.
     */
    public SnapshotManifest currentManifest() {
        SnapshotManifest cached = latestManifest;
        if (cached != null) {
            return cached;
        }
        if (!exists()) {
            return null;
        }
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(latestZip()))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (MANIFEST_ENTRY.equals(entry.getName())) {
                    SnapshotManifest m = objectMapper.readValue(zis.readAllBytes(), SnapshotManifest.class);
                    this.latestManifest = m;
                    return m;
                }
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }
}
