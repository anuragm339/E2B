package com.messaging.broker.snapshot;

import com.messaging.common.api.StorageEngine;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically builds the data snapshot a child POS downloads to bootstrap (see
 * {@link SnapshotBuilder}). Follows the {@code CompactionScheduler} convention: single-flight
 * skip-don't-queue, every skip logged. Disabled by default on leaf nodes via config; a parent-
 * capable POS turns it on.
 *
 * <p>Builds into {@link SnapshotStore#tempZip()} then atomically publishes, so the serve endpoint
 * never sees a half-written ZIP. The {@code snapshots/} dir is itself excluded from the snapshot.
 */
@Singleton
public class SnapshotScheduler {
    private static final Logger log = LoggerFactory.getLogger(SnapshotScheduler.class);

    private final boolean enabled;
    private final String dataDir;
    private final StorageEngine storage;
    private final SnapshotBuilder builder;
    private final SnapshotStore store;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SnapshotScheduler(
            @Value("${broker.snapshot.enabled:false}") boolean enabled,
            @Value("${broker.storage.dataDir:./data}") String dataDir,
            StorageEngine storage,
            SnapshotBuilder builder,
            SnapshotStore store) {
        this.enabled = enabled;
        this.dataDir = dataDir;
        this.storage = storage;
        this.builder = builder;
        this.store = store;
    }

    @Scheduled(
            fixedDelay = "${broker.snapshot.interval:6h}",
            initialDelay = "${broker.snapshot.initial-delay:10m}")
    public void scheduled() {
        if (!enabled) {
            return;
        }
        buildNow();
    }

    /**
     * Build and publish a snapshot now. Returns the manifest, or {@code null} if skipped
     * (another build in flight) or it failed (logged, never thrown to the scheduler thread).
     */
    public SnapshotManifest buildNow() {
        if (!running.compareAndSet(false, true)) {
            log.info("event=snapshot.build_skipped reason=already_running");
            return null;
        }
        try {
            Map<String, Long> heads = new LinkedHashMap<>();
            for (String topic : storage.getTopicNames()) {
                heads.put(topic, storage.getCurrentOffset(topic, 0));
            }
            SnapshotManifest manifest = builder.build(Paths.get(dataDir), store.tempZip(), heads);
            store.publish(store.tempZip(), manifest);
            log.info("event=snapshot.published topics={}", heads.size());
            return manifest;
        } catch (Exception e) {
            // Never propagate to the Micronaut scheduled pool — log and try again next tick.
            log.error("event=snapshot.build_failed err={}", e.toString(), e);
            return null;
        } finally {
            running.set(false);
        }
    }
}
