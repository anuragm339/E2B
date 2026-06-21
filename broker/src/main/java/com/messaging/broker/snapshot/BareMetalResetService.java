package com.messaging.broker.snapshot;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

/**
 * Bare-metal hard reset: stop the whole node, wipe the storage directory's contents, and exit.
 *
 * <p>The admin {@code BARE_METAL} refresh type drives this path (via {@code DownloadRefreshService}):
 * the connected consumers are RESET best-effort first, then {@link #reset()} tears the node down and
 * {@code System.exit}s. It is the destructive variant — distinct from the in-process
 * stop-network-server / wipe / re-source lifecycle that the other (SNAPSHOT/STREAM/CLOUD) refresh
 * types follow without exiting.
 *
 * <p>Order (per design): stop pipe + broker + everything FIRST so nothing holds the storage files,
 * THEN delete all contents, THEN exit. {@link ApplicationContext#stop()} runs every {@code @PreDestroy}
 * in dependency order — {@code PipeConnector.disconnect()}, {@code BrokerService.shutdown()},
 * {@code StorageEngine.close()} (flush + release handles) — so the wipe never touches an open file.
 *
 * <p>The node does NOT self-restart: it {@code System.exit}s and an external supervisor/operator
 * brings it back up (the broker container intentionally has no auto-restart policy). On the fresh
 * boot the empty node re-syncs from its parent/cloud through the normal startup path.
 *
 * <p>The directory itself is preserved; only its contents are removed.
 */
@Singleton
public class BareMetalResetService {
    private static final Logger log = LoggerFactory.getLogger(BareMetalResetService.class);

    private final ApplicationContext context;
    private final String dataDir;
    private final int exitCode;
    private final long settleMs;

    public BareMetalResetService(
            ApplicationContext context,
            @Value("${broker.storage.dataDir:./data}") String dataDir,
            @Value("${broker.bare-metal.exit-code:70}") int exitCode,
            @Value("${broker.bare-metal.settle-ms:500}") long settleMs) {
        this.context = context;
        this.dataDir = dataDir;
        this.exitCode = exitCode;
        this.settleMs = settleMs;
    }

    /**
     * Kick off the reset asynchronously on a non-context thread (it must survive
     * {@code context.stop()}), so the HTTP "INITIATED" response can flush first.
     */
    public void reset() {
        Thread t = new Thread(this::doReset, "bare-metal-reset");
        t.setDaemon(false);
        t.start();
    }

    private void doReset() {
        try {
            Thread.sleep(settleMs); // let the admin response flush before we tear the server down
            log.warn("event=bare_metal.stopping_all — stopping pipe, broker and storage");
            stopEverything();
            log.warn("event=bare_metal.wiping dir={}", dataDir);
            wipeContents(Paths.get(dataDir));
            log.warn("event=bare_metal.complete exitCode={} — external supervisor must restart the node", exitCode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("event=bare_metal.failed err={}", e.toString(), e);
        } finally {
            exit(exitCode);
        }
    }

    /** Stop every component (pipe, broker, storage) via the context's @PreDestroy chain. Overridable for tests. */
    protected void stopEverything() {
        context.stop();
    }

    /** Hard exit so an external supervisor restarts a fresh process. Overridable for tests. */
    protected void exit(int code) {
        System.exit(code);
    }

    /** Delete everything inside {@code dir}, keeping {@code dir} itself. */
    static void wipeContents(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        List<Path> top;
        try (var entries = Files.list(dir)) {
            top = entries.toList();
        }
        for (Path p : top) {
            deleteRecursively(p);
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
