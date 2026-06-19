package com.messaging.broker.snapshot;

import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.common.api.StorageEngine;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Glue between the data-sourcing bootstrap and the consumer refresh: run the
 * {@link DownloadRefreshOrchestrator} to re-source authoritative data, then trigger the existing
 * RESET→replay→READY per topic so connected consumers receive the fresh copy.
 *
 * <p>Topics to refresh come from the snapshot manifest (SNAPSHOT path) or, for the incremental/cloud
 * paths, from whatever topics now exist in storage after the pull.
 */
@Singleton
public class DownloadRefreshService {
    private static final Logger log = LoggerFactory.getLogger(DownloadRefreshService.class);

    private final DownloadRefreshOrchestrator orchestrator;
    private final RefreshCoordinator refreshCoordinator;
    private final StorageEngine storage;
    private final BootstrapProgressTracker progress;
    private final BareMetalResetService bareMetalReset;

    public DownloadRefreshService(
            DownloadRefreshOrchestrator orchestrator,
            RefreshCoordinator refreshCoordinator,
            StorageEngine storage,
            BootstrapProgressTracker progress,
            BareMetalResetService bareMetalReset) {
        this.orchestrator = orchestrator;
        this.refreshCoordinator = refreshCoordinator;
        this.storage = storage;
        this.progress = progress;
        this.bareMetalReset = bareMetalReset;
    }

    /** Backward-compatible entry point — auto-selected download refresh. */
    public DownloadRefreshResult runBootstrapAndRefresh() {
        return runRefresh(RefreshType.PIPE_AND_PROVIDER_REFRESH);
    }

    /**
     * Dispatch a refresh by type. {@code LOCAL} replays the node's own segments (no download);
     * the rest wipe + re-source (auto or forced source) and then refresh consumers. Never throws.
     */
    public DownloadRefreshResult runRefresh(RefreshType type) {
        if (type.isBareMetal()) {
            log.warn("event=refresh.bare_metal_initiated — node will stop, wipe storage, and exit for restart");
            bareMetalReset.reset(); // async: stop everything → wipe → System.exit
            return DownloadRefreshResult.ok(null, null);
        }
        if (type.isLocal()) {
            return runLocalRefresh();
        }
        return runDownloadRefresh(type);
    }

    private DownloadRefreshResult runLocalRefresh() {
        progress.start("local-refresh", BootstrapProgressTracker.Phase.REFRESHING);
        Collection<String> topics = storage.getTopicNames();
        log.info("event=local_refresh.triggering_consumer_refresh topics={}", topics.size());
        refreshTopics(topics, "LOCAL");
        progress.done();
        return DownloadRefreshResult.ok(null, null); // no bootstrap source for a local refresh
    }

    private DownloadRefreshResult runDownloadRefresh(RefreshType type) {
        progress.start("download-refresh", BootstrapProgressTracker.Phase.DOWNLOADING);
        DownloadRefreshResult result = orchestrator.bootstrap(type.forcedSource());
        if (!result.isSuccess()) {
            progress.failed();
            log.warn("event=download_refresh.bootstrap_failed source={} err={} — skipping consumer refresh",
                    result.getSource(), result.getError());
            return result;
        }
        progress.setPhase(BootstrapProgressTracker.Phase.REFRESHING);
        Collection<String> topics = (result.getManifest() != null)
                ? result.getManifest().getTopicHeads().keySet()
                : storage.getTopicNames();
        log.info("event=download_refresh.triggering_consumer_refresh source={} topics={}",
                result.getSource(), topics.size());
        refreshTopics(topics, result.getSource().name());
        // Stay "running" until the consumer RESET→replay→READY actually finishes, so a second
        // download refresh can't start mid-replay. (isRefreshActive stays true through the ~60s
        // post-COMPLETED cleanup window, so this slightly over-waits — safe.)
        awaitRefreshesComplete(topics);
        progress.done();
        return result;
    }

    private static final long REFRESH_POLL_MS = 500;
    /** Bounded wait — comfortably past the refresh abort watchdog (10 min) + cleanup window. */
    private static final long REFRESH_MAX_WAIT_MS = 15L * 60L * 1000L;

    private void awaitRefreshesComplete(Collection<String> topics) {
        long deadline = System.currentTimeMillis() + REFRESH_MAX_WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            boolean anyActive = false;
            for (String topic : topics) {
                if (refreshCoordinator.isRefreshActive(topic)) {
                    anyActive = true;
                    break;
                }
            }
            if (!anyActive) {
                return;
            }
            try {
                Thread.sleep(REFRESH_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("event=download_refresh.await_refresh_timeout topics={}", topics.size());
    }

    private void refreshTopics(Collection<String> topics, String refreshType) {
        for (String topic : topics) {
            try {
                refreshCoordinator.startRefresh(topic, refreshType);
            } catch (Exception e) {
                log.error("event=refresh.trigger_failed topic={} err={}", topic, e.toString());
            }
        }
    }
}
