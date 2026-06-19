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
        refreshTopics(topics);
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
        refreshTopics(topics);
        progress.done();
        return result;
    }

    private void refreshTopics(Collection<String> topics) {
        for (String topic : topics) {
            try {
                refreshCoordinator.startRefresh(topic);
            } catch (Exception e) {
                log.error("event=refresh.trigger_failed topic={} err={}", topic, e.toString());
            }
        }
    }
}
