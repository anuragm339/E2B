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

    public DownloadRefreshService(
            DownloadRefreshOrchestrator orchestrator,
            RefreshCoordinator refreshCoordinator,
            StorageEngine storage,
            BootstrapProgressTracker progress) {
        this.orchestrator = orchestrator;
        this.refreshCoordinator = refreshCoordinator;
        this.storage = storage;
        this.progress = progress;
    }

    /** Source fresh data, then refresh consumers for every affected topic. Never throws. */
    public DownloadRefreshResult runBootstrapAndRefresh() {
        progress.start("download-refresh", BootstrapProgressTracker.Phase.DOWNLOADING);
        DownloadRefreshResult result = orchestrator.bootstrap();
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
        for (String topic : topics) {
            try {
                refreshCoordinator.startRefresh(topic);
            } catch (Exception e) {
                log.error("event=download_refresh.refresh_trigger_failed topic={} err={}", topic, e.toString());
            }
        }
        progress.done();
        return result;
    }
}
