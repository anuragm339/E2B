package com.messaging.broker.snapshot;

import com.messaging.broker.consumer.RefreshCoordinator;
import com.messaging.broker.legacy.LegacyClientConfig;
import com.messaging.common.api.NetworkServer;
import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.NetworkException;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
    private final NetworkServer networkServer;
    private final LegacyClientConfig legacyClientConfig;

    public DownloadRefreshService(
            DownloadRefreshOrchestrator orchestrator,
            RefreshCoordinator refreshCoordinator,
            StorageEngine storage,
            BootstrapProgressTracker progress,
            BareMetalResetService bareMetalReset,
            NetworkServer networkServer,
            LegacyClientConfig legacyClientConfig) {
        this.orchestrator = orchestrator;
        this.refreshCoordinator = refreshCoordinator;
        this.storage = storage;
        this.progress = progress;
        this.bareMetalReset = bareMetalReset;
        this.networkServer = networkServer;
        this.legacyClientConfig = legacyClientConfig;
    }

    /**
     * Fresh-install / empty-boot entry — auto-selected re-source, but the consumer network server is
     * NOT stopped: the node is empty (nothing to wipe, no consumer-offset race to protect), so stopping
     * it would only lock consumers out for the whole load. Data arrives over the normal pipe.
     */
    public DownloadRefreshResult runBootstrapAndRefresh() {
        // Label these consumer refreshes FRESH_INSTALL (not the raw bootstrap source) so the dashboard
        // distinguishes a boot-time fresh-install bootstrap from an operator-triggered re-source — both
        // may pull from the same source (e.g. CLOUD_SYNC on a root node), but they mean different things
        // operationally. Non-LOCAL, so the settled/drained replay gate still applies.
        return runDownloadRefresh(RefreshType.PIPE_AND_PROVIDER_REFRESH, false, "FRESH_INSTALL");
    }

    /**
     * Dispatch a refresh by type. {@code LOCAL} replays the node's own segments (no download);
     * the rest wipe + re-source (auto or forced source) and then refresh consumers. Never throws.
     */
    public DownloadRefreshResult runRefresh(RefreshType type) {
        if (type.isLocal()) {
            return runLocalRefresh();
        }
        if (type.isBareMetal()) {
            return runBareMetalRefresh();
        }
        // Admin refresh of a populated node: stop the consumer transport during the wipe (real ACK race).
        // No label override → consumers are refreshed under the actual bootstrap source (CLOUD_SYNC /
        // PIPE_AND_PROVIDER_*), which is exactly what the operator asked for.
        return runDownloadRefresh(type, true, null);
    }

    /**
     * Hard reset: best-effort RESET the connected consumers so they drop stale data, then hand off to
     * {@link BareMetalResetService#reset()} which stops pipe+broker+storage, wipes the data dir, and
     * {@code System.exit}s. An external supervisor restarts the (now empty) node, which re-sources via
     * the normal startup path. We do NOT await RESET ACKs — the process is about to exit and the wipe
     * removes ALL state, so there is nothing to recover.
     */
    private DownloadRefreshResult runBareMetalRefresh() {
        Collection<String> topics = storage.getTopicNames();
        log.warn("event=bare_metal.requested topics={} — RESET consumers, then stop+wipe+exit", topics.size());
        refreshTopics(topics, "BARE_METAL");
        bareMetalReset.reset();
        return DownloadRefreshResult.ok(null, null); // node is exiting; no bootstrap source
    }

    private DownloadRefreshResult runLocalRefresh() {
        progress.start("local-refresh", BootstrapProgressTracker.Phase.REFRESHING);
        Collection<String> topics = storage.getTopicNames();
        log.info("event=local_refresh.triggering_consumer_refresh topics={}", topics.size());
        refreshTopics(topics, "LOCAL");
        progress.done();
        return DownloadRefreshResult.ok(null, null); // no bootstrap source for a local refresh
    }

    private DownloadRefreshResult runDownloadRefresh(RefreshType type, boolean stopServer,
                                                     String refreshLabelOverride) {
        progress.start("download-refresh", BootstrapProgressTracker.Phase.DOWNLOADING);
        // When stopServer (admin refresh of a populated node): take the consumer transport DOWN for the
        // whole wipe + re-source window so no ACK can advance consumer offsets while local state is
        // deleted; RESET→replay→READY runs only after it is back up. resumeAccepting() is in a finally.
        // Fresh-boot passes stopServer=false — the empty node has nothing to protect and consumers stay
        // connected (held by the refresh) instead of thrashing for the whole load.
        DownloadRefreshResult result;
        if (stopServer) {
            stopNetworkServer();
        }
        try {
            result = orchestrator.bootstrap(type.forcedSource());
        } finally {
            if (stopServer) {
                resumeNetworkServer();
            }
        }
        if (!result.isSuccess()) {
            // Failure → drop the re-sourcing health gate and fall back to normal pipe transfer: the
            // pipe (never stopped) keeps streaming, so the node converges via normal delivery. Never
            // stuck-DOWN, never a crash-loop — the bootstrap is an optimisation, not a hard dependency.
            progress.failed();
            log.warn("event=download_refresh.bootstrap_failed source={} err={} — falling back to normal pipe transfer",
                    result.getSource(), result.getError());
            return result;
        }
        progress.setPhase(BootstrapProgressTracker.Phase.REFRESHING);
        // Snapshot path: topics are known synchronously from the restored manifest. STREAM/CLOUD
        // (pipe-only): the data streams in asynchronously, so storage is typically EMPTY at this
        // instant — fall back to the CONFIGURED topic set so we still RESET→replay→READY the right
        // topics (the dynamic settled-target waits for each topic's data to arrive over the pipe).
        Collection<String> topics = (result.getManifest() != null)
                ? result.getManifest().getTopicHeads().keySet()
                : streamRefreshTopics();
        // Override the per-topic refresh label when the trigger differs from the source (fresh-install
        // → FRESH_INSTALL); otherwise record the actual source the data came from.
        String refreshLabel = refreshLabelOverride != null ? refreshLabelOverride : result.getSource().name();
        log.info("event=download_refresh.triggering_consumer_refresh source={} label={} topics={}",
                result.getSource(), refreshLabel, topics.size());
        refreshTopics(topics, refreshLabel);
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

    /**
     * Topic set to refresh on a STREAM/CLOUD (pipe-only) re-source: the union of the configured
     * service-topics (known up front, even when storage is still empty mid-async-load) and whatever
     * topics already exist in storage. A configured topic with no connected consumers is naturally
     * skipped by the refresh (no expected consumers), so over-listing is safe.
     */
    private Collection<String> streamRefreshTopics() {
        Set<String> topics = new LinkedHashSet<>();
        java.util.Map<String, List<String>> serviceTopics = legacyClientConfig.getServiceTopics();
        if (serviceTopics != null) {
            for (List<String> t : serviceTopics.values()) {
                if (t != null) {
                    topics.addAll(t);
                }
            }
        }
        topics.addAll(storage.getTopicNames());
        return topics;
    }

    private void stopNetworkServer() {
        try {
            networkServer.stopAccepting();
        } catch (Exception e) {
            // Best-effort: if we cannot take the transport down, the wipe still proceeds. The store
            // quiescing in DownloadRefreshOrchestrator is the second line of defence for the offset race.
            log.error("event=download_refresh.network_stop_failed err={}", e.toString(), e);
        }
    }

    private void resumeNetworkServer() {
        try {
            networkServer.resumeAccepting();
        } catch (NetworkException e) {
            // A failure to rebind leaves consumers unable to reconnect — surface loudly. The refresh
            // result/await below will still proceed, but operators must see this.
            log.error("event=download_refresh.network_resume_failed err={}", e.toString(), e);
        }
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
