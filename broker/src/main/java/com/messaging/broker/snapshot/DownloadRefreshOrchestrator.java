package com.messaging.broker.snapshot;

import com.messaging.broker.core.TopologyManager;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Orchestrates a download-refresh bootstrap: choose a source, clear local state, re-source
 * authoritative data, leaving the node ready for the normal RESET→replay→READY to run.
 *
 * <p>Source selection (reactive, no cross-node coordination):
 * <ol>
 *   <li>No parent (root) → CLOUD_SYNC.</li>
 *   <li>Parent {@code /health} DOWN (mid-refresh or unreachable) → escalate to CLOUD_SYNC.</li>
 *   <li>Parent has a snapshot → PIPE_AND_PROVIDER_FILE_DOWNLOAD (download + restore).</li>
 *   <li>Otherwise → PIPE_AND_PROVIDER_STREAM (k-way-merge pull from the parent).</li>
 * </ol>
 *
 * <p>Crash-safety: the file-download path downloads + atomically swaps topic folders BEFORE clearing
 * ack/offset state, so a failed download leaves live data intact. The stream/cloud paths wipe then
 * re-fetch from 0; ingestion is offset-idempotent, so a retry after a partial fetch simply resumes
 * without duplicates.
 */
@Singleton
public class DownloadRefreshOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(DownloadRefreshOrchestrator.class);

    private final String dataDir;
    private final TopologyManager topology;
    private final BootstrapSourceClient client;
    private final LocalStateCleaner cleaner;
    private final SnapshotRestorer restorer;
    // Upper bound on a random delay before escalating to the cloud, so that a parent entering
    // refresh doesn't fan its children into a synchronized cloud stampede. 0 disables.
    private final long escalationJitterMs;

    public DownloadRefreshOrchestrator(
            @Value("${broker.storage.dataDir:./data}") String dataDir,
            TopologyManager topology,
            BootstrapSourceClient client,
            LocalStateCleaner cleaner,
            SnapshotRestorer restorer,
            @Value("${broker.bootstrap.escalation-jitter-ms:30000}") long escalationJitterMs) {
        this.dataDir = dataDir;
        this.topology = topology;
        this.client = client;
        this.cleaner = cleaner;
        this.restorer = restorer;
        this.escalationJitterMs = escalationJitterMs;
    }

    /** Decide where to bootstrap from, given the current parent (null when this node is root). */
    BootstrapSource chooseSource(String parentUrl) {
        if (parentUrl == null) {
            return BootstrapSource.CLOUD_SYNC; // root node — only the cloud is upstream
        }
        if (!client.isParentHealthy(parentUrl)) {
            log.info("event=bootstrap.escalate_to_cloud reason=parent_unhealthy parentUrl={}", parentUrl);
            return BootstrapSource.CLOUD_SYNC; // parent mid-refresh / unreachable
        }
        if (client.snapshotAvailable(parentUrl)) {
            return BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD;
        }
        return BootstrapSource.PIPE_AND_PROVIDER_STREAM;
    }

    /** Run the bootstrap with auto source selection. Never throws — failures returned as a result. */
    public DownloadRefreshResult bootstrap() {
        return bootstrap(null);
    }

    /**
     * Run the bootstrap end to end. When {@code forced} is non-null that source is used; otherwise
     * the source is auto-selected. Never throws — failures are returned as a result.
     */
    public DownloadRefreshResult bootstrap(BootstrapSource forced) {
        String parentUrl = topology.getCurrentParentUrl();
        BootstrapSource source = (forced != null) ? forced : chooseSource(parentUrl);
        log.info("event=bootstrap.started source={} parentUrl={}", source, parentUrl);
        try {
            switch (source) {
                case PIPE_AND_PROVIDER_FILE_DOWNLOAD:
                    // Mid-stream parent failure (parent enters refresh / dies during download) —
                    // escalate to the cloud rather than fail. The parent is unusable now.
                    try {
                        return bootstrapFromSnapshot(parentUrl);
                    } catch (Exception e) {
                        log.warn("event=bootstrap.parent_failed_midstream source=PIPE_AND_PROVIDER_FILE_DOWNLOAD err={} — escalating to cloud",
                                e.toString());
                        return escalateToCloud();
                    }
                case PIPE_AND_PROVIDER_STREAM:
                    try {
                        return bootstrapIncremental(parentUrl);
                    } catch (Exception e) {
                        log.warn("event=bootstrap.parent_failed_midstream source=PIPE_AND_PROVIDER_STREAM err={} — escalating to cloud",
                                e.toString());
                        return escalateToCloud();
                    }
                case CLOUD_SYNC:
                default:
                    // Escalation at selection (parent unhealthy) — jitter to avoid a cloud stampede.
                    // A root node (no parent) has no herd, so no jitter.
                    if (parentUrl != null) {
                        applyEscalationJitter();
                    }
                    return bootstrapFromCloud();
            }
        } catch (Exception e) {
            log.error("event=bootstrap.failed source={} err={}", source, e.toString(), e);
            return DownloadRefreshResult.failure(source, e.toString());
        }
    }

    /** Full cloud bootstrap used when a parent path fails mid-stream. Jittered, self-contained. */
    private DownloadRefreshResult escalateToCloud() {
        try {
            applyEscalationJitter();
            return bootstrapFromCloud();
        } catch (Exception e) {
            log.error("event=bootstrap.cloud_escalation_failed err={}", e.toString(), e);
            return DownloadRefreshResult.failure(BootstrapSource.CLOUD_SYNC, e.toString());
        }
    }

    private DownloadRefreshResult bootstrapFromSnapshot(String parentUrl) {
        Path dir = Paths.get(dataDir);
        // Download + restore FIRST (crash-safe stage→swap); only then reset ack/offset state.
        Path zip = client.downloadSnapshot(parentUrl, dir);
        SnapshotManifest manifest = restorer.restore(zip, dir);
        cleaner.clearState(dataDir);
        log.info("event=bootstrap.completed source=PIPE_AND_PROVIDER_FILE_DOWNLOAD topics={}", manifest.getTopicHeads().size());
        return DownloadRefreshResult.ok(BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD, manifest);
    }

    private DownloadRefreshResult bootstrapIncremental(String parentUrl) {
        cleaner.clearState(dataDir);
        cleaner.clearTopicData(dataDir);
        client.bulkFetchFromParent(parentUrl, dataDir);
        log.info("event=bootstrap.completed source=PIPE_AND_PROVIDER_STREAM parentUrl={}", parentUrl);
        return DownloadRefreshResult.ok(BootstrapSource.PIPE_AND_PROVIDER_STREAM, null);
    }

    private DownloadRefreshResult bootstrapFromCloud() {
        cleaner.clearState(dataDir);
        cleaner.clearTopicData(dataDir);
        client.bulkFetchFromCloud(dataDir);
        log.info("event=bootstrap.completed source=CLOUD_SYNC");
        return DownloadRefreshResult.ok(BootstrapSource.CLOUD_SYNC, null);
    }

    /** Sleep a random 0..escalationJitterMs to de-synchronize a fan-out of children onto the cloud. */
    private void applyEscalationJitter() {
        if (escalationJitterMs <= 0) {
            return;
        }
        long delay = java.util.concurrent.ThreadLocalRandom.current().nextLong(escalationJitterMs + 1);
        log.info("event=bootstrap.escalation_jitter delayMs={}", delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
