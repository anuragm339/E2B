package com.messaging.broker.snapshot;

import com.messaging.broker.core.TopologyManager;
import com.messaging.common.exception.DataRefreshException;
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
 *   <li>No parent (root) → CLOUD.</li>
 *   <li>Parent {@code /health} DOWN (mid-refresh or unreachable) → escalate to CLOUD.</li>
 *   <li>Parent has a snapshot → SNAPSHOT (download + restore).</li>
 *   <li>Otherwise → INCREMENTAL_PARENT (k-way-merge pull from the parent).</li>
 * </ol>
 *
 * <p>Crash-safety: the SNAPSHOT path downloads + atomically swaps topic folders BEFORE clearing
 * ack/offset state, so a failed download leaves live data intact. The INCREMENTAL/CLOUD paths wipe
 * then re-fetch from 0; ingestion is offset-idempotent, so a retry after a partial fetch simply
 * resumes without duplicates.
 */
@Singleton
public class DownloadRefreshOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(DownloadRefreshOrchestrator.class);

    private final String dataDir;
    private final TopologyManager topology;
    private final BootstrapSourceClient client;
    private final LocalStateCleaner cleaner;
    private final SnapshotRestorer restorer;

    public DownloadRefreshOrchestrator(
            @Value("${broker.storage.dataDir:./data}") String dataDir,
            TopologyManager topology,
            BootstrapSourceClient client,
            LocalStateCleaner cleaner,
            SnapshotRestorer restorer) {
        this.dataDir = dataDir;
        this.topology = topology;
        this.client = client;
        this.cleaner = cleaner;
        this.restorer = restorer;
    }

    /** Decide where to bootstrap from, given the current parent (null when this node is root). */
    BootstrapSource chooseSource(String parentUrl) {
        if (parentUrl == null) {
            return BootstrapSource.CLOUD; // root node — only the cloud is upstream
        }
        if (!client.isParentHealthy(parentUrl)) {
            log.info("event=bootstrap.escalate_to_cloud reason=parent_unhealthy parentUrl={}", parentUrl);
            return BootstrapSource.CLOUD; // parent mid-refresh / unreachable
        }
        if (client.snapshotAvailable(parentUrl)) {
            return BootstrapSource.SNAPSHOT;
        }
        return BootstrapSource.INCREMENTAL_PARENT;
    }

    /** Run the bootstrap end to end. Never throws — failures are returned as a result. */
    public DownloadRefreshResult bootstrap() {
        String parentUrl = topology.getCurrentParentUrl();
        BootstrapSource source = chooseSource(parentUrl);
        log.info("event=bootstrap.started source={} parentUrl={}", source, parentUrl);
        try {
            return switch (source) {
                case SNAPSHOT -> bootstrapFromSnapshot(parentUrl);
                case INCREMENTAL_PARENT -> bootstrapIncremental(parentUrl);
                case CLOUD -> bootstrapFromCloud();
            };
        } catch (DataRefreshException e) {
            log.error("event=bootstrap.failed source={} code={} err={}", source, e.getErrorCode(), e.getMessage());
            return DownloadRefreshResult.failure(source, e.getMessage());
        } catch (Exception e) {
            log.error("event=bootstrap.failed source={} err={}", source, e.toString(), e);
            return DownloadRefreshResult.failure(source, e.toString());
        }
    }

    private DownloadRefreshResult bootstrapFromSnapshot(String parentUrl) {
        Path dir = Paths.get(dataDir);
        // Download + restore FIRST (crash-safe stage→swap); only then reset ack/offset state.
        Path zip = client.downloadSnapshot(parentUrl, dir);
        SnapshotManifest manifest = restorer.restore(zip, dir);
        cleaner.clearState(dataDir);
        log.info("event=bootstrap.completed source=SNAPSHOT topics={}", manifest.getTopicHeads().size());
        return DownloadRefreshResult.ok(BootstrapSource.SNAPSHOT, manifest);
    }

    private DownloadRefreshResult bootstrapIncremental(String parentUrl) {
        cleaner.clearState(dataDir);
        cleaner.clearTopicData(dataDir);
        client.bulkFetchFromParent(parentUrl, dataDir);
        log.info("event=bootstrap.completed source=INCREMENTAL_PARENT parentUrl={}", parentUrl);
        return DownloadRefreshResult.ok(BootstrapSource.INCREMENTAL_PARENT, null);
    }

    private DownloadRefreshResult bootstrapFromCloud() {
        cleaner.clearState(dataDir);
        cleaner.clearTopicData(dataDir);
        client.bulkFetchFromCloud(dataDir);
        log.info("event=bootstrap.completed source=CLOUD");
        return DownloadRefreshResult.ok(BootstrapSource.CLOUD, null);
    }
}
