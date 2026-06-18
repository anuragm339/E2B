package com.messaging.broker.snapshot;

import java.nio.file.Path;

/**
 * Seam over the network/IO a download-refresh bootstrap needs, so {@link DownloadRefreshOrchestrator}
 * can be unit-tested without real HTTP. The HTTP implementation is {@link HttpBootstrapSourceClient}.
 */
public interface BootstrapSourceClient {

    /** Probe a parent's {@code /health}. A parent mid-refresh reports DOWN, so this returns false. */
    boolean isParentHealthy(String parentUrl);

    /** True if the parent has a snapshot available to download ({@code /pipe/snapshot/info}). */
    boolean snapshotAvailable(String parentUrl);

    /**
     * Download the parent's snapshot ZIP to a local file (under {@code dataDir/snapshots/}).
     *
     * @return the path of the downloaded ZIP.
     */
    Path downloadSnapshot(String parentUrl, Path dataDir);

    /**
     * Incrementally pull ALL topics from a parent POS via the k-way-merge {@code /pipe/poll}
     * (per-topic cursor header), ingesting into local storage until caught up. No duplicates.
     */
    void bulkFetchFromParent(String parentUrl, String dataDir);

    /** Pull all data from the cloud's global {@code /pipe/poll} stream from offset 0, ingesting locally. */
    void bulkFetchFromCloud(String dataDir);
}
