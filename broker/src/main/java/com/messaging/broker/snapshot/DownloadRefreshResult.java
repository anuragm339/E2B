package com.messaging.broker.snapshot;

/**
 * Outcome of a download-refresh bootstrap: which source was used and whether it succeeded.
 */
public class DownloadRefreshResult {

    private final boolean success;
    private final BootstrapSource source;
    private final SnapshotManifest manifest; // non-null only for the SNAPSHOT path
    private final String error;              // non-null only on failure

    private DownloadRefreshResult(boolean success, BootstrapSource source,
                                  SnapshotManifest manifest, String error) {
        this.success = success;
        this.source = source;
        this.manifest = manifest;
        this.error = error;
    }

    public static DownloadRefreshResult ok(BootstrapSource source, SnapshotManifest manifest) {
        return new DownloadRefreshResult(true, source, manifest, null);
    }

    public static DownloadRefreshResult failure(BootstrapSource source, String error) {
        return new DownloadRefreshResult(false, source, null, error);
    }

    public boolean isSuccess() {
        return success;
    }

    public BootstrapSource getSource() {
        return source;
    }

    public SnapshotManifest getManifest() {
        return manifest;
    }

    public String getError() {
        return error;
    }
}
