package com.messaging.broker.snapshot;

/**
 * The resolved source used to re-source data during a download-refresh (reported back in status).
 */
public enum BootstrapSource {
    /** Parent POS had a ready-made snapshot ZIP — downloaded + restored (fast path). */
    PIPE_AND_PROVIDER_FILE_DOWNLOAD,
    /** Parent POS healthy but no snapshot — record-by-record k-way-merge stream from it. */
    PIPE_AND_PROVIDER_STREAM,
    /** No usable parent (root, or parent mid-refresh) — pulled from the cloud. */
    CLOUD_SYNC
}
