package com.messaging.broker.snapshot;

/**
 * The chosen source for a download-refresh bootstrap.
 */
public enum BootstrapSource {
    /** Parent POS has a ready-made snapshot ZIP — download + restore (fast path). */
    SNAPSHOT,
    /** Parent POS is healthy but has no snapshot — incremental k-way-merge pull from it. */
    INCREMENTAL_PARENT,
    /** No usable parent (this node is root, or the parent is mid-refresh) — pull from the cloud. */
    CLOUD
}
