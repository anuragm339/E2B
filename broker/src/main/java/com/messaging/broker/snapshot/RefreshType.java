package com.messaging.broker.snapshot;

/**
 * The kind of refresh requested via the admin API. {@code LOCAL} replays the node's own segments;
 * the rest wipe and re-source from upstream — {@code DOWNLOAD} auto-selects the source, while
 * {@code SNAPSHOT}/{@code INCREMENTAL}/{@code CLOUD} force a specific one.
 */
public enum RefreshType {
    /** Replay local segments to consumers only — no download (the original refresh). */
    LOCAL,
    /** Wipe + re-source, auto-selecting the source (snapshot → incremental → cloud). */
    DOWNLOAD,
    /** Force download from a parent snapshot. */
    SNAPSHOT,
    /** Force incremental k-way-merge pull from the parent. */
    INCREMENTAL,
    /** Force pull from the cloud. */
    CLOUD,
    /** Factory reset: stop everything, wipe the storage dir's contents, exit (operator restarts). */
    BARE_METAL;

    public boolean isLocal() {
        return this == LOCAL;
    }

    public boolean isBareMetal() {
        return this == BARE_METAL;
    }

    /** The forced bootstrap source, or {@code null} to auto-select / N-A (DOWNLOAD/LOCAL/BARE_METAL). */
    public BootstrapSource forcedSource() {
        return switch (this) {
            case SNAPSHOT -> BootstrapSource.SNAPSHOT;
            case INCREMENTAL -> BootstrapSource.INCREMENTAL_PARENT;
            case CLOUD -> BootstrapSource.CLOUD;
            case DOWNLOAD, LOCAL, BARE_METAL -> null;
        };
    }

    /** Parse a request value, defaulting to DOWNLOAD (auto) when blank/unknown. */
    public static RefreshType from(String raw) {
        if (raw == null || raw.isBlank()) {
            return DOWNLOAD;
        }
        try {
            return valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return DOWNLOAD;
        }
    }
}
