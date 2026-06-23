package com.messaging.broker.snapshot;

/**
 * The kind of refresh requested via the admin API.
 *
 * <ul>
 *   <li>{@code LOCAL} — replay the node's own segments to consumers (no download).</li>
 *   <li>{@code PIPE_AND_PROVIDER_REFRESH} — re-source from the parent provider over the pipe;
 *       the system auto-resolves to a file download or a record stream by availability.</li>
 *   <li>{@code PIPE_AND_PROVIDER_FILE_DOWNLOAD} / {@code PIPE_AND_PROVIDER_STREAM} — advanced: force
 *       the file-download or record-stream source respectively.</li>
 *   <li>{@code CLOUD_SYNC} — re-source from the cloud.</li>
 *   <li>{@code BARE_METAL} — hard reset: RESET consumers best-effort, then stop the node, wipe the
 *       data dir, and {@code System.exit}; an external supervisor restarts the empty node, which
 *       re-sources via the normal startup path. Distinct from the in-process refresh lifecycle.</li>
 * </ul>
 */
public enum RefreshType {
    LOCAL,
    PIPE_AND_PROVIDER_REFRESH,
    PIPE_AND_PROVIDER_FILE_DOWNLOAD,
    PIPE_AND_PROVIDER_STREAM,
    CLOUD_SYNC,
    BARE_METAL;

    public boolean isLocal() {
        return this == LOCAL;
    }

    public boolean isBareMetal() {
        return this == BARE_METAL;
    }

    /**
     * The forced bootstrap source, or {@code null} to auto-select / N-A
     * ({@code PIPE_AND_PROVIDER_REFRESH} / {@code LOCAL} / {@code BARE_METAL}).
     * BARE_METAL never reaches the orchestrator (it exits the process); the empty node re-sources
     * on the next boot by auto-selecting the best available upstream source.
     */
    public BootstrapSource forcedSource() {
        return switch (this) {
            case PIPE_AND_PROVIDER_FILE_DOWNLOAD -> BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD;
            case PIPE_AND_PROVIDER_STREAM -> BootstrapSource.PIPE_AND_PROVIDER_STREAM;
            case CLOUD_SYNC -> BootstrapSource.CLOUD_SYNC;
            case PIPE_AND_PROVIDER_REFRESH, LOCAL, BARE_METAL -> null;
        };
    }

    /** Parse a request value, defaulting to PIPE_AND_PROVIDER_REFRESH (auto) when blank/unknown. */
    public static RefreshType from(String raw) {
        return tryParse(raw).orElse(PIPE_AND_PROVIDER_REFRESH);
    }

    /**
     * Strict parse: blank → the default ({@code PIPE_AND_PROVIDER_REFRESH}); a recognized value →
     * that value; an UNKNOWN non-blank value → empty (so the caller can reject it with a 400 rather
     * than silently running a destructive refresh on a typo).
     */
    public static java.util.Optional<RefreshType> tryParse(String raw) {
        if (raw == null || raw.isBlank()) {
            return java.util.Optional.of(PIPE_AND_PROVIDER_REFRESH);
        }
        try {
            return java.util.Optional.of(valueOf(raw.trim().toUpperCase()));
        } catch (IllegalArgumentException e) {
            return java.util.Optional.empty();
        }
    }
}
