package com.messaging.broker.snapshot;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metadata embedded as {@code manifest.json} at the root of a data snapshot ZIP.
 *
 * <p>Tells a bootstrapping child what the snapshot contains and the per-topic head offsets at
 * snapshot time — the watermark each topic targets ("bulk load done for topic X once its offset
 * reaches head") and the denominator for the POS→POS progress %.
 */
public class SnapshotManifest {

    /** Bump when the snapshot layout/exclusions change in a non-backward-compatible way. */
    public static final int SCHEMA_VERSION = 1;

    @JsonProperty("schemaVersion")
    private int schemaVersion = SCHEMA_VERSION;

    @JsonProperty("createdAtMs")
    private long createdAtMs;

    /** topic -> head offset (last stored offset; -1 for empty) captured when the snapshot was built. */
    @JsonProperty("topicHeads")
    private Map<String, Long> topicHeads = new LinkedHashMap<>();

    public SnapshotManifest() {
    }

    public SnapshotManifest(long createdAtMs, Map<String, Long> topicHeads) {
        this.schemaVersion = SCHEMA_VERSION;
        this.createdAtMs = createdAtMs;
        if (topicHeads != null) {
            this.topicHeads = new LinkedHashMap<>(topicHeads);
        }
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public long getCreatedAtMs() {
        return createdAtMs;
    }

    public void setCreatedAtMs(long createdAtMs) {
        this.createdAtMs = createdAtMs;
    }

    public Map<String, Long> getTopicHeads() {
        return topicHeads;
    }

    public void setTopicHeads(Map<String, Long> topicHeads) {
        this.topicHeads = topicHeads;
    }
}
