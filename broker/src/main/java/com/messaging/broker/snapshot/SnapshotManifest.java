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

    /**
     * N* — the global pipe cursor ({@code HttpPipeConnector.getCurrentOffset()}) captured atomically
     * with the topic data when the snapshot was built. The restoring child seeds its pipe-offset to
     * this so the pipe resumes from N* (fetching only the tail the parent appended after the snapshot)
     * instead of re-streaming the whole history from 0. {@code -1} = absent (legacy snapshot) →
     * restore falls back to streaming from 0.
     */
    @JsonProperty("pipeOffset")
    private long pipeOffset = -1;

    public SnapshotManifest() {
    }

    public SnapshotManifest(long createdAtMs, Map<String, Long> topicHeads) {
        this(createdAtMs, topicHeads, -1);
    }

    public SnapshotManifest(long createdAtMs, Map<String, Long> topicHeads, long pipeOffset) {
        this.schemaVersion = SCHEMA_VERSION;
        this.createdAtMs = createdAtMs;
        if (topicHeads != null) {
            this.topicHeads = new LinkedHashMap<>(topicHeads);
        }
        this.pipeOffset = pipeOffset;
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

    /** N* — the global pipe cursor at snapshot-build time; -1 when absent (legacy snapshot). */
    public long getPipeOffset() {
        return pipeOffset;
    }

    public void setPipeOffset(long pipeOffset) {
        this.pipeOffset = pipeOffset;
    }
}
