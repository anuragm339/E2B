package com.messaging.broker.consistency;

import java.time.Instant;

/**
 * One row in pipe_lineage: a contiguous offset range that this broker received from
 * a specific upstream parent. offsetEndExclusive == null means the row is still open
 * (i.e. that parent is the current source).
 */
public final class PipeLineageEntry {
    private final long id;
    private final long offsetStart;
    private final Long offsetEndExclusive;
    private final String parentUrl;
    private final Instant recordedAt;

    public PipeLineageEntry(long id, long offsetStart, Long offsetEndExclusive, String parentUrl, Instant recordedAt) {
        this.id = id;
        this.offsetStart = offsetStart;
        this.offsetEndExclusive = offsetEndExclusive;
        this.parentUrl = parentUrl;
        this.recordedAt = recordedAt;
    }

    public long getId() { return id; }
    public long getOffsetStart() { return offsetStart; }
    public Long getOffsetEndExclusive() { return offsetEndExclusive; }
    public String getParentUrl() { return parentUrl; }
    public Instant getRecordedAt() { return recordedAt; }
    public boolean isOpen() { return offsetEndExclusive == null; }

    /**
     * Returns the inclusive end offset of this lineage row, or Long.MAX_VALUE if the row is still open.
     */
    public long maxOffsetInclusive() {
        return offsetEndExclusive == null ? Long.MAX_VALUE : offsetEndExclusive - 1;
    }
}
