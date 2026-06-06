package com.messaging.broker.consistency;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Audit result for one (topic, partition, mode) check. Persisted by PipeConsistencyReportStore.
 *
 * <p>Bounded by design: record-level mismatch arrays are capped at
 * {@code pipe.consistency.drill-down.max-mismatch-report}; when capped, {@link #truncated}
 * is set so operators know to query the original ranges via {@code /pipe/consistency/range}.
 */
public class PipeConsistencyReport {
    public enum Mode { HOP, DEEP }
    public enum Status { CONSISTENT, MISMATCH, ERROR, LINEAGE_STALE, DEEP_WALK_ABORTED, UPSTREAM_UNSUPPORTED }

    public final String topic;
    public final int partition;
    public final Mode mode;
    public final Instant checkedAt;
    public final Status status;
    public final String localRootHex;
    public final String upstreamRootHex;
    public final String comparedUpstreamNodeId;
    public final String comparedUpstreamUrl;
    public final List<SegmentSummary> mismatchedSegments;
    public final List<RecordDiff> missingOnBroker;
    public final List<RecordDiff> extraOnBroker;
    public final List<RecordDiff> dataMismatch;
    public final boolean truncated;
    public final String firstDivergentHopNodeId; // DEEP only
    public final String errorMessage;             // ERROR only

    private PipeConsistencyReport(Builder b) {
        this.topic = b.topic;
        this.partition = b.partition;
        this.mode = b.mode;
        this.checkedAt = b.checkedAt != null ? b.checkedAt : Instant.now();
        this.status = b.status;
        this.localRootHex = b.localRootHex;
        this.upstreamRootHex = b.upstreamRootHex;
        this.comparedUpstreamNodeId = b.comparedUpstreamNodeId;
        this.comparedUpstreamUrl = b.comparedUpstreamUrl;
        this.mismatchedSegments = List.copyOf(b.mismatchedSegments);
        this.missingOnBroker = List.copyOf(b.missingOnBroker);
        this.extraOnBroker = List.copyOf(b.extraOnBroker);
        this.dataMismatch = List.copyOf(b.dataMismatch);
        this.truncated = b.truncated;
        this.firstDivergentHopNodeId = b.firstDivergentHopNodeId;
        this.errorMessage = b.errorMessage;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", topic);
        body.put("partition", partition);
        body.put("mode", mode.name().toLowerCase());
        body.put("checkedAt", checkedAt.toString());
        body.put("status", status.name().toLowerCase());
        body.put("localRoot", localRootHex);
        body.put("upstreamRoot", upstreamRootHex);
        body.put("comparedUpstreamNodeId", comparedUpstreamNodeId);
        body.put("comparedUpstreamUrl", comparedUpstreamUrl);
        body.put("mismatchedSegments", mismatchedSegments);
        body.put("missingOnBroker", missingOnBroker);
        body.put("extraOnBroker", extraOnBroker);
        body.put("dataMismatch", dataMismatch);
        body.put("truncated", truncated);
        body.put("firstDivergentHopNodeId", firstDivergentHopNodeId);
        body.put("errorMessage", errorMessage);
        return body;
    }

    public static Builder builder(String topic, int partition, Mode mode) {
        return new Builder(topic, partition, mode);
    }

    public static class Builder {
        private final String topic;
        private final int partition;
        private final Mode mode;
        private Instant checkedAt;
        private Status status = Status.CONSISTENT;
        private String localRootHex;
        private String upstreamRootHex;
        private String comparedUpstreamNodeId;
        private String comparedUpstreamUrl;
        private List<SegmentSummary> mismatchedSegments = new ArrayList<>();
        private List<RecordDiff> missingOnBroker = new ArrayList<>();
        private List<RecordDiff> extraOnBroker = new ArrayList<>();
        private List<RecordDiff> dataMismatch = new ArrayList<>();
        private boolean truncated;
        private String firstDivergentHopNodeId;
        private String errorMessage;

        Builder(String topic, int partition, Mode mode) {
            this.topic = topic;
            this.partition = partition;
            this.mode = mode;
        }

        public Builder status(Status s) { this.status = s; return this; }
        public Builder checkedAt(Instant t) { this.checkedAt = t; return this; }
        public Builder localRoot(String hex) { this.localRootHex = hex; return this; }
        public Builder upstreamRoot(String hex) { this.upstreamRootHex = hex; return this; }
        public Builder comparedUpstream(String nodeId, String url) {
            this.comparedUpstreamNodeId = nodeId;
            this.comparedUpstreamUrl = url;
            return this;
        }
        public Builder addMismatchedSegment(SegmentSummary s) { this.mismatchedSegments.add(s); return this; }
        public Builder addMissingOnBroker(RecordDiff r) { this.missingOnBroker.add(r); return this; }
        public Builder addExtraOnBroker(RecordDiff r) { this.extraOnBroker.add(r); return this; }
        public Builder addDataMismatch(RecordDiff r) { this.dataMismatch.add(r); return this; }
        public Builder truncated(boolean t) { this.truncated = t; return this; }
        public Builder firstDivergentHop(String nodeId) { this.firstDivergentHopNodeId = nodeId; return this; }
        public Builder errorMessage(String m) { this.errorMessage = m; return this; }

        public PipeConsistencyReport build() { return new PipeConsistencyReport(this); }
    }

    public static final class SegmentSummary {
        public final long baseOffset;
        public final long maxOffset;
        public final String localHash;
        public final String upstreamHash;
        public final long localRecordCount;
        public final long upstreamRecordCount;

        public SegmentSummary(long baseOffset, long maxOffset, String localHash, String upstreamHash,
                              long localRecordCount, long upstreamRecordCount) {
            this.baseOffset = baseOffset;
            this.maxOffset = maxOffset;
            this.localHash = localHash;
            this.upstreamHash = upstreamHash;
            this.localRecordCount = localRecordCount;
            this.upstreamRecordCount = upstreamRecordCount;
        }
    }

    public static final class RecordDiff {
        public final long offset;
        public final String msgKey;
        public final String detail; // free-form context (e.g. "type=D vs type=M")

        public RecordDiff(long offset, String msgKey, String detail) {
            this.offset = offset;
            this.msgKey = msgKey;
            this.detail = detail;
        }
    }
}
