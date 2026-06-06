package com.messaging.storage.metadata;

import java.time.Instant;
import java.util.Arrays;

/**
 * Metadata about a segment
 */
public class SegmentMetadata {
    public static final String HASH_STATE_PENDING = "pending";
    public static final String HASH_STATE_FINAL = "final";
    public static final String HASH_STATE_COMPACTED = "compacted";

    private final String topic;
    private final int partition;
    private final long baseOffset;
    private final long maxOffset;
    private final String logFilePath;
    private final String indexFilePath;
    private final long sizeBytes;
    private final long recordCount;
    private final Instant createdAt;
    private final byte[] segmentHash;
    private final long hashRecordCount;
    private final int compactionEpoch;
    private final String hashState;

    private SegmentMetadata(Builder builder) {
        this.topic = builder.topic;
        this.partition = builder.partition;
        this.baseOffset = builder.baseOffset;
        this.maxOffset = builder.maxOffset;
        this.logFilePath = builder.logFilePath;
        this.indexFilePath = builder.indexFilePath;
        this.sizeBytes = builder.sizeBytes;
        this.recordCount = builder.recordCount;
        this.createdAt = builder.createdAt != null ? builder.createdAt : Instant.now();
        this.segmentHash = builder.segmentHash == null ? null : builder.segmentHash.clone();
        this.hashRecordCount = builder.hashRecordCount;
        this.compactionEpoch = builder.compactionEpoch;
        this.hashState = builder.hashState != null ? builder.hashState : HASH_STATE_PENDING;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public String getLogFilePath() {
        return logFilePath;
    }

    public String getIndexFilePath() {
        return indexFilePath;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public byte[] getSegmentHash() {
        return segmentHash == null ? null : segmentHash.clone();
    }

    public long getHashRecordCount() {
        return hashRecordCount;
    }

    public int getCompactionEpoch() {
        return compactionEpoch;
    }

    public String getHashState() {
        return hashState;
    }

    public boolean hasFinalHash() {
        return segmentHash != null
                && (HASH_STATE_FINAL.equals(hashState) || HASH_STATE_COMPACTED.equals(hashState));
    }

    public static class Builder {
        private String topic;
        private int partition;
        private long baseOffset;
        private long maxOffset;
        private String logFilePath;
        private String indexFilePath;
        private long sizeBytes;
        private long recordCount;
        private Instant createdAt;
        private byte[] segmentHash;
        private long hashRecordCount;
        private int compactionEpoch;
        private String hashState;

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder baseOffset(long baseOffset) {
            this.baseOffset = baseOffset;
            return this;
        }

        public Builder maxOffset(long maxOffset) {
            this.maxOffset = maxOffset;
            return this;
        }

        public Builder logFilePath(String logFilePath) {
            this.logFilePath = logFilePath;
            return this;
        }

        public Builder indexFilePath(String indexFilePath) {
            this.indexFilePath = indexFilePath;
            return this;
        }

        public Builder sizeBytes(long sizeBytes) {
            this.sizeBytes = sizeBytes;
            return this;
        }

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder segmentHash(byte[] segmentHash) {
            this.segmentHash = segmentHash == null ? null : segmentHash.clone();
            return this;
        }

        public Builder hashRecordCount(long hashRecordCount) {
            this.hashRecordCount = hashRecordCount;
            return this;
        }

        public Builder compactionEpoch(int compactionEpoch) {
            this.compactionEpoch = compactionEpoch;
            return this;
        }

        public Builder hashState(String hashState) {
            this.hashState = hashState;
            return this;
        }

        public SegmentMetadata build() {
            return new SegmentMetadata(this);
        }
    }
}
