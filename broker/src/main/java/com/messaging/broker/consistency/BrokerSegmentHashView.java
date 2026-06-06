package com.messaging.broker.consistency;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.MessagingException;
import com.messaging.common.hash.RecordHasher;
import com.messaging.common.model.MessageRecord;
import com.messaging.storage.metadata.SegmentMetadata;
import com.messaging.storage.metadata.SegmentMetadataStore;
import com.messaging.storage.segment.Segment;
import com.messaging.storage.segment.SegmentAccess;
import com.messaging.storage.segment.SegmentHasher;
import com.messaging.storage.segment.SegmentManager;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes PipeConsistency hashes for the broker's own storage. Used by:
 * <ul>
 *   <li>{@link BrokerPipeConsistencyController} to serve {@code /pipe/consistency/hash} to child brokers</li>
 *   <li>The PipeConsistencyChecker to build its local root before comparing against upstream</li>
 * </ul>
 *
 * <p>Cost model:
 * <ul>
 *   <li>Range exactly equals one sealed segment → O(1) lookup from metadata.</li>
 *   <li>Range covers a contiguous set of sealed segments → O(N segments) Merkle combine of stored hashes.</li>
 *   <li>Range crosses or hits the active segment → O(active records) re-read for the partial portion only.</li>
 * </ul>
 */
@Singleton
public class BrokerSegmentHashView {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerSegmentHashView.class);

    private final StorageEngine storage;

    public BrokerSegmentHashView(StorageEngine storage) {
        this.storage = storage;
    }

    /**
     * Compute the rolling hash over the broker's records in {@code [fromOffset, toOffset]}.
     * Partition is assumed to be 0 (current single-partition shape).
     */
    public Computed computeHash(String topic, long fromOffset, long toOffset) {
        if (toOffset < fromOffset) {
            return new Computed(RecordHasher.EMPTY_HASH.clone(), 0L, 0);
        }
        SegmentManager mgr = managerFor(topic, 0);
        if (mgr == null) {
            return new Computed(RecordHasher.EMPTY_HASH.clone(), 0L, 0);
        }

        // Compute the chunk hash by combining per-record CRCs in offset order — same
        // operation cloud uses in EmissionView.computeHash. Earlier versions folded
        // stored segment hashes with mergeNodes (Merkle combine) which produces a
        // different result from combine() over the same records — including the
        // edge case where both sides hold zero records but the broker's rolling
        // value transitions away from EMPTY_HASH through mergeNodes(EMPTY, EMPTY).
        //
        // The maxCompactionEpoch is still derived from segment metadata so callers
        // can pick the right `projection` flag when talking upstream.
        int maxEpoch = highestEpochInRange(mgr, topic, fromOffset, toOffset);
        Computed records = computeViaRead(mgr, topic, 0, fromOffset, toOffset);
        return new Computed(records.hash, records.recordCount, maxEpoch);
    }

    private int highestEpochInRange(SegmentManager mgr, String topic, long fromOffset, long toOffset) {
        int max = 0;
        try {
            for (SegmentMetadata sm : mgr.getMetadataStore().getSegments(topic, 0)) {
                if (sm.getMaxOffset() < fromOffset || sm.getBaseOffset() > toOffset) continue;
                if (sm.getCompactionEpoch() > max) max = sm.getCompactionEpoch();
            }
        } catch (MessagingException e) {
            LOG.warn("Failed to read epochs for topic={}: {}", topic, e.getMessage());
        }
        return max;
    }

    /**
     * Slow path: read records in [lo, hi] one by one and fold their CRCs in order.
     *
     * <p>Note: {@code SegmentManager.read} caps each call at 1 MB of payload (regardless
     * of the requested {@code maxRecords}). So we MUST NOT use {@code batch.size() &lt; max}
     * as an end-of-data signal — at 20 KB/record we'd terminate after the first batch.
     * Instead, advance by the highest offset seen, and break only on EMPTY batch or when
     * we've crossed {@code hi}. A no-progress backstop guards against the pathological
     * case where storage returns the same records repeatedly.
     */
    private Computed computeViaRead(SegmentManager mgr, String topic, int partition, long lo, long hi) {
        byte[] rolling = RecordHasher.EMPTY_HASH.clone();
        long count = 0;
        long nextOffset = lo;
        while (nextOffset <= hi) {
            int max = (int) Math.min(1000L, hi - nextOffset + 1);
            List<MessageRecord> batch;
            try {
                batch = storage.read(topic, partition, nextOffset, max);
            } catch (Exception e) {
                LOG.warn("read failed at offset={}: {}", nextOffset, e.getMessage());
                break;
            }
            if (batch == null || batch.isEmpty()) break;
            long highestSeen = nextOffset - 1;
            for (MessageRecord r : batch) {
                if (r.getOffset() > hi) {
                    // Past the chunk window — record progress so we can break.
                    if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
                    continue;
                }
                if (r.getOffset() < lo) continue;
                int crc = RecordHasher.recordCrc(
                        r.getOffset(),
                        r.getMsgKey(),
                        (char) r.getEventType().getCode(),
                        r.getData());
                rolling = RecordHasher.combine(rolling, crc);
                count++;
                if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
            }
            long newOffset = highestSeen + 1;
            if (newOffset <= nextOffset) break; // no-progress backstop
            nextOffset = newOffset;
        }
        return new Computed(rolling, count, 0);
    }

    /**
     * Treat a stored 16-byte segment hash as if it were a single opaque "record" for the
     * purposes of combining with the running rolling hash. We hash the bytes through
     * {@code mergeNodes}-style combine to fold deterministically without unpacking back
     * into per-record CRCs.
     */
    private byte[] foldHashBytes(byte[] prev, byte[] toFold) {
        if (toFold == null || toFold.length != RecordHasher.HASH_LEN) {
            return prev;
        }
        return RecordHasher.mergeNodes(prev, toFold);
    }

    public long maxOffset(String topic) {
        return storage.getCurrentOffset(topic, 0);
    }

    // ── Global (across-all-topics) hashing ─────────────────────────────────────
    // The pipe is a single global cloud-offset stream; broker splits it into
    // per-topic segments locally. Cloud's /pipe/consistency hashes are over the
    // entire stream regardless of topic. Per-topic broker comparisons are
    // fundamentally apples-to-oranges. These methods compute the broker's view
    // over the global stream so it can be compared against cloud's view directly.

    /**
     * Highest offset the broker has stored across all topics. -1 if no topics
     * have any records yet.
     */
    public long globalMaxOffset() {
        long max = -1;
        for (String topic : storage.getTopicNames()) {
            long m = storage.getCurrentOffset(topic, 0);
            if (m > max) max = m;
        }
        return max;
    }

    /**
     * Smallest offset the broker has stored across any topic. -1 if no topics
     * have any records yet.
     */
    public long globalEarliestOffset() {
        long min = -1;
        for (String topic : storage.getTopicNames()) {
            long e = storage.getEarliestOffset(topic, 0);
            if (e < 0) continue;
            if (min < 0 || e < min) min = e;
        }
        return min;
    }

    /**
     * Compute the rolling hash over EVERY record the broker has with offset in
     * {@code [fromOffset, toOffset]}, regardless of topic — merged into a single
     * stream sorted by offset. This matches cloud's
     * {@code EmissionView.computeHash} semantics (which doesn't filter by topic
     * either since the event table is one global stream).
     */
    public Computed computeGlobalHash(long fromOffset, long toOffset) {
        if (toOffset < fromOffset) {
            return new Computed(RecordHasher.EMPTY_HASH.clone(), 0L, 0);
        }

        // Walk every topic's segments, collect all records whose offset falls in
        // the chunk window, merge by offset, then fold CRCs in order. For audit
        // chunks bounded by pipe.consistency.chunk-size this stays in memory.
        java.util.TreeMap<Long, MessageRecord> byOffset = new java.util.TreeMap<>();
        int maxEpoch = 0;

        for (String topic : storage.getTopicNames()) {
            SegmentManager mgr = managerFor(topic, 0);
            if (mgr == null) continue;
            try {
                for (SegmentMetadata sm : mgr.getMetadataStore().getSegments(topic, 0)) {
                    if (sm.getMaxOffset() < fromOffset || sm.getBaseOffset() > toOffset) continue;
                    if (sm.getCompactionEpoch() > maxEpoch) maxEpoch = sm.getCompactionEpoch();
                }
            } catch (MessagingException ignored) {
                // Per-topic metadata read failures shouldn't kill the global compute.
            }

            // Page through this topic's records in the chunk range.
            // See computeViaRead note: storage caps batches at 1 MB, so batch.size() < max
            // CANNOT be used as an end-of-data signal. Advance by highest offset seen.
            long nextOffset = fromOffset;
            while (nextOffset <= toOffset) {
                int max = (int) Math.min(1000L, toOffset - nextOffset + 1);
                List<MessageRecord> batch;
                try {
                    batch = storage.read(topic, 0, nextOffset, max);
                } catch (Exception e) {
                    LOG.debug("global read failed topic={} at offset={}: {}", topic, nextOffset, e.getMessage());
                    break;
                }
                if (batch == null || batch.isEmpty()) break;
                long highestSeen = nextOffset - 1;
                for (MessageRecord r : batch) {
                    if (r.getOffset() > toOffset) {
                        if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
                        continue;
                    }
                    if (r.getOffset() < fromOffset) continue;
                    byOffset.put(r.getOffset(), r);
                    if (r.getOffset() > highestSeen) highestSeen = r.getOffset();
                }
                long newOffset = highestSeen + 1;
                if (newOffset <= nextOffset) break;
                nextOffset = newOffset;
            }
        }

        byte[] rolling = RecordHasher.EMPTY_HASH.clone();
        for (MessageRecord r : byOffset.values()) {
            int crc = RecordHasher.recordCrc(
                    r.getOffset(),
                    r.getMsgKey(),
                    (char) r.getEventType().getCode(),
                    r.getData());
            rolling = RecordHasher.combine(rolling, crc);
        }
        return new Computed(rolling, byOffset.size(), maxEpoch);
    }

    private SegmentManager managerFor(String topic, int partition) {
        // Use the SegmentAccess interface — both MMapStorageEngine and
        // FileChannelStorageEngine implement it. Previous code only matched the mmap
        // type, which silently returned null on file-channel deployments and left
        // audits reporting recordCount=0 / hash=EMPTY for every range.
        if (storage instanceof SegmentAccess sa) {
            return sa.getSegmentManager(topic, partition);
        }
        return null;
    }

    public static final class Computed {
        public final byte[] hash;
        public final long recordCount;
        public final int maxCompactionEpoch;

        public Computed(byte[] hash, long recordCount, int maxCompactionEpoch) {
            this.hash = hash;
            this.recordCount = recordCount;
            this.maxCompactionEpoch = maxCompactionEpoch;
        }
    }

    /** Convenience: gather all sealed segment summaries for /admin/consistency/segments. */
    public List<SegmentSummary> sealedSummaries(String topic) {
        SegmentManager mgr = managerFor(topic, 0);
        if (mgr == null) return List.of();
        try {
            List<SegmentSummary> out = new ArrayList<>();
            for (SegmentMetadata sm : mgr.getMetadataStore().getSegments(topic, 0)) {
                out.add(new SegmentSummary(
                        sm.getBaseOffset(),
                        sm.getMaxOffset(),
                        sm.getSegmentHash(),
                        sm.getHashRecordCount(),
                        sm.getCompactionEpoch(),
                        sm.getHashState()));
            }
            return out;
        } catch (MessagingException e) {
            LOG.error("Failed to load summaries for {}", topic, e);
            return List.of();
        }
    }

    public static final class SegmentSummary {
        public final long baseOffset;
        public final long maxOffset;
        public final byte[] hash;
        public final long recordCount;
        public final int compactionEpoch;
        public final String hashState;

        public SegmentSummary(long baseOffset, long maxOffset, byte[] hash, long recordCount,
                              int compactionEpoch, String hashState) {
            this.baseOffset = baseOffset;
            this.maxOffset = maxOffset;
            this.hash = hash;
            this.recordCount = recordCount;
            this.compactionEpoch = compactionEpoch;
            this.hashState = hashState;
        }
    }
}
