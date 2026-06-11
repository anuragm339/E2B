package com.messaging.broker.compaction;

import java.util.Map;

/**
 * Per-{@code (topic, msgKey)} index of the latest offset + timestamp. Backs both the
 * delivery-time superseded-record filter and the compaction sweep.
 *
 * <p>Implementations are selected at runtime via the {@code compaction-index.backend}
 * property ({@code rocks} by default; see {@code application.yml}). Adding a new backend
 * means implementing this interface and annotating with {@code @Requires(property =
 * "compaction-index.backend", value = "&lt;name&gt;")}.
 *
 * <p>All operations must be thread-safe: the index is read from the delivery hot-path and
 * written from the pipe-ingest and producer paths concurrently. {@link #updateKey} must be
 * a monotonic-advance — an out-of-order call with a lower offset must not regress the index.
 */
public interface CompactionIndex {

    /**
     * Record that {@code newOffset} is the latest known offset for {@code (topic, msgKey)}.
     *
     * <p>No-op when {@code msgKey} is {@code null} (records without a key are not compaction-tracked).
     * No-op when {@code newOffset <= existing} (the index never regresses).
     *
     * <p>When the call <em>does</em> advance the index, implementations must also record that
     * the previously-latest offset is now stale so {@link #shouldFilterDelivery} can return
     * {@code true} until the compaction sweep removes it.
     */
    void updateKey(String topic, String msgKey, long newOffset, long newTimestampMs);

    /**
     * Returns {@code true} when a strictly newer record exists for {@code (topic, msgKey)}
     * — i.e. the record at {@code recordOffset} has been superseded and must not be delivered.
     *
     * <p>Returns {@code false} for records with a {@code null} key.
     */
    boolean isSuperseded(String topic, String msgKey, long recordOffset);

    /**
     * Returns {@code [latestOffset, latestTimestampMs]} for a specific {@code (topic, msgKey)},
     * or {@code null} if the key is not in the index.
     */
    long[] getLatestOffsetAndTimestamp(String topic, String msgKey);

    /**
     * O(1) check: returns {@code true} if the index contains at least one entry for {@code topic}.
     * Used by the delivery hot-path to skip the batch decode entirely when nothing for the
     * topic has ever been compaction-tracked.
     */
    boolean hasIndexedKeysForTopic(String topic);

    /**
     * Returns {@code true} only when {@code deliveryStartOffset} could still contain superseded
     * records that have not yet been physically compacted away. Implementations should track
     * the highest stale offset per topic and answer false above that watermark — that keeps the
     * batch delivery path on its zero-copy fast path whenever no filter would actually drop anything.
     */
    boolean shouldFilterDelivery(String topic, long deliveryStartOffset);

    /**
     * Mark all stale offsets up to {@code compactedThroughOffset} as physically removed for
     * {@code topic}. When the compaction sweep has covered the highest known stale offset, the
     * delivery-time filter is disabled again until a newer duplicate arrives.
     */
    void markCompactedThrough(String topic, long compactedThroughOffset);

    /**
     * Snapshot of all entries for {@code topic} as {@code msgKey -> [latestOffset, latestTimestampMs]}.
     * Used by {@code CompactionRewriter} to decide which records survive a sweep.
     */
    Map<String, long[]> getLatestOffsetsForTopic(String topic);
}
