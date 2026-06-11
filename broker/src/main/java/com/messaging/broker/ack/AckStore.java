package com.messaging.broker.ack;

/**
 * Persistence boundary for per-(topic, group, offset) ACK records.
 *
 * <p>Offset is the unique event identity. Implementations must key by offset (never solely
 * by {@code msgKey}) so duplicate keys at different offsets are tracked separately and records
 * with a {@code null} key are still represented.
 *
 * <p>Implementations are selected at runtime via the {@code ack-store.backend} property
 * ({@code rocks} by default; see {@code application.yml}). Adding a new backend means
 * implementing this interface and annotating with {@code @Requires(property = "ack-store.backend",
 * value = "&lt;name&gt;")}.
 *
 * <p>All operations must be thread-safe — the broker calls them from delivery, ACK,
 * reconciliation, and refresh paths concurrently. On persistence failure, implementations
 * must throw {@link AckStoreException} rather than swallowing the error.
 */
public interface AckStore {

    /**
     * Write or overwrite the ACK record for a single {@code (topic, group, offset)} triple.
     *
     * @throws AckStoreException if the backend write fails
     */
    void put(String topic, String group, long offset, AckRecord record);

    /**
     * Return the ACK record for a {@code (topic, group, offset)} triple, or {@code null}
     * if no record exists.
     *
     * @throws AckStoreException if the backend read fails
     */
    AckRecord get(String topic, String group, long offset);

    /**
     * Atomically write multiple ACK records. Arrays are parallel — element {@code i} of each
     * forms one entry. Implementations must persist all-or-nothing (no partial application).
     *
     * @throws IllegalArgumentException if the three arrays have differing lengths
     * @throws AckStoreException        if the backend batch write fails
     */
    void putBatch(String[] topics, String[] groups, AckRecord[] records);

    /**
     * Delete every ACK record for a {@code (topic, group)} pair. Called by the data refresh
     * workflow when a consumer enters {@code RESET_SENT} so stale ACK state does not persist
     * across a consumer wipe.
     *
     * @throws AckStoreException if the backend delete fails
     */
    void clearByTopicAndGroup(String topic, String group);

    /**
     * Return all acked offsets for a {@code (topic, group)} pair in
     * {@code [fromOffset, toOffsetExclusive)}.
     *
     * <p>Exists so bulk verification paths (startup seeding, reconciliation) can replace
     * one point lookup per record with a single range scan — on the RocksDB backend a
     * prefix iteration instead of N {@code get()} calls.
     *
     * @throws AckStoreException if the backend read fails
     */
    java.util.Set<Long> getAckedOffsetsInRange(String topic, String group, long fromOffset, long toOffsetExclusive);
}
