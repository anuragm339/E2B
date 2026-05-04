package com.messaging.common.model;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * A delivery unit produced by storage — carries the segment metadata and byte payload
 * needed to stream records to a consumer.
 *
 * {@code topic} is included because it is a storage-level concept (data is partitioned by topic).
 * {@code group} is NOT included — it is a delivery-routing concept that belongs to the transport
 * call site, not to the storage-produced object.
 *
 * Ownership rule:
 *   - Before sendBatch() is called: caller owns the batch and must call close() on any early exit.
 *   - After sendBatch() is called: the transport owns the batch and must call close() in all
 *     outcomes (success, failure, cancellation, backpressure rejection).
 *
 * Implementations:
 *   - Segment.BatchFileRegion  — file-backed, uses FileChannel.transferTo() (OS sendfile)
 *   - ByteArrayDeliveryBatch   — heap-backed, for in-memory or DB-backed storage engines
 */
public interface DeliveryBatch extends AutoCloseable {

    // ── Storage metadata ───────────────────────────────────────────────────────

    String getTopic();

    int getRecordCount();

    long getTotalBytes();

    // ── Broker ACK tracking ────────────────────────────────────────────────────

    /** First message offset in this batch. Used by BatchAckService to re-read batch from storage. */
    long getFirstOffset();

    /** Last message offset in this batch. Broker uses this to advance consumer offset after ACK. */
    long getLastOffset();

    // ── Convenience ───────────────────────────────────────────────────────────

    default boolean isEmpty() {
        return getRecordCount() == 0;
    }

    // ── Byte transfer ─────────────────────────────────────────────────────────

    /**
     * Transfer bytes starting at {@code position} (relative offset within this batch) to target.
     * {@code position} is required because Netty may call transferTo() multiple times for partial sends.
     * File-backed implementations use FileChannel.transferTo() for OS sendfile(); others buffer-copy.
     *
     * @return number of bytes transferred, or -1 if no bytes remain at the given position
     */
    long transferTo(WritableByteChannel target, long position) throws IOException;

    @Override
    void close() throws IOException;
}
