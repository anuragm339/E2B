package com.messaging.common.api;

import com.messaging.common.exception.MessagingException;
import com.messaging.common.model.DeliveryBatch;

/**
 * Optional capability interface for storage engines that can produce a DeliveryBatch for
 * zero-copy or chunked delivery to consumers.
 *
 * Storage engines that implement this interface (alongside StorageEngine) allow the network
 * transport to choose the most efficient transfer strategy (sendfile, buffered copy, etc.)
 * without any broker-layer coupling to the storage or network implementation.
 *
 * Caller owns the returned DeliveryBatch until sendBatch() is called; the transport owns it after.
 */
public interface BatchReadableStorage {

    /**
     * Return a DeliveryBatch for delivery to a remote consumer.
     *
     * {@code group} is intentionally absent — consumer group is a delivery-routing concept,
     * not a storage concept. The caller passes group separately to {@code NetworkServer.sendBatch()}.
     *
     * @param topic      topic name
     * @param partition  partition index
     * @param fromOffset first offset to include (inclusive)
     * @param maxBytes   maximum payload bytes to include
     * @return a DeliveryBatch (may be empty, never null); caller must close() on early exit
     */
    DeliveryBatch getBatch(String topic, int partition, long fromOffset, long maxBytes)
            throws MessagingException;
}
