package com.messaging.storage.segment;

/**
 * Optional capability interface for storage engines that expose their underlying
 * {@link SegmentManager} instances. Used by the compaction infrastructure to
 * perform segment-level operations (select dirty segments, replace after rewrite).
 *
 * <p>Implementations: {@code FileChannelStorageEngine}, {@code MMapStorageEngine}.
 */
public interface SegmentAccess {

    /**
     * Return the {@link SegmentManager} for the given topic-partition, or {@code null}
     * if no data has been written to that topic-partition yet.
     */
    SegmentManager getSegmentManager(String topic, int partition);
}
