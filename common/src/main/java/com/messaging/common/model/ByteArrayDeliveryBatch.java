package com.messaging.common.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Heap-backed DeliveryBatch for storage engines that cannot provide a FileChannel
 * (e.g. PostgreSQL, SQLite, in-memory stores).
 *
 * transferTo() copies bytes via ByteBuffer — no OS sendfile(), but the broker and transport
 * layers are otherwise identical to the file-backed path.
 *
 * close() is a no-op — GC handles byte arrays.
 */
public final class ByteArrayDeliveryBatch implements DeliveryBatch {

    private final String topic;
    private final byte[] data;
    private final int recordCount;
    private final long firstOffset;
    private final long lastOffset;

    public ByteArrayDeliveryBatch(String topic, byte[] data, int recordCount, long firstOffset, long lastOffset) {
        this.topic = topic;
        this.data = data;
        this.recordCount = recordCount;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }

    @Override public String getTopic()       { return topic; }
    @Override public int getRecordCount()    { return recordCount; }
    @Override public long getTotalBytes()    { return data.length; }
    @Override public long getFirstOffset()   { return firstOffset; }
    @Override public long getLastOffset()    { return lastOffset; }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (position >= data.length) {
            return -1;
        }
        ByteBuffer buf = ByteBuffer.wrap(data, (int) position, data.length - (int) position);
        return target.write(buf);
    }

    @Override
    public void close() {
        // GC handles byte arrays — nothing to release
    }
}
