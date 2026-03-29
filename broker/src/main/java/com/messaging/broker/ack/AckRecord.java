package com.messaging.broker.ack;

import java.nio.ByteBuffer;

/**
 * Immutable value stored in RocksDB for each (topic, group, msgKey) triple.
 * Fixed 16-byte binary serialisation: [offset:8B][ackedAtMs:8B].
 */
public final class AckRecord {

    public final long offset;
    public final long ackedAtMs;

    public AckRecord(long offset, long ackedAtMs) {
        this.offset = offset;
        this.ackedAtMs = ackedAtMs;
    }

    /** Serialise to exactly 16 bytes. */
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(offset);
        buf.putLong(ackedAtMs);
        return buf.array();
    }

    /** Deserialise from exactly 16 bytes. */
    public static AckRecord fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long offset = buf.getLong();
        long ackedAtMs = buf.getLong();
        return new AckRecord(offset, ackedAtMs);
    }

    @Override
    public String toString() {
        return "AckRecord{offset=" + offset + ", ackedAtMs=" + ackedAtMs + "}";
    }
}
