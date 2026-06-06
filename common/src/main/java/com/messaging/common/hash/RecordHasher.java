package com.messaging.common.hash;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Canonical hashing contract for PipeConsistency.
 *
 * The byte format here is load-bearing: provider/common and cloud-server MUST emit
 * byte-for-byte identical hashes for the same logical record. Any change requires a
 * version bump and forced recomputation across all nodes.
 */
public final class RecordHasher {

    private static final byte VERSION_RECORD = 0x01;
    private static final byte VERSION_COMBINE = 0x02;
    private static final byte VERSION_MERGE_NODES = 0x03;

    public static final int HASH_LEN = 16;
    public static final byte[] EMPTY_HASH = new byte[HASH_LEN];

    private RecordHasher() {}

    public static int recordCrc(long offset, String msgKey, char eventTypeCode, String data) {
        byte[] keyBytes = msgKey == null ? new byte[0] : msgKey.getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = data == null ? null : data.getBytes(StandardCharsets.UTF_8);

        ByteBuffer hdr = ByteBuffer.allocate(1 + 8 + 1 + 4 + 4)
                .put(VERSION_RECORD)
                .putLong(offset)
                .put((byte) eventTypeCode)
                .putInt(keyBytes.length)
                .putInt(dataBytes == null ? -1 : dataBytes.length);

        CRC32C crc = new CRC32C();
        crc.update(hdr.array(), 0, hdr.position());
        if (keyBytes.length > 0) {
            crc.update(keyBytes, 0, keyBytes.length);
        }
        if (dataBytes != null && dataBytes.length > 0) {
            crc.update(dataBytes, 0, dataBytes.length);
        }
        return (int) crc.getValue();
    }

    public static byte[] combine(byte[] prev, int recordCrc) {
        if (prev == null || prev.length != HASH_LEN) {
            throw new IllegalArgumentException("prev must be " + HASH_LEN + " bytes");
        }
        Hasher h = Hashing.murmur3_128().newHasher();
        h.putByte(VERSION_COMBINE);
        h.putBytes(prev);
        h.putInt(recordCrc);
        return h.hash().asBytes();
    }

    public static byte[] mergeNodes(byte[] left, byte[] right) {
        if (left == null || left.length != HASH_LEN || right == null || right.length != HASH_LEN) {
            throw new IllegalArgumentException("merge inputs must be " + HASH_LEN + " bytes");
        }
        Hasher h = Hashing.murmur3_128().newHasher();
        h.putByte(VERSION_MERGE_NODES);
        h.putBytes(left);
        h.putBytes(right);
        return h.hash().asBytes();
    }

    public static byte[] merkleRoot(List<byte[]> leaves) {
        if (leaves == null || leaves.isEmpty()) {
            return EMPTY_HASH.clone();
        }
        byte[][] level = leaves.toArray(new byte[0][]);
        for (byte[] leaf : level) {
            if (leaf == null || leaf.length != HASH_LEN) {
                throw new IllegalArgumentException("leaf must be " + HASH_LEN + " bytes");
            }
        }
        while (level.length > 1) {
            int outLen = (level.length + 1) / 2;
            byte[][] next = new byte[outLen][];
            for (int i = 0; i < outLen; i++) {
                byte[] left = level[2 * i];
                byte[] right = (2 * i + 1 < level.length) ? level[2 * i + 1] : left;
                next[i] = mergeNodes(left, right);
            }
            level = next;
        }
        return level[0];
    }
}
