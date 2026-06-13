package com.messaging.broker.consistency;

import com.messaging.broker.compaction.CompactionIndex;

import java.nio.charset.StandardCharsets;

/**
 * Compaction-invariant keyspace digest for pipe-consistency checks.
 *
 * <p>Folds every compaction-index entry {@code (msgKey, latestOffset)} with
 * {@code latestOffset <= watermark} into {@code B} XOR buckets selected by the key's 64-bit
 * hash. Because compaction never changes the latest offset per key, two nodes holding the
 * same logical state produce identical digests regardless of when either side compacted.
 *
 * <p>Properties:
 * <ul>
 *   <li>order-independent — XOR folding; iteration order does not matter</li>
 *   <li>O(1) memory — {@code B} longs of digest + {@code B} ints of count, computed in one
 *       streaming scan of the index (no map materialisation)</li>
 *   <li>any single-entry difference flips its bucket's digest with probability
 *       {@code 1 - 2^-64}; per-bucket counts additionally catch add/remove asymmetries</li>
 * </ul>
 */
public final class KeyspaceDigest {

    /** Result of one digest scan: per-bucket XOR digests and entry counts. */
    public static final class Result {
        public final long[] digests;
        public final int[] counts;
        public final long entriesScanned;

        public Result(long[] digests, int[] counts, long entriesScanned) {
            this.digests = digests;
            this.counts = counts;
            this.entriesScanned = entriesScanned;
        }
    }

    private KeyspaceDigest() {}

    /**
     * Compute bucket digests for {@code topic} over index entries with
     * {@code latestOffset <= watermark}.
     *
     * @param yieldEvery cooperatively yield the thread every N scanned entries so a large scan
     *                   cannot monopolise a core on POS hardware (0 disables)
     */
    public static Result compute(CompactionIndex index, String topic, long watermark,
                                 int buckets, int yieldEvery) {
        long[] digests = new long[buckets];
        int[] counts = new int[buckets];
        long[] scanned = new long[1];

        index.forEachEntry(topic, (msgKey, latestOffset, latestTimestampMs) -> {
            scanned[0]++;
            if (yieldEvery > 0 && scanned[0] % yieldEvery == 0) {
                Thread.yield();
            }
            if (latestOffset > watermark) {
                return; // beyond the child's watermark — lag, not part of the comparison
            }
            long keyHash = hash64(msgKey);
            int bucket = bucketOf(keyHash, buckets);
            digests[bucket] ^= contribution(keyHash, latestOffset);
            counts[bucket]++;
        });

        return new Result(digests, counts, scanned[0]);
    }

    /** 64-bit FNV-1a over the key's UTF-8 bytes, finalised with splitmix64. */
    public static long hash64(String msgKey) {
        byte[] bytes = msgKey.getBytes(StandardCharsets.UTF_8);
        long h = 0xcbf29ce484222325L;
        for (byte b : bytes) {
            h ^= (b & 0xffL);
            h *= 0x100000001b3L;
        }
        return splitmix64(h);
    }

    /** Bucket selection — uses unsigned remainder so negative hashes map correctly. */
    public static int bucketOf(long keyHash, int buckets) {
        return (int) Long.remainderUnsigned(keyHash, buckets);
    }

    /** Order-independent per-entry contribution; mixes the offset so stale entries differ. */
    public static long contribution(long keyHash, long latestOffset) {
        return splitmix64(keyHash ^ splitmix64(latestOffset));
    }

    private static long splitmix64(long z) {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
