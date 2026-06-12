package com.messaging.broker.consistency

import com.messaging.broker.compaction.InMemoryCompactionIndex
import spock.lang.Specification

class KeyspaceDigestSpec extends Specification {

    static final int BUCKETS = 16

    def "identical keyspaces produce identical digests regardless of insertion order"() {
        given:
        def a = new InMemoryCompactionIndex()
        def b = new InMemoryCompactionIndex()
        def entries = (1..500).collect { ["key-${it}".toString(), 1000L + it] }
        entries.each { k, o -> a.updateKey('t', k, o, 1L) }
        entries.reverse().each { k, o -> b.updateKey('t', k, o, 1L) }

        when:
        def da = KeyspaceDigest.compute(a, 't', 2000L, BUCKETS, 0)
        def db = KeyspaceDigest.compute(b, 't', 2000L, BUCKETS, 0)

        then:
        da.digests == db.digests
        da.counts == db.counts
        da.entriesScanned == 500
    }

    def "watermark filter excludes entries beyond the watermark"() {
        given:
        def withinOnly = new InMemoryCompactionIndex()
        withinOnly.updateKey('t', 'k1', 10L, 1L)

        def withBeyond = new InMemoryCompactionIndex()
        withBeyond.updateKey('t', 'k1', 10L, 1L)
        withBeyond.updateKey('t', 'k2', 99L, 1L)   // beyond watermark — must not contribute

        expect:
        KeyspaceDigest.compute(withinOnly, 't', 50L, BUCKETS, 0).digests ==
                KeyspaceDigest.compute(withBeyond, 't', 50L, BUCKETS, 0).digests
    }

    def "a single key difference flips exactly that key's bucket"() {
        given:
        def a = new InMemoryCompactionIndex()
        def b = new InMemoryCompactionIndex()
        (1..100).each { a.updateKey('t', "key-$it".toString(), (long) it, 1L) }
        (1..100).each { b.updateKey('t', "key-$it".toString(), (long) it, 1L) }
        b.updateKey('t', 'key-7', 500L, 2L)   // advance one key on b

        when:
        def da = KeyspaceDigest.compute(a, 't', 1000L, BUCKETS, 0)
        def db = KeyspaceDigest.compute(b, 't', 1000L, BUCKETS, 0)
        def expectedBucket = KeyspaceDigest.bucketOf(KeyspaceDigest.hash64('key-7'), BUCKETS)

        then:
        (0..<BUCKETS).findAll { da.digests[it] != db.digests[it] } == [expectedBucket]
    }

    def "per-bucket counts catch a removed entry"() {
        given:
        def full = new InMemoryCompactionIndex()
        def missing = new InMemoryCompactionIndex()
        (1..50).each { full.updateKey('t', "key-$it".toString(), (long) it, 1L) }
        (1..49).each { missing.updateKey('t', "key-$it".toString(), (long) it, 1L) }

        when:
        def df = KeyspaceDigest.compute(full, 't', 100L, BUCKETS, 0)
        def dm = KeyspaceDigest.compute(missing, 't', 100L, BUCKETS, 0)
        def bucket = KeyspaceDigest.bucketOf(KeyspaceDigest.hash64('key-50'), BUCKETS)

        then:
        df.counts[bucket] == dm.counts[bucket] + 1
        df.digests[bucket] != dm.digests[bucket]
    }

    def "hash and bucket selection are deterministic and in range"() {
        expect:
        KeyspaceDigest.hash64('product-123') == KeyspaceDigest.hash64('product-123')
        KeyspaceDigest.hash64('product-123') != KeyspaceDigest.hash64('product-124')
        (1..1000).every {
            def b = KeyspaceDigest.bucketOf(KeyspaceDigest.hash64("k-$it"), BUCKETS)
            b >= 0 && b < BUCKETS
        }
    }

    def "cross-repo canonical vectors — must match cloud-server's KeyspaceDigestVectorTest"() {
        // The cloud-server repo pins THE SAME literals. If this fails after an edit, the two
        // implementations drifted and every child-vs-cloud check reports false divergence.
        // Fix the drift, never the literals (unless both repos change together).
        expect:
        KeyspaceDigest.hash64('product-123') == -6305294276135507699L
        KeyspaceDigest.hash64('prices-v1:k1') == 4333333253278312467L
        KeyspaceDigest.bucketOf(KeyspaceDigest.hash64('product-123'), 64) == 13
        KeyspaceDigest.contribution(KeyspaceDigest.hash64('product-123'), 1207504L) == -8154119652901298443L
    }

    def "empty topic digests to all-zero buckets"() {
        expect:
        def r = KeyspaceDigest.compute(new InMemoryCompactionIndex(), 'none', 100L, BUCKETS, 0)
        r.digests.every { it == 0L }
        r.counts.every { it == 0 }
    }
}
