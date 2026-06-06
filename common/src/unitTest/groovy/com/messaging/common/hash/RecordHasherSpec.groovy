package com.messaging.common.hash

import spock.lang.Specification

class RecordHasherSpec extends Specification {

    def "recordCrc is deterministic for identical inputs"() {
        when:
        int a = RecordHasher.recordCrc(42L, 'k1', 'M' as char, '{"v":1}')
        int b = RecordHasher.recordCrc(42L, 'k1', 'M' as char, '{"v":1}')

        then:
        a == b
    }

    def "recordCrc changes when offset changes"() {
        expect:
        RecordHasher.recordCrc(1L, 'k', 'M' as char, 'd') !=
                RecordHasher.recordCrc(2L, 'k', 'M' as char, 'd')
    }

    def "recordCrc changes when key changes"() {
        expect:
        RecordHasher.recordCrc(1L, 'k1', 'M' as char, 'd') !=
                RecordHasher.recordCrc(1L, 'k2', 'M' as char, 'd')
    }

    def "recordCrc changes when data changes"() {
        expect:
        RecordHasher.recordCrc(1L, 'k', 'M' as char, 'a') !=
                RecordHasher.recordCrc(1L, 'k', 'M' as char, 'b')
    }

    def "recordCrc distinguishes DELETE (null data) from empty-string MESSAGE"() {
        expect:
        RecordHasher.recordCrc(1L, 'k', 'D' as char, null) !=
                RecordHasher.recordCrc(1L, 'k', 'M' as char, '')
    }

    def "recordCrc distinguishes event type even with same payload"() {
        expect:
        RecordHasher.recordCrc(1L, 'k', 'M' as char, 'data') !=
                RecordHasher.recordCrc(1L, 'k', 'D' as char, 'data')
    }

    def "combine is deterministic"() {
        given:
        int crc = RecordHasher.recordCrc(1L, 'k', 'M' as char, 'd')

        when:
        byte[] a = RecordHasher.combine(RecordHasher.EMPTY_HASH, crc)
        byte[] b = RecordHasher.combine(RecordHasher.EMPTY_HASH, crc)

        then:
        a == b
        a.length == RecordHasher.HASH_LEN
    }

    def "combine is order-sensitive"() {
        given:
        int c1 = RecordHasher.recordCrc(1L, 'k', 'M' as char, 'a')
        int c2 = RecordHasher.recordCrc(2L, 'k', 'M' as char, 'b')

        when:
        byte[] forward = RecordHasher.combine(RecordHasher.combine(RecordHasher.EMPTY_HASH, c1), c2)
        byte[] reverse = RecordHasher.combine(RecordHasher.combine(RecordHasher.EMPTY_HASH, c2), c1)

        then:
        forward != reverse
    }

    def "combine rejects prev of wrong length"() {
        when:
        RecordHasher.combine(new byte[8], 0)

        then:
        thrown(IllegalArgumentException)
    }

    def "mergeNodes is deterministic and order-sensitive"() {
        given:
        byte[] left = RecordHasher.combine(RecordHasher.EMPTY_HASH, 1)
        byte[] right = RecordHasher.combine(RecordHasher.EMPTY_HASH, 2)

        when:
        byte[] lr = RecordHasher.mergeNodes(left, right)
        byte[] lrAgain = RecordHasher.mergeNodes(left, right)
        byte[] rl = RecordHasher.mergeNodes(right, left)

        then:
        lr == lrAgain
        lr != rl
        lr.length == RecordHasher.HASH_LEN
    }

    def "merkleRoot with empty list returns EMPTY_HASH"() {
        expect:
        RecordHasher.merkleRoot([]) == RecordHasher.EMPTY_HASH
    }

    def "merkleRoot with single leaf returns that leaf"() {
        given:
        byte[] only = RecordHasher.combine(RecordHasher.EMPTY_HASH, 7)

        expect:
        RecordHasher.merkleRoot([only]) == only
    }

    def "merkleRoot with two leaves equals mergeNodes of the pair"() {
        given:
        byte[] a = RecordHasher.combine(RecordHasher.EMPTY_HASH, 1)
        byte[] b = RecordHasher.combine(RecordHasher.EMPTY_HASH, 2)

        expect:
        RecordHasher.merkleRoot([a, b]) == RecordHasher.mergeNodes(a, b)
    }

    def "merkleRoot duplicates last leaf on odd-leaf level"() {
        given:
        byte[] a = RecordHasher.combine(RecordHasher.EMPTY_HASH, 1)
        byte[] b = RecordHasher.combine(RecordHasher.EMPTY_HASH, 2)
        byte[] c = RecordHasher.combine(RecordHasher.EMPTY_HASH, 3)

        when:
        // level 0: [a, b, c] -> level 1: [merge(a,b), merge(c,c)] -> root
        byte[] expected = RecordHasher.mergeNodes(
                RecordHasher.mergeNodes(a, b),
                RecordHasher.mergeNodes(c, c)
        )

        then:
        RecordHasher.merkleRoot([a, b, c]) == expected
    }

    def "rolling combine over a stream equals fold over the same crc sequence"() {
        given:
        def records = [
                [1L, 'k1', 'M' as char, 'a'],
                [2L, 'k2', 'M' as char, 'b'],
                [3L, 'k1', 'D' as char, null],
                [4L, 'k3', 'M' as char, '{"x":42}']
        ]
        def crcs = records.collect { RecordHasher.recordCrc(it[0] as long, it[1] as String, it[2] as char, it[3] as String) }

        when:
        byte[] folded = RecordHasher.EMPTY_HASH
        crcs.each { folded = RecordHasher.combine(folded, it) }

        and:
        byte[] manual = RecordHasher.combine(
                RecordHasher.combine(
                        RecordHasher.combine(
                                RecordHasher.combine(RecordHasher.EMPTY_HASH, crcs[0]),
                                crcs[1]),
                        crcs[2]),
                crcs[3])

        then:
        folded == manual
    }

    // CRITICAL: any change to a golden value means the canonical byte format
    // changed. If the change is intentional, bump VERSION_RECORD / VERSION_COMBINE
    // / VERSION_MERGE_NODES in RecordHasher and update the matching goldens in
    // cloud-server's RecordHasherSpec.

    def "golden vector for recordCrc locks the byte format"() {
        expect:
        Integer.toHexString(
                RecordHasher.recordCrc(100L, 'orderbook-key', 'M' as char, '{"price":99.5}')
        ) == '2bd6bf6f'
    }

    def "golden vectors for combine and mergeNodes lock the byte format"() {
        given:
        int crc1 = RecordHasher.recordCrc(100L, 'orderbook-key', 'M' as char, '{"price":99.5}')
        int crc2 = RecordHasher.recordCrc(101L, 'orderbook-key', 'D' as char, null)

        when:
        byte[] step1 = RecordHasher.combine(RecordHasher.EMPTY_HASH, crc1)
        byte[] step2 = RecordHasher.combine(step1, crc2)
        byte[] merged = RecordHasher.mergeNodes(step1, step2)

        then:
        toHex(step1)  == '86d950683b2a818cd9672b88e7ca2254'
        toHex(step2)  == '15c75b8d6c0e99cb0b042eaa9eae1647'
        toHex(merged) == '61be706731fffd9ca3ee4046de28bf0b'
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder()
        for (byte b : bytes) {
            sb.append(String.format('%02x', b))
        }
        return sb.toString()
    }
}
