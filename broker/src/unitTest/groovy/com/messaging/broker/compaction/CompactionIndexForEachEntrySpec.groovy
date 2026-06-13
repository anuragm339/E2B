package com.messaging.broker.compaction

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

/**
 * Streaming-iteration contract shared by both CompactionIndex backends —
 * the pipe-consistency digest depends on it.
 */
class CompactionIndexForEachEntrySpec extends Specification {

    @TempDir
    Path tempDir

    def "in-memory backend streams exactly the topic's entries"() {
        given:
        def index = new InMemoryCompactionIndex()
        index.updateKey('t1', 'a', 1L, 10L)
        index.updateKey('t1', 'b', 2L, 20L)
        index.updateKey('t2', 'c', 3L, 30L)

        when:
        def seen = [:]
        index.forEachEntry('t1') { k, o, ts -> seen[k] = [o, ts] }

        then:
        seen == ['a': [1L, 10L], 'b': [2L, 20L]]
    }

    def "rocksdb backend streams exactly the topic's entries and skips meta keys"() {
        given:
        def sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        def index = new RocksDbCompactionIndex(sharedDb)
        index.updateKey('t1', 'a', 1L, 10L)
        index.updateKey('t1', 'a', 5L, 50L)   // supersede — creates a __meta__ stale entry too
        index.updateKey('t1', 'b', 2L, 20L)
        index.updateKey('t2', 'c', 3L, 30L)

        when:
        def seen = [:]
        index.forEachEntry('t1') { k, o, ts -> seen[k] = o }

        then: 'latest offsets only, no meta keys, no other topics'
        seen == ['a': 5L, 'b': 2L]

        cleanup:
        sharedDb?.close()
    }

    def "rocksdb backend streams large keyspaces without materialising them"() {
        given:
        def sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        def index = new RocksDbCompactionIndex(sharedDb)
        (1..5000).each { index.updateKey('big', "key-$it".toString(), (long) it, 1L) }

        when:
        long count = 0
        long offsetSum = 0
        index.forEachEntry('big') { k, o, ts -> count++; offsetSum += o }

        then:
        count == 5000
        offsetSum == (1..5000).sum()

        cleanup:
        sharedDb?.close()
    }
}
