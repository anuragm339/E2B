package com.messaging.broker.compaction

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

/**
 * Verifies {@link SharedRocksDb#clearCompactionAndAck()} — the in-place wipe that replaced deleting
 * the {@code ack-store/} directory. Deleting the dir corrupted the long-lived open handle (every
 * subsequent write threw "No such file or directory") and stalled the pipe on the first post-wipe
 * record. Clearing in place must (a) empty both column families and (b) leave the handle usable.
 */
class SharedRocksDbSpec extends Specification {

    @TempDir
    Path tempDir

    SharedRocksDb sharedDb
    RocksDbCompactionIndex index

    def setup() {
        sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        index = new RocksDbCompactionIndex(sharedDb)
    }

    def cleanup() {
        sharedDb?.close()
    }

    def "clearCompactionAndAck empties the compaction CF and the handle stays writable afterwards"() {
        given: "several keys spanning the compaction CF"
        (1..50).each { index.updateKey("prices-v1", "product-${it}", it as long, System.currentTimeMillis()) }
        assert index.getLatestOffsetAndTimestamp("prices-v1", "product-25") != null

        when: "the CFs are cleared in place (no file deletion)"
        sharedDb.clearCompactionAndAck()

        then: "the index is empty"
        index.getLatestOffsetAndTimestamp("prices-v1", "product-1") == null
        index.getLatestOffsetAndTimestamp("prices-v1", "product-50") == null

        and: "the SAME open handle still accepts writes — i.e. it was NOT corrupted by the wipe"
        index.updateKey("prices-v1", "product-new", 999L, System.currentTimeMillis())
        index.getLatestOffsetAndTimestamp("prices-v1", "product-new") != null
    }

    def "clearCompactionAndAck is a no-op safe call on empty column families"() {
        when:
        sharedDb.clearCompactionAndAck()

        then:
        noExceptionThrown()

        and: "still writable"
        index.updateKey("ref-data-v5", "k1", 1L, System.currentTimeMillis())
        index.getLatestOffsetAndTimestamp("ref-data-v5", "k1") != null
    }
}
