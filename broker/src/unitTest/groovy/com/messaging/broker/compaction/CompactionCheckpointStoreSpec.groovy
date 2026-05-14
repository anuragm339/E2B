package com.messaging.broker.compaction

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class CompactionCheckpointStoreSpec extends Specification {

    @TempDir
    Path tempDir

    SharedRocksDb sharedDb
    CompactionCheckpointStore store

    def setup() {
        sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        store = new CompactionCheckpointStore(sharedDb)
    }

    def cleanup() {
        sharedDb?.close()
    }

    def "loadCheckpoint returns -1 when no checkpoint has been saved"() {
        expect:
        store.loadCheckpoint("prices-v1", 0) == -1L
    }

    def "saveCheckpoint and loadCheckpoint round-trip"() {
        when:
        store.saveCheckpoint("prices-v1", 0, 42L)

        then:
        store.loadCheckpoint("prices-v1", 0) == 42L
    }

    def "checkpoints are scoped per topic-partition"() {
        when:
        store.saveCheckpoint("topic-a", 0, 10L)
        store.saveCheckpoint("topic-b", 0, 20L)
        store.saveCheckpoint("topic-a", 1, 30L)

        then:
        store.loadCheckpoint("topic-a", 0) == 10L
        store.loadCheckpoint("topic-b", 0) == 20L
        store.loadCheckpoint("topic-a", 1) == 30L
    }

    def "saveCheckpoint overwrites previous value"() {
        given:
        store.saveCheckpoint("prices-v1", 0, 100L)

        when:
        store.saveCheckpoint("prices-v1", 0, 500L)

        then:
        store.loadCheckpoint("prices-v1", 0) == 500L
    }

    def "checkpoint survives DB reopen"() {
        given:
        store.saveCheckpoint("prices-v1", 0, 999L)
        sharedDb.close()

        when:
        sharedDb = new SharedRocksDb(tempDir.toString(), 8 * 1024 * 1024L)
        sharedDb.init()
        store = new CompactionCheckpointStore(sharedDb)

        then:
        store.loadCheckpoint("prices-v1", 0) == 999L
    }
}
