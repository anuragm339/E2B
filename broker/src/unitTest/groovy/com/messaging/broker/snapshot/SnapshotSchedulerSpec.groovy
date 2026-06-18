package com.messaging.broker.snapshot

import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.nio.file.Path

class SnapshotSchedulerSpec extends Specification {

    StorageEngine storage = Mock()
    SnapshotBuilder builder = Mock()
    SnapshotStore store = Mock()
    SnapshotScheduler scheduler = new SnapshotScheduler(true, "/tmp/data", storage, builder, store)

    def "buildNow gathers per-topic heads and publishes the snapshot"() {
        given:
        storage.getTopicNames() >> (["prices-v1", "reference-data-v5"] as Set)
        storage.getCurrentOffset("prices-v1", 0) >> 41L
        storage.getCurrentOffset("reference-data-v5", 0) >> -1L
        store.tempZip() >> Path.of("/tmp/data/snapshots/latest.zip.tmp")
        def manifest = new SnapshotManifest(123L, ["prices-v1": 41L, "reference-data-v5": -1L])

        when:
        def result = scheduler.buildNow()

        then: "builder is invoked with the gathered heads, then published"
        1 * builder.build(Path.of("/tmp/data"), Path.of("/tmp/data/snapshots/latest.zip.tmp"),
                ["prices-v1": 41L, "reference-data-v5": -1L]) >> manifest
        1 * store.publish(Path.of("/tmp/data/snapshots/latest.zip.tmp"), manifest)
        result.is(manifest)
    }

    def "buildNow returns null and never throws when the build fails"() {
        given:
        storage.getTopicNames() >> (["prices-v1"] as Set)
        storage.getCurrentOffset("prices-v1", 0) >> 1L
        store.tempZip() >> Path.of("/tmp/data/snapshots/latest.zip.tmp")
        builder.build(_, _, _) >> { throw new RuntimeException("disk full") }

        when:
        def result = scheduler.buildNow()

        then:
        noExceptionThrown()
        result == null
    }

    def "scheduled does nothing when disabled"() {
        given:
        def disabled = new SnapshotScheduler(false, "/tmp/data", storage, builder, store)

        when:
        disabled.scheduled()

        then:
        0 * storage.getTopicNames()
        0 * builder.build(_, _, _)
    }
}
