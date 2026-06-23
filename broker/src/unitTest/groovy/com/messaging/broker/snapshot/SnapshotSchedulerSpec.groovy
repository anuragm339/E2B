package com.messaging.broker.snapshot

import com.messaging.common.api.PipeConnector
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.nio.file.Path

class SnapshotSchedulerSpec extends Specification {

    StorageEngine storage = Mock()
    SnapshotBuilder builder = Mock()
    SnapshotStore store = Mock()
    PipeConnector pipe = Mock()
    SnapshotScheduler scheduler = new SnapshotScheduler(true, "/tmp/data", storage, builder, store, pipe)

    def "buildNow captures heads + N* under a paused pipe, then publishes"() {
        given:
        storage.getTopicNames() >> (["prices-v1", "reference-data-v5"] as Set)
        storage.getCurrentOffset("prices-v1", 0) >> 41L
        storage.getCurrentOffset("reference-data-v5", 0) >> -1L
        pipe.getCurrentOffset() >> 99L   // N*
        store.tempZip() >> Path.of("/tmp/data/snapshots/latest.zip.tmp")
        def manifest = new SnapshotManifest(123L, ["prices-v1": 41L, "reference-data-v5": -1L], 99L)

        when:
        def result = scheduler.buildNow()

        then: "pipe paused for the consistent cut, builder invoked with heads + N*, then resumed"
        1 * pipe.pausePipeCalls()
        1 * builder.build(Path.of("/tmp/data"), Path.of("/tmp/data/snapshots/latest.zip.tmp"),
                ["prices-v1": 41L, "reference-data-v5": -1L], 99L) >> manifest
        1 * store.publish(Path.of("/tmp/data/snapshots/latest.zip.tmp"), manifest)
        1 * pipe.resumePipeCalls()
        result.is(manifest)
    }

    def "buildNow resumes the pipe and returns null when the build fails"() {
        given:
        storage.getTopicNames() >> (["prices-v1"] as Set)
        storage.getCurrentOffset("prices-v1", 0) >> 1L
        pipe.getCurrentOffset() >> 5L
        store.tempZip() >> Path.of("/tmp/data/snapshots/latest.zip.tmp")
        builder.build(_, _, _, _) >> { throw new RuntimeException("disk full") }

        when:
        def result = scheduler.buildNow()

        then: "never throws, and the pipe is always resumed (finally)"
        noExceptionThrown()
        result == null
        1 * pipe.resumePipeCalls()
    }

    def "scheduled does nothing when disabled"() {
        given:
        def disabled = new SnapshotScheduler(false, "/tmp/data", storage, builder, store, pipe)

        when:
        disabled.scheduled()

        then:
        0 * storage.getTopicNames()
        0 * builder.build(_, _, _, _)
        0 * pipe.pausePipeCalls()
    }
}
