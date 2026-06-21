package com.messaging.broker.snapshot

import com.messaging.broker.compaction.SharedRocksDb
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.DeliveryStateStore
import com.messaging.broker.core.TopologyManager
import com.messaging.common.api.PipeConnector
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.nio.file.Path

class DownloadRefreshOrchestratorSpec extends Specification {

    TopologyManager topology = Mock()
    BootstrapSourceClient client = Mock()
    LocalStateCleaner cleaner = Mock()
    SnapshotRestorer restorer = Mock()
    PipeConnector pipeConnector = Mock()
    StorageEngine storage = Mock()
    ConsumerOffsetTracker consumerOffsets = Mock()
    DeliveryStateStore deliveryState = Mock()
    SharedRocksDb sharedRocksDb = Mock()

    DownloadRefreshOrchestrator orchestrator = new DownloadRefreshOrchestrator(
            "/tmp/data", topology, client, cleaner, restorer, pipeConnector, storage,
            consumerOffsets, deliveryState, sharedRocksDb, 0L) // jitter 0 in tests

    // ── source selection ─────────────────────────────────────────────────────

    def "root node (no parent) bootstraps from the cloud"() {
        expect:
        orchestrator.chooseSource(null) == BootstrapSource.CLOUD_SYNC
    }

    def "unhealthy parent (mid-refresh) escalates to the cloud"() {
        given:
        client.isParentHealthy("http://parent") >> false

        expect:
        orchestrator.chooseSource("http://parent") == BootstrapSource.CLOUD_SYNC
    }

    def "healthy parent with a snapshot uses the SNAPSHOT path"() {
        given:
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> true

        expect:
        orchestrator.chooseSource("http://parent") == BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD
    }

    def "healthy parent without a snapshot uses the INCREMENTAL_PARENT path"() {
        given:
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> false

        expect:
        orchestrator.chooseSource("http://parent") == BootstrapSource.PIPE_AND_PROVIDER_STREAM
    }

    // ── quiesce ───────────────────────────────────────────────────────────────

    def "download bootstrap pauses the pipe only around local wipe and storage recovery"() {
        given:
        topology.getCurrentParentUrl() >> null // CLOUD_SYNC path

        when:
        orchestrator.bootstrap()

        then: "pipe paused immediately before local destructive work"
        1 * pipeConnector.pausePipeCalls()

        then: "offset/delivery stores quiesced before the wipe so a stray flush cannot resurrect them"
        1 * consumerOffsets.quiesceForWipe()
        1 * deliveryState.quiesceForWipe()

        then:
        1 * storage.close()

        then: "RocksDB ack + compaction CFs cleared IN PLACE (not by deleting the dir under the open handle)"
        1 * sharedRocksDb.clearCompactionAndAck()

        then:
        1 * cleaner.clearState("/tmp/data")

        then:
        1 * cleaner.clearTopicData("/tmp/data")

        then: "pipe cursor reset to 0 (re-stream from start) while the pipe is still paused"
        1 * pipeConnector.resetOffset(0)

        then:
        1 * storage.recover()

        then: "stores resumed (reloaded from the wiped file) and pipe released, in that order"
        1 * deliveryState.resumeAfterWipe()
        1 * consumerOffsets.resumeAfterWipe()

        then:
        1 * pipeConnector.resumePipeCalls()

        and: "pipe-only — no separate bulk pull"
        0 * client.bulkFetchFromCloud(_)
    }

    // ── sequencing per path ──────────────────────────────────────────────────

    def "SNAPSHOT path: download + restore BEFORE clearing state (crash-safe order)"() {
        given:
        topology.getCurrentParentUrl() >> "http://parent"
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> true
        def zip = Path.of("/tmp/data/snapshots/incoming.zip")
        def manifest = new SnapshotManifest(1L, ["prices-v1": 9L], 777L)  // N* = 777

        when:
        def result = orchestrator.bootstrap()

        then: "download, then restore, then clearState — in that order"
        1 * client.downloadSnapshot("http://parent", Path.of("/tmp/data")) >> zip

        then:
        1 * restorer.restore(zip, Path.of("/tmp/data")) >> manifest

        then:
        1 * cleaner.clearState("/tmp/data")
        0 * cleaner.clearTopicData(_)

        then: "pipe resumes from N* (not 0) — only the tail since the snapshot"
        1 * pipeConnector.resetOffset(777L)

        and:
        result.success
        result.source == BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD
        result.manifest.is(manifest)
    }

    def "INCREMENTAL_PARENT path: wipe + reset pipe to 0 (pipe-only, no bulk pull)"() {
        given:
        topology.getCurrentParentUrl() >> "http://parent"
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> false

        when:
        def result = orchestrator.bootstrap()

        then: "local wipe + pipe reset to 0; pipe resumes and streams it — no bulk pull, no restore"
        1 * cleaner.clearState("/tmp/data")
        1 * cleaner.clearTopicData("/tmp/data")
        1 * pipeConnector.resetOffset(0)
        0 * client.bulkFetchFromParent(_, _)
        0 * restorer.restore(_, _)

        and:
        result.success
        result.source == BootstrapSource.PIPE_AND_PROVIDER_STREAM
    }

    def "CLOUD path: wipe + reset pipe to 0 (pipe-only, no bulk pull)"() {
        given:
        topology.getCurrentParentUrl() >> null

        when:
        def result = orchestrator.bootstrap()

        then:
        1 * cleaner.clearState("/tmp/data")
        1 * cleaner.clearTopicData("/tmp/data")
        1 * pipeConnector.resetOffset(0)
        0 * client.bulkFetchFromCloud(_)

        and:
        result.success
        result.source == BootstrapSource.CLOUD_SYNC
    }

    def "SNAPSHOT path escalates to the cloud when the parent fails mid-download"() {
        given:
        topology.getCurrentParentUrl() >> "http://parent"
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> true
        client.downloadSnapshot(_, _) >> { throw new RuntimeException("parent died mid-download") }

        when:
        def result = orchestrator.bootstrap()

        then: "falls back to a cloud bootstrap (pipe-only — wipe + reset, no bulk)"
        1 * pipeConnector.resetOffset(0)
        0 * client.bulkFetchFromCloud(_)
        result.success
        result.source == BootstrapSource.CLOUD_SYNC
    }

    def "if the stream path fails and cloud escalation ALSO fails, a failure result is returned (never thrown)"() {
        given: "no snapshot → STREAM; the wipe throws on every path (stream, then cloud)"
        topology.getCurrentParentUrl() >> "http://parent"
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> false
        cleaner.clearTopicData(_) >> { throw new RuntimeException("wipe failed everywhere") }

        when:
        def result = orchestrator.bootstrap()

        then:
        noExceptionThrown()
        !result.success
        result.source == BootstrapSource.CLOUD_SYNC
        result.error.contains("wipe failed everywhere")
    }

    def "a failure during sourcing is returned as a result, never thrown"() {
        given:
        topology.getCurrentParentUrl() >> null
        cleaner.clearTopicData(_) >> { throw new RuntimeException("cloud unreachable") }

        when:
        def result = orchestrator.bootstrap()

        then:
        noExceptionThrown()
        !result.success
        result.source == BootstrapSource.CLOUD_SYNC
        result.error.contains("cloud unreachable")
    }
}
