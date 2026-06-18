package com.messaging.broker.snapshot

import com.messaging.broker.core.TopologyManager
import spock.lang.Specification

import java.nio.file.Path

class DownloadRefreshOrchestratorSpec extends Specification {

    TopologyManager topology = Mock()
    BootstrapSourceClient client = Mock()
    LocalStateCleaner cleaner = Mock()
    SnapshotRestorer restorer = Mock()

    DownloadRefreshOrchestrator orchestrator = new DownloadRefreshOrchestrator(
            "/tmp/data", topology, client, cleaner, restorer)

    // ── source selection ─────────────────────────────────────────────────────

    def "root node (no parent) bootstraps from the cloud"() {
        expect:
        orchestrator.chooseSource(null) == BootstrapSource.CLOUD
    }

    def "unhealthy parent (mid-refresh) escalates to the cloud"() {
        given:
        client.isParentHealthy("http://parent") >> false

        expect:
        orchestrator.chooseSource("http://parent") == BootstrapSource.CLOUD
    }

    def "healthy parent with a snapshot uses the SNAPSHOT path"() {
        given:
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> true

        expect:
        orchestrator.chooseSource("http://parent") == BootstrapSource.SNAPSHOT
    }

    def "healthy parent without a snapshot uses the INCREMENTAL_PARENT path"() {
        given:
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> false

        expect:
        orchestrator.chooseSource("http://parent") == BootstrapSource.INCREMENTAL_PARENT
    }

    // ── sequencing per path ──────────────────────────────────────────────────

    def "SNAPSHOT path: download + restore BEFORE clearing state (crash-safe order)"() {
        given:
        topology.getCurrentParentUrl() >> "http://parent"
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> true
        def zip = Path.of("/tmp/data/snapshots/incoming.zip")
        def manifest = new SnapshotManifest(1L, ["prices-v1": 9L])

        when:
        def result = orchestrator.bootstrap()

        then: "download, then restore, then clearState — in that order"
        1 * client.downloadSnapshot("http://parent", Path.of("/tmp/data")) >> zip

        then:
        1 * restorer.restore(zip, Path.of("/tmp/data")) >> manifest

        then:
        1 * cleaner.clearState("/tmp/data")
        0 * cleaner.clearTopicData(_)

        and:
        result.success
        result.source == BootstrapSource.SNAPSHOT
        result.manifest.is(manifest)
    }

    def "INCREMENTAL_PARENT path: clear state + topic data, then k-way merge pull"() {
        given:
        topology.getCurrentParentUrl() >> "http://parent"
        client.isParentHealthy("http://parent") >> true
        client.snapshotAvailable("http://parent") >> false

        when:
        def result = orchestrator.bootstrap()

        then:
        1 * cleaner.clearState("/tmp/data")
        1 * cleaner.clearTopicData("/tmp/data")
        1 * client.bulkFetchFromParent("http://parent", "/tmp/data")
        0 * restorer.restore(_, _)

        and:
        result.success
        result.source == BootstrapSource.INCREMENTAL_PARENT
    }

    def "CLOUD path: clear state + topic data, then pull from cloud"() {
        given:
        topology.getCurrentParentUrl() >> null

        when:
        def result = orchestrator.bootstrap()

        then:
        1 * cleaner.clearState("/tmp/data")
        1 * cleaner.clearTopicData("/tmp/data")
        1 * client.bulkFetchFromCloud("/tmp/data")

        and:
        result.success
        result.source == BootstrapSource.CLOUD
    }

    def "a failure during sourcing is returned as a result, never thrown"() {
        given:
        topology.getCurrentParentUrl() >> null
        client.bulkFetchFromCloud(_) >> { throw new RuntimeException("cloud unreachable") }

        when:
        def result = orchestrator.bootstrap()

        then:
        noExceptionThrown()
        !result.success
        result.source == BootstrapSource.CLOUD
        result.error.contains("cloud unreachable")
    }
}
