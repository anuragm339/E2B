package com.messaging.broker.snapshot

import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class DownloadRefreshServiceSpec extends Specification {

    DownloadRefreshOrchestrator orchestrator = Mock()
    RefreshCoordinator refreshCoordinator = Mock()
    StorageEngine storage = Mock()
    BareMetalResetService bareMetalReset = Mock()

    DownloadRefreshService service = new DownloadRefreshService(orchestrator, refreshCoordinator, storage, new BootstrapProgressTracker(), bareMetalReset)

    def "SNAPSHOT success refreshes every topic from the manifest"() {
        given:
        def manifest = new SnapshotManifest(1L, ["prices-v1": 5L, "reference-data-v5": 9L])
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.SNAPSHOT, manifest)

        when:
        def result = service.runBootstrapAndRefresh()

        then:
        1 * refreshCoordinator.startRefresh("prices-v1") >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("reference-data-v5") >> CompletableFuture.completedFuture(null)
        0 * storage.getTopicNames()
        result.source == BootstrapSource.SNAPSHOT
    }

    def "CLOUD/incremental success (no manifest) refreshes topics discovered in storage"() {
        given:
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD, null)
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)

        when:
        service.runBootstrapAndRefresh()

        then:
        1 * refreshCoordinator.startRefresh("t-a") >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("t-b") >> CompletableFuture.completedFuture(null)
    }

    def "a failed bootstrap does NOT trigger any consumer refresh"() {
        given:
        orchestrator.bootstrap(_) >> DownloadRefreshResult.failure(BootstrapSource.CLOUD, "cloud unreachable")

        when:
        def result = service.runBootstrapAndRefresh()

        then:
        0 * refreshCoordinator.startRefresh(_)
        !result.success
    }

    def "LOCAL refresh replays all topics WITHOUT any bootstrap/download"() {
        given:
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)

        when:
        def result = service.runRefresh(RefreshType.LOCAL)

        then: "no orchestrator bootstrap at all; just the local replay per topic"
        0 * orchestrator.bootstrap(_)
        1 * refreshCoordinator.startRefresh("t-a") >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("t-b") >> CompletableFuture.completedFuture(null)
        result.success
        result.source == null
    }

    def "BARE_METAL triggers the reset and does NO bootstrap or consumer refresh"() {
        when:
        def result = service.runRefresh(RefreshType.BARE_METAL)

        then:
        1 * bareMetalReset.reset()
        0 * orchestrator.bootstrap(_)
        0 * refreshCoordinator.startRefresh(_)
        result.success
    }

    def "a forced source type is passed through to the orchestrator"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)

        when:
        service.runRefresh(RefreshType.CLOUD)

        then: "orchestrator is asked to force CLOUD"
        1 * orchestrator.bootstrap(BootstrapSource.CLOUD) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD, null)
        1 * refreshCoordinator.startRefresh("t-a") >> CompletableFuture.completedFuture(null)
    }

    def "DOWNLOAD (auto) passes a null forced source to the orchestrator"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)

        when:
        service.runRefresh(RefreshType.DOWNLOAD)

        then:
        1 * orchestrator.bootstrap(null) >> DownloadRefreshResult.ok(BootstrapSource.INCREMENTAL_PARENT, null)
        1 * refreshCoordinator.startRefresh("t-a") >> CompletableFuture.completedFuture(null)
    }

    def "a failing startRefresh for one topic does not abort the others"() {
        given:
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD, null)
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)
        refreshCoordinator.startRefresh("t-a") >> { throw new RuntimeException("boom") }

        when:
        service.runBootstrapAndRefresh()

        then:
        noExceptionThrown()
        1 * refreshCoordinator.startRefresh("t-b") >> CompletableFuture.completedFuture(null)
    }
}
