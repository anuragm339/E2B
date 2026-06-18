package com.messaging.broker.snapshot

import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class DownloadRefreshServiceSpec extends Specification {

    DownloadRefreshOrchestrator orchestrator = Mock()
    RefreshCoordinator refreshCoordinator = Mock()
    StorageEngine storage = Mock()

    DownloadRefreshService service = new DownloadRefreshService(orchestrator, refreshCoordinator, storage, new BootstrapProgressTracker())

    def "SNAPSHOT success refreshes every topic from the manifest"() {
        given:
        def manifest = new SnapshotManifest(1L, ["prices-v1": 5L, "reference-data-v5": 9L])
        orchestrator.bootstrap() >> DownloadRefreshResult.ok(BootstrapSource.SNAPSHOT, manifest)

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
        orchestrator.bootstrap() >> DownloadRefreshResult.ok(BootstrapSource.CLOUD, null)
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)

        when:
        service.runBootstrapAndRefresh()

        then:
        1 * refreshCoordinator.startRefresh("t-a") >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("t-b") >> CompletableFuture.completedFuture(null)
    }

    def "a failed bootstrap does NOT trigger any consumer refresh"() {
        given:
        orchestrator.bootstrap() >> DownloadRefreshResult.failure(BootstrapSource.CLOUD, "cloud unreachable")

        when:
        def result = service.runBootstrapAndRefresh()

        then:
        0 * refreshCoordinator.startRefresh(_)
        !result.success
    }

    def "a failing startRefresh for one topic does not abort the others"() {
        given:
        orchestrator.bootstrap() >> DownloadRefreshResult.ok(BootstrapSource.CLOUD, null)
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)
        refreshCoordinator.startRefresh("t-a") >> { throw new RuntimeException("boom") }

        when:
        service.runBootstrapAndRefresh()

        then:
        noExceptionThrown()
        1 * refreshCoordinator.startRefresh("t-b") >> CompletableFuture.completedFuture(null)
    }
}
