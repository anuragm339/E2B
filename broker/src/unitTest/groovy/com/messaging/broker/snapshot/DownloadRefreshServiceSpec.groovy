package com.messaging.broker.snapshot

import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.legacy.LegacyClientConfig
import com.messaging.common.api.NetworkServer
import com.messaging.common.api.StorageEngine
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class DownloadRefreshServiceSpec extends Specification {

    DownloadRefreshOrchestrator orchestrator = Mock()
    RefreshCoordinator refreshCoordinator = Mock()
    StorageEngine storage = Mock()
    BareMetalResetService bareMetalReset = Mock()
    NetworkServer networkServer = Mock()
    LegacyClientConfig legacyClientConfig = Mock()  // getServiceTopics() defaults to [:] (Spock)

    DownloadRefreshService service = new DownloadRefreshService(orchestrator, refreshCoordinator, storage, new BootstrapProgressTracker(), bareMetalReset, networkServer, legacyClientConfig)

    def "SNAPSHOT success refreshes every topic from the manifest"() {
        given:
        def manifest = new SnapshotManifest(1L, ["prices-v1": 5L, "reference-data-v5": 9L])
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD, manifest)

        when:
        def result = service.runBootstrapAndRefresh()

        then:
        1 * refreshCoordinator.startRefresh("prices-v1", _) >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("reference-data-v5", _) >> CompletableFuture.completedFuture(null)
        0 * storage.getTopicNames()
        result.source == BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD
    }

    def "CLOUD/incremental success (no manifest) refreshes topics discovered in storage"() {
        given:
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD_SYNC, null)
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)

        when:
        service.runBootstrapAndRefresh()

        then: "fresh-install bootstrap labels consumer refreshes FRESH_INSTALL (not the raw CLOUD_SYNC source)"
        1 * refreshCoordinator.startRefresh("t-a", "FRESH_INSTALL") >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("t-b", "FRESH_INSTALL") >> CompletableFuture.completedFuture(null)
    }

    def "a failed bootstrap does NOT trigger any consumer refresh"() {
        given:
        orchestrator.bootstrap(_) >> DownloadRefreshResult.failure(BootstrapSource.CLOUD_SYNC, "cloud unreachable")

        when:
        def result = service.runBootstrapAndRefresh()

        then:
        0 * refreshCoordinator.startRefresh(_, _)
        !result.success
    }

    def "LOCAL refresh replays all topics WITHOUT any bootstrap/download"() {
        given:
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)

        when:
        def result = service.runRefresh(RefreshType.LOCAL)

        then: "no orchestrator bootstrap at all; just the local replay per topic"
        0 * orchestrator.bootstrap(_)
        1 * refreshCoordinator.startRefresh("t-a", _) >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("t-b", _) >> CompletableFuture.completedFuture(null)
        result.success
        result.source == null
    }

    def "BARE_METAL RESETs consumers best-effort then hands off to the System.exit hard reset"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)

        when:
        def result = service.runRefresh(RefreshType.BARE_METAL)

        then: "no bootstrap/download and no in-process server bounce — this is the exit path"
        0 * orchestrator.bootstrap(_)
        0 * networkServer.stopAccepting()
        1 * refreshCoordinator.startRefresh("t-a", "BARE_METAL") >> CompletableFuture.completedFuture(null)
        1 * bareMetalReset.reset()
        result.success
        result.source == null
    }

    def "STREAM/CLOUD refresh uses the CONFIGURED topics when storage is still empty (async pipe load)"() {
        given: "no manifest (STREAM/CLOUD); storage empty mid-load; topics known from config"
        storage.getTopicNames() >> ([] as Set)
        legacyClientConfig.getServiceTopics() >> ["price-quote": ["prices-v1", "reference-data-v5"]]
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.PIPE_AND_PROVIDER_STREAM, null)

        when:
        service.runBootstrapAndRefresh()

        then: "RESET→READY is triggered for the configured topics, not skipped on empty storage"
        1 * refreshCoordinator.startRefresh("prices-v1", _) >> CompletableFuture.completedFuture(null)
        1 * refreshCoordinator.startRefresh("reference-data-v5", _) >> CompletableFuture.completedFuture(null)
    }

    def "fresh-install bootstrap does NOT stop the network server (consumers stay connected)"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)
        orchestrator.bootstrap(null) >> DownloadRefreshResult.ok(BootstrapSource.PIPE_AND_PROVIDER_STREAM, null)

        when:
        service.runBootstrapAndRefresh()

        then: "empty node — nothing to wipe, so the transport is never bounced"
        0 * networkServer.stopAccepting()
        0 * networkServer.resumeAccepting()
        1 * refreshCoordinator.startRefresh("t-a", _) >> CompletableFuture.completedFuture(null)
    }

    def "a download refresh bounces the consumer network server around the wipe+re-source window"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)

        when:
        service.runRefresh(RefreshType.CLOUD_SYNC)

        then: "server stopped BEFORE bootstrap, resumed AFTER it, then consumers refreshed"
        1 * networkServer.stopAccepting()

        then:
        1 * orchestrator.bootstrap(BootstrapSource.CLOUD_SYNC) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD_SYNC, null)

        then:
        1 * networkServer.resumeAccepting()

        then: "operator-triggered refresh keeps the actual source label (CLOUD_SYNC), not FRESH_INSTALL"
        1 * refreshCoordinator.startRefresh("t-a", "CLOUD_SYNC") >> CompletableFuture.completedFuture(null)
    }

    def "the network server is resumed even when the bootstrap fails"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)
        orchestrator.bootstrap(_) >> DownloadRefreshResult.failure(BootstrapSource.CLOUD_SYNC, "cloud unreachable")

        when:
        def result = service.runRefresh(RefreshType.PIPE_AND_PROVIDER_REFRESH)

        then: "transport comes back up, and no consumer refresh runs on a failed bootstrap"
        1 * networkServer.stopAccepting()
        1 * networkServer.resumeAccepting()
        0 * refreshCoordinator.startRefresh(_, _)
        !result.success
    }

    def "a forced source type is passed through to the orchestrator"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)

        when:
        service.runRefresh(RefreshType.CLOUD_SYNC)

        then: "orchestrator is asked to force CLOUD"
        1 * orchestrator.bootstrap(BootstrapSource.CLOUD_SYNC) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD_SYNC, null)
        1 * refreshCoordinator.startRefresh("t-a", _) >> CompletableFuture.completedFuture(null)
    }

    def "DOWNLOAD (auto) passes a null forced source to the orchestrator"() {
        given:
        storage.getTopicNames() >> (["t-a"] as Set)

        when:
        service.runRefresh(RefreshType.PIPE_AND_PROVIDER_REFRESH)

        then:
        1 * orchestrator.bootstrap(null) >> DownloadRefreshResult.ok(BootstrapSource.PIPE_AND_PROVIDER_STREAM, null)
        1 * refreshCoordinator.startRefresh("t-a", _) >> CompletableFuture.completedFuture(null)
    }

    def "a failing startRefresh for one topic does not abort the others"() {
        given:
        orchestrator.bootstrap(_) >> DownloadRefreshResult.ok(BootstrapSource.CLOUD_SYNC, null)
        storage.getTopicNames() >> (["t-a", "t-b"] as Set)
        refreshCoordinator.startRefresh("t-a", _) >> { throw new RuntimeException("boom") }

        when:
        service.runBootstrapAndRefresh()

        then:
        noExceptionThrown()
        1 * refreshCoordinator.startRefresh("t-b", _) >> CompletableFuture.completedFuture(null)
    }
}
