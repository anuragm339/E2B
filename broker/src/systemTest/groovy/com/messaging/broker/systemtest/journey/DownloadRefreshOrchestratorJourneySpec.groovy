package com.messaging.broker.systemtest.journey

import com.messaging.broker.snapshot.BootstrapSource
import com.messaging.broker.snapshot.DownloadRefreshOrchestrator
import com.messaging.broker.snapshot.SnapshotScheduler
import com.messaging.broker.systemtest.support.TwoBrokerJourneySupport
import com.messaging.common.api.StorageEngine

/**
 * Full orchestrator end-to-end across two brokers: the child's {@link DownloadRefreshOrchestrator}
 * (topology pointed at the parent) auto-selects a source, clears local state, and re-sources the
 * parent's data — no mocks, real HTTP, separate storages.
 */
class DownloadRefreshOrchestratorJourneySpec extends TwoBrokerJourneySupport {

    def "orchestrator auto-selects INCREMENTAL (no snapshot) and re-sources the parent's data"() {
        given: "the parent has data and NO snapshot; the child's topology points at the parent"
        def parentStorage = parentBean(StorageEngine)
        append(parentStorage, 'prices-v1', [10000L, 10002L])
        append(parentStorage, 'reference-data-v5', [20000L])
        pointChildAtParent(parentUrl())

        when: "the child runs the full orchestrator bootstrap"
        def result = childBean(DownloadRefreshOrchestrator).bootstrap()

        then: "it chose the incremental path and succeeded"
        result.success
        result.source == BootstrapSource.PIPE_AND_PROVIDER_STREAM

        and: "the parent's data was re-sourced into the child's storage"
        def childStorage = childBean(StorageEngine)
        offsetsOf(childStorage, 'prices-v1') == [10000L, 10002L]
        offsetsOf(childStorage, 'reference-data-v5') == [20000L]
    }

    def "orchestrator auto-selects SNAPSHOT once the parent has published one"() {
        given: "the parent builds + publishes a snapshot of its data"
        // declared after the INCREMENTAL test so that test ran while no snapshot existed.
        def manifest = parentBean(SnapshotScheduler).buildNow()
        assert manifest != null
        pointChildAtParent(parentUrl())

        when: "the child runs the orchestrator bootstrap"
        def result = childBean(DownloadRefreshOrchestrator).bootstrap()

        then: "it chose the snapshot fast path and succeeded"
        result.success
        result.source == BootstrapSource.PIPE_AND_PROVIDER_FILE_DOWNLOAD
    }
}
