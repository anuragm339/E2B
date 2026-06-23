package com.messaging.broker.systemtest.journey

import com.messaging.broker.snapshot.BootstrapSource
import com.messaging.broker.snapshot.DownloadRefreshOrchestrator
import com.messaging.broker.snapshot.SnapshotScheduler
import com.messaging.broker.systemtest.support.TwoBrokerJourneySupport
import com.messaging.common.api.PipeConnector
import com.messaging.common.api.StorageEngine

/**
 * Full orchestrator end-to-end across two brokers: the child's {@link DownloadRefreshOrchestrator}
 * (topology pointed at the parent) auto-selects a source and preps the node. Pipe-only: for the
 * STREAM path the orchestrator wipes + resets the pipe cursor and the NORMAL pipe re-streams the
 * data (no synchronous bulk pull). The snapshot path restores segments synchronously.
 */
class DownloadRefreshOrchestratorJourneySpec extends TwoBrokerJourneySupport {

    def "orchestrator auto-selects INCREMENTAL (no snapshot), wipes, and resets the pipe to re-stream"() {
        given: "the parent has data and NO snapshot; the child's topology points at the parent"
        def parentStorage = parentBean(StorageEngine)
        append(parentStorage, 'prices-v1', [10000L, 10002L])
        pointChildAtParent(parentUrl())
        // Pre-seed the child's pipe cursor non-zero to prove the orchestrator resets it back to 0.
        childBean(PipeConnector).resetOffset(999L)

        when: "the child runs the full orchestrator bootstrap"
        def result = childBean(DownloadRefreshOrchestrator).bootstrap()

        then: "it chose the incremental path and succeeded"
        result.success
        result.source == BootstrapSource.PIPE_AND_PROVIDER_STREAM

        and: "pipe-only: the cursor is reset to 0 so the normal pipe re-streams the parent's history"
        childBean(PipeConnector).getCurrentOffset() == 0L
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
