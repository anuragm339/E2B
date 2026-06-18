package com.messaging.broker.systemtest.journey

import com.messaging.broker.compaction.CompactionScheduler
import com.messaging.broker.consistency.PipeConsistencyScheduler
import com.messaging.broker.http.DownloadRefreshController
import com.messaging.broker.snapshot.BootstrapSourceClient
import com.messaging.broker.snapshot.DownloadRefreshOrchestrator
import com.messaging.broker.snapshot.DownloadRefreshService
import com.messaging.broker.snapshot.HttpBootstrapSourceClient
import com.messaging.broker.snapshot.LocalStateCleaner
import com.messaging.broker.snapshot.SnapshotBuilder
import com.messaging.broker.snapshot.SnapshotController
import com.messaging.broker.snapshot.SnapshotRestorer
import com.messaging.broker.snapshot.SnapshotScheduler
import com.messaging.broker.snapshot.SnapshotStore
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport

/**
 * Smoke test: boot the full broker ApplicationContext and confirm the entire download-refresh
 * bean graph resolves — catches DI cycles, missing beans, and bad @Value defaults that unit tests
 * (which construct beans directly) cannot. Forcing getBean(...) eagerly instantiates each lazy
 * @Singleton and its transitive dependencies.
 */
class DownloadRefreshWiringSystemSpec extends BrokerSystemTestSupport {

    def "the full download-refresh bean graph resolves in a booted broker context"() {
        expect: "every new download-refresh bean is wired"
        brokerCtx.getBean(DownloadRefreshController) != null
        brokerCtx.getBean(DownloadRefreshService) != null
        brokerCtx.getBean(DownloadRefreshOrchestrator) != null
        brokerCtx.getBean(BootstrapSourceClient) instanceof HttpBootstrapSourceClient
        brokerCtx.getBean(LocalStateCleaner) != null
        brokerCtx.getBean(SnapshotBuilder) != null
        brokerCtx.getBean(SnapshotRestorer) != null
        brokerCtx.getBean(SnapshotStore) != null
        brokerCtx.getBean(SnapshotScheduler) != null
        brokerCtx.getBean(SnapshotController) != null
    }

    def "CompactionScheduler resolves in-context with its new RefreshCoordinator BeanProvider (no DI cycle)"() {
        expect: "CompactionScheduler (a @Scheduled @Singleton, created at boot) wires with the lazy provider"
        brokerCtx.getBean(CompactionScheduler) != null

        and: "PipeConsistencyScheduler is @Requires(pipe.consistency.enabled=true) — disabled in this"
        // journey context, so it is legitimately absent here; its BeanProvider wiring is covered by
        // PipeConsistencySchedulerSpec and mirrors CompactionScheduler's (validated above).
        !brokerCtx.containsBean(PipeConsistencyScheduler)
    }
}
