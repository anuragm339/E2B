package com.messaging.broker.consumer

import com.messaging.common.api.NetworkServer
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * Covers the READY-retry chain surviving a failed send (#12). The scheduler mock runs the
 * scheduled task immediately, so the recursive reschedule plays out synchronously and is bounded
 * by MAX_READY_RETRIES (3). With the bug (no reschedule on send failure) the chain would stop
 * after a single send.
 */
class ConsumerReadinessManagerSpec extends Specification {

    ReadyStateStore readyStateStore = Mock()
    NetworkServer server = Mock()
    ScheduledExecutorService scheduler = Mock()

    ConsumerReadinessManager manager = new ConsumerReadinessManager(readyStateStore, server, scheduler)

    def "a failed READY-retry send still reschedules, bounded by MAX_READY_RETRIES (#12)"() {
        given: "the consumer never becomes ready; scheduled tasks run immediately"
        readyStateStore.isLegacyConsumerReady("c1") >> false
        scheduler.schedule(_ as Runnable, _ as Long, _ as TimeUnit) >> { Runnable r, Long d, TimeUnit u ->
            r.run()
            Mock(ScheduledFuture)
        }

        when:
        manager.scheduleReadyRetry("c1", null, null, 0)

        then: "every send fails, yet the chain keeps retrying up to MAX_READY_RETRIES (3 sends), not 1"
        3 * server.send("c1", _) >> CompletableFuture.failedFuture(new RuntimeException("boom"))
    }
}
