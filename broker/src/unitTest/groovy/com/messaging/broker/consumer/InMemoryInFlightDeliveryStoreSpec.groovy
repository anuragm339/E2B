package com.messaging.broker.consumer

import com.messaging.broker.model.DeliveryKey
import spock.lang.Specification

import java.util.concurrent.ScheduledFuture

class InMemoryInFlightDeliveryStoreSpec extends Specification {

    def "stale timeout generation cannot release or clear a newer delivery"() {
        given:
        def store = new InMemoryInFlightDeliveryStore()
        def key = DeliveryKey.of("group-a", "prices-v1")
        def inFlight = store.markInFlight(key)
        assert inFlight.compareAndSet(false, true)

        long firstGeneration = store.beginDelivery(key)
        store.setOriginalOffset(key, firstGeneration, 10L)
        store.setPendingOffset(key, 20L)
        store.setFromOffset(key, 10L)
        store.recordBatchSendTime(key, 1000L)
        store.recordTraceId(key, "trace-1")
        store.scheduleTimeout(key, firstGeneration, Mock(ScheduledFuture))

        when: "the timeout claims the first generation"
        def timeoutClaim = store.claimPendingDelivery(key, firstGeneration)

        then:
        timeoutClaim.pendingOffset() == 20L
        store.claimPendingDelivery(key) == null
        store.isInFlight(key)
        !inFlight.compareAndSet(false, true)

        when: "the timeout completes and a new generation starts"
        assert store.completeDelivery(key, firstGeneration)
        assert inFlight.compareAndSet(false, true)
        long secondGeneration = store.beginDelivery(key)
        store.setOriginalOffset(key, secondGeneration, 20L)
        store.setPendingOffset(key, 30L)

        then: "cleanup from the old timeout cannot touch the new generation"
        !store.completeDelivery(key, firstGeneration)
        store.isInFlight(key)
        store.getPendingOffset(key) == 30L
        secondGeneration > firstGeneration
    }

    def "per-key synchronization uses a fixed number of lock stripes"() {
        given:
        def store = new InMemoryInFlightDeliveryStore()

        when:
        10_000.times { index ->
            store.markInFlight(DeliveryKey.of("group-${index}", "topic-${index}"))
        }

        then:
        ((Object[]) getPrivateField(store, "stateLocks")).length == 64
    }

    private static Object getPrivateField(Object target, String fieldName) {
        def field = target.class.getDeclaredField(fieldName)
        field.setAccessible(true)
        field.get(target)
    }
}
