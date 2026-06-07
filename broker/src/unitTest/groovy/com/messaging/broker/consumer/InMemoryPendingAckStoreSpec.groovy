package com.messaging.broker.consumer

import com.messaging.broker.legacy.MergedBatch
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

class InMemoryPendingAckStoreSpec extends Specification {

    def "stale timeout generation cannot claim a newer legacy batch"() {
        given:
        def store = new InMemoryPendingAckStore()
        def firstBatch = new MergedBatch()
        def secondBatch = new MergedBatch()
        def firstTimer = Mock(Timer.Sample)
        def secondTimer = Mock(Timer.Sample)

        when:
        long firstGeneration =
                store.reservePendingBatch("client-1", firstBatch, firstTimer, 1000L)
        def firstClaim = store.claimPendingDelivery("client-1", firstGeneration)
        long secondGeneration =
                store.reservePendingBatch("client-1", secondBatch, secondTimer, 1000L)

        then:
        firstClaim.batch().is(firstBatch)
        secondGeneration > firstGeneration
        store.claimPendingDelivery("client-1", firstGeneration) == null
        store.getPendingBatch("client-1").is(secondBatch)

        when:
        def secondClaim = store.claimPendingDelivery("client-1", secondGeneration)

        then:
        secondClaim.batch().is(secondBatch)
        secondClaim.timer().is(secondTimer)
        secondClaim.sendTime() == 1000L
        store.getPendingBatch("client-1") == null
    }
}
