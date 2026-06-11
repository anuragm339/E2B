package com.messaging.broker.consumer

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class DeliveryStateStoreSpec extends Specification {

    @TempDir
    Path tempDir

    def "delivery state persists across reload"() {
        given:
        def store = new DeliveryStateStore(tempDir.toString())
        store.saveState("client1:topicA", 10L, System.currentTimeMillis() + 1000)
        store.flush()

        when:
        def reloaded = new DeliveryStateStore(tempDir.toString())
        def state = reloaded.getState("client1:topicA")

        then:
        state.lastAckedOffset == 10L
        state.inFlightUntil > 0

        cleanup:
        store.shutdown()
        reloaded.shutdown()
    }

    def "removeConsumerState clears all keys for client"() {
        given:
        def store = new DeliveryStateStore(tempDir.toString())
        store.saveState("client1:topicA", 1L, 0L)
        store.saveState("client1:topicB", 2L, 0L)
        store.saveState("client2:topicA", 3L, 0L)
        store.flush()

        when:
        store.removeConsumerState("client1")
        store.flush()
        def reloaded = new DeliveryStateStore(tempDir.toString())

        then:
        reloaded.getState("client1:topicA").lastAckedOffset == 0L
        reloaded.getState("client1:topicB").lastAckedOffset == 0L
        reloaded.getState("client2:topicA").lastAckedOffset == 3L

        cleanup:
        store.shutdown()
        reloaded.shutdown()
    }

    def "concurrent partial updates preserve both delivery-state fields"() {
        given:
        def store = new DeliveryStateStore(tempDir.toString())
        def executor = Executors.newFixedThreadPool(2)

        when:
        100.times { iteration ->
            store.saveState("group:topic", 0L, 0L)
            def start = new CountDownLatch(1)
            def offsetUpdate = executor.submit {
                start.await()
                store.updateAckedOffset("group:topic", iteration + 1L)
            }
            def inFlightUpdate = executor.submit {
                start.await()
                store.updateInFlightUntil("group:topic", 10_000L + iteration)
            }
            start.countDown()
            offsetUpdate.get(2, TimeUnit.SECONDS)
            inFlightUpdate.get(2, TimeUnit.SECONDS)

            def state = store.getState("group:topic")
            assert state.lastAckedOffset == iteration + 1L
            assert state.inFlightUntil == 10_000L + iteration
        }

        then:
        noExceptionThrown()

        cleanup:
        executor.shutdownNow()
        store.shutdown()
    }
}
