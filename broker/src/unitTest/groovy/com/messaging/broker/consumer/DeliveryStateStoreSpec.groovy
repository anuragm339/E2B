package com.messaging.broker.consumer

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

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
}
