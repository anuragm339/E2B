package com.messaging.broker.consumer

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

class RefreshStateStoreSpec extends Specification {

    @TempDir
    Path tempDir

    def "save and load refresh context for multiple consumers"() {
        given: "a refresh context with state and acks"
        def store = new RefreshStateStore(tempDir.toString())
        def consumers = ["groupA:topic", "groupB:topic"] as Set
        def context = new RefreshContext("topic", consumers)
        context.setState(RefreshState.REPLAYING)
        context.setResetSentTime(Instant.parse("2025-01-01T00:00:00Z"))
        context.setReadySentTime(Instant.parse("2025-01-01T00:10:00Z"))
        context.setRefreshId("refresh-1")
        context.recordResetAck("groupA:topic")
        context.recordReadyAck("groupA:topic")
        context.updateConsumerOffset("groupA:topic", 123L)
        context.markConsumerReplaying("groupA:topic")

        when: "saving and loading"
        store.saveState(context)
        def reloaded = new RefreshStateStore(tempDir.toString())
        def contexts = reloaded.loadAllRefreshes()

        then: "state is restored"
        contexts.containsKey("topic")
        def loaded = contexts.get("topic")
        loaded.getState() == RefreshState.REPLAYING
        loaded.getExpectedConsumers() == consumers
        loaded.getReceivedResetAcks().contains("groupA:topic")
        loaded.getReceivedReadyAcks().contains("groupA:topic")
        loaded.getConsumerOffsets().get("groupA:topic") == 123L
        loaded.getRefreshId() == "refresh-1"
        loaded.getResetSentTime() != null
        loaded.getReadySentTime() != null
    }

    def "clearState removes topic entry"() {
        given:
        def store = new RefreshStateStore(tempDir.toString())
        def consumers = ["groupA:topic"] as Set
        def context = new RefreshContext("topic", consumers)
        context.setState(RefreshState.REPLAYING)
        store.saveState(context)

        when:
        store.clearState("topic")
        def reloaded = new RefreshStateStore(tempDir.toString())
        def contexts = reloaded.loadAllRefreshes()

        then:
        contexts.isEmpty()
    }
}
