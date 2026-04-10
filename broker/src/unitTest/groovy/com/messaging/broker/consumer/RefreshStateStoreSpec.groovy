package com.messaging.broker.consumer

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
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

    def "loadState supports deprecated old single topic format"() {
        given:
        def store = new RefreshStateStore(tempDir.toString())
        def stateFile = tempDir.resolve("data-refresh-state.properties")
        Files.writeString(stateFile, """\
active.refresh.topic=topic
active.refresh.state=READY_SENT
active.refresh.expected.consumers=groupA:topic,groupB:topic
consumer.groupA\\:topic.reset.ack.received=true
consumer.groupA\\:topic.ready.ack.received=true
consumer.groupA\\:topic.current.offset=77
""".stripIndent())

        when:
        def loaded = store.loadState()

        then:
        loaded != null
        loaded.topic == "topic"
        loaded.state == RefreshState.READY_SENT
        loaded.receivedResetAcks == ["groupA:topic"] as Set
        loaded.receivedReadyAcks == ["groupA:topic"] as Set
        loaded.consumerOffsets["groupA:topic"] == 77L
    }

    def "clearState preserves other active topics and clearState without topic removes file"() {
        given:
        def store = new RefreshStateStore(tempDir.toString())
        def first = new RefreshContext("topic-a", ["groupA:topic-a"] as Set)
        first.setState(RefreshState.REPLAYING)
        first.setRefreshId("refresh-a")
        first.setLastShutdownTime(Instant.parse("2025-01-01T00:00:00Z"))
        def second = new RefreshContext("topic-b", ["groupB:topic-b"] as Set)
        second.setState(RefreshState.READY_SENT)
        second.setRefreshId("refresh-b")
        store.saveState(first)
        store.saveState(second)

        when:
        store.clearState("topic-a")
        def remaining = store.loadAllRefreshes()

        then:
        remaining.keySet() == ["topic-b"] as Set
        remaining["topic-b"].refreshId == "refresh-b"

        when:
        store.clearState()

        then:
        store.loadAllRefreshes().isEmpty()
    }
}
