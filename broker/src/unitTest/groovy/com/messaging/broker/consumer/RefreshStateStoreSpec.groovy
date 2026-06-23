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
        context.setReplayStartOffset(200L)
        context.setReplayTargetOffset(300L)
        context.setReplayCutoffTime(Instant.parse("2024-12-31T00:00:00Z"))
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
        loaded.getReplayStartOffset() == 200L
        loaded.getReplayTargetOffset() == 300L
        loaded.getReplayCutoffTime() == Instant.parse("2024-12-31T00:00:00Z")
        loaded.getResetSentTime() != null
        loaded.getReadySentTime() != null
    }

    def "refresh type round-trips so a recovered refresh keeps its real type (not LOCAL)"() {
        given: "a non-LOCAL refresh context (e.g. an admin stream refresh)"
        def store = new RefreshStateStore(tempDir.toString())
        def context = new RefreshContext("topic", ["groupA:topic"] as Set, "TOPIC", "PIPE_AND_PROVIDER_STREAM")
        context.setState(RefreshState.REPLAYING)
        context.setRefreshId("refresh-2")

        when:
        store.saveState(context)
        def reloaded = new RefreshStateStore(tempDir.toString()).loadAllRefreshes().get("topic")

        then: "the restored context carries the persisted type, not the default LOCAL"
        reloaded.getRefreshType() == "PIPE_AND_PROVIDER_STREAM"
    }

    def "a state file without a persisted type loads as LOCAL (backward-compatible)"() {
        given: "a state file written before the refresh.type field existed"
        def file = tempDir.resolve("data-refresh-state.properties")
        Files.writeString(file, [
                "active.refresh.topics=topic",
                "topic.topic.state=REPLAYING",
                "topic.topic.start.time=2025-01-01T00:00:00Z",
                "topic.topic.expected.consumers=groupA:topic",
                "topic.topic.refresh.id=refresh-old"
        ].join("\n") + "\n")

        when:
        def loaded = new RefreshStateStore(tempDir.toString()).loadAllRefreshes().get("topic")

        then: "missing type defaults to LOCAL"
        loaded != null
        loaded.getRefreshType() == "LOCAL"
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
