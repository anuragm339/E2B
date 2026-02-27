package com.messaging.broker.consumer

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class ConsumerOffsetTrackerSpec extends Specification {

    @TempDir
    Path tempDir

    def "offsets persist across shutdown and reload"() {
        given: "a tracker with persisted offsets"
        def tracker = new ConsumerOffsetTracker(tempDir.toString())
        tracker.init()
        tracker.updateOffset("groupA:topicA", 5L)
        tracker.updateOffset("groupB:topicB", 12L)
        tracker.shutdown()

        when: "a new tracker loads offsets from disk"
        def reloaded = new ConsumerOffsetTracker(tempDir.toString())
        reloaded.init()

        then: "offsets are preserved"
        reloaded.getOffset("groupA:topicA") == 5L
        reloaded.getOffset("groupB:topicB") == 12L

        cleanup:
        reloaded.shutdown()
    }

    def "resetOffset flushes updated value"() {
        given:
        def tracker = new ConsumerOffsetTracker(tempDir.toString())
        tracker.init()
        tracker.updateOffset("groupA:topicA", 5L)
        tracker.resetOffset("groupA:topicA", 9L)
        tracker.shutdown()

        when:
        def reloaded = new ConsumerOffsetTracker(tempDir.toString())
        reloaded.init()

        then:
        reloaded.getOffset("groupA:topicA") == 9L

        cleanup:
        reloaded.shutdown()
    }
}
