package com.messaging.storage.watermark

import spock.lang.Specification

class StorageWatermarkTrackerSpec extends Specification {

    def "watermark updates are monotonic"() {
        given: "a tracker"
        def tracker = new StorageWatermarkTracker()

        when: "updating with increasing and decreasing offsets"
        tracker.updateWatermark("topic", 0, 5)
        tracker.updateWatermark("topic", 0, 3)

        then: "watermark stays at the highest value"
        tracker.getWatermark("topic", 0) == 5
    }

    def "hasNewData reflects watermark vs last delivered"() {
        given:
        def tracker = new StorageWatermarkTracker()
        tracker.updateWatermark("topic", 0, 10)

        expect:
        tracker.hasNewData("topic", 0, 9)
        !tracker.hasNewData("topic", 0, 10)
    }

    def "separate topics and partitions are tracked independently"() {
        given:
        def tracker = new StorageWatermarkTracker()
        tracker.updateWatermark("topicA", 0, 5)
        tracker.updateWatermark("topicA", 1, 7)
        tracker.updateWatermark("topicB", 0, 3)

        expect:
        tracker.getWatermark("topicA", 0) == 5
        tracker.getWatermark("topicA", 1) == 7
        tracker.getWatermark("topicB", 0) == 3
    }
}
