package com.messaging.storage.watermark

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

/**
 * Integration tests for StorageWatermarkTracker.
 *
 * StorageWatermarkTracker is a @Singleton — injected directly from the Micronaut context.
 * Tests verify monotonic watermark updates, hasNewData logic, and multi-topic isolation.
 */
@MicronautTest
class StorageWatermarkTrackerIntegrationSpec extends Specification {

    @Inject StorageWatermarkTracker tracker

    // =========================================================================
    // updateWatermark / getWatermark
    // =========================================================================

    def "getWatermark returns 0 for an unknown topic"() {
        expect:
        tracker.getWatermark("unknown-topic", 0) == 0L
    }

    def "updateWatermark stores the offset and getWatermark returns it"() {
        when:
        tracker.updateWatermark("topic-a", 0, 42L)

        then:
        tracker.getWatermark("topic-a", 0) == 42L
    }

    def "updateWatermark is monotonic — lower value does not overwrite higher"() {
        when:
        tracker.updateWatermark("monotonic-topic", 0, 100L)
        tracker.updateWatermark("monotonic-topic", 0, 50L)   // lower — ignored

        then:
        tracker.getWatermark("monotonic-topic", 0) == 100L
    }

    def "updateWatermark advances when a higher value is written"() {
        given:
        tracker.updateWatermark("advance-topic", 0, 10L)

        when:
        tracker.updateWatermark("advance-topic", 0, 25L)

        then:
        tracker.getWatermark("advance-topic", 0) == 25L
    }

    // =========================================================================
    // hasNewData
    // =========================================================================

    def "hasNewData returns false when watermark equals lastDeliveredOffset"() {
        given:
        tracker.updateWatermark("hnd-topic", 0, 5L)

        expect:
        !tracker.hasNewData("hnd-topic", 0, 5L)
    }

    def "hasNewData returns true when watermark is ahead of lastDeliveredOffset"() {
        given:
        tracker.updateWatermark("hnd-ahead-topic", 0, 10L)

        expect:
        tracker.hasNewData("hnd-ahead-topic", 0, 7L)
    }

    def "hasNewData returns false for unknown topic (watermark is 0)"() {
        expect:
        !tracker.hasNewData("never-written-topic", 0, 0L)
    }

    // =========================================================================
    // Multi-topic / multi-partition isolation
    // =========================================================================

    def "different topics have independent watermarks"() {
        when:
        tracker.updateWatermark("iso-topic-x", 0, 99L)
        tracker.updateWatermark("iso-topic-y", 0, 1L)

        then:
        tracker.getWatermark("iso-topic-x", 0) == 99L
        tracker.getWatermark("iso-topic-y", 0) == 1L
    }

    def "different partitions of the same topic have independent watermarks"() {
        when:
        tracker.updateWatermark("part-topic", 0, 10L)
        tracker.updateWatermark("part-topic", 1, 20L)
        tracker.updateWatermark("part-topic", 2, 30L)

        then:
        tracker.getWatermark("part-topic", 0) == 10L
        tracker.getWatermark("part-topic", 1) == 20L
        tracker.getWatermark("part-topic", 2) == 30L
    }

    def "getWatermarkCount increases as new topics are tracked"() {
        given:
        def initialCount = tracker.getWatermarkCount()

        when:
        tracker.updateWatermark("count-topic-1", 0, 1L)
        tracker.updateWatermark("count-topic-2", 0, 2L)

        then:
        tracker.getWatermarkCount() >= initialCount + 2
    }
}
