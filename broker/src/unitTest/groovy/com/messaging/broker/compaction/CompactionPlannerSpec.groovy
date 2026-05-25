package com.messaging.broker.compaction

import com.messaging.storage.segment.Segment
import spock.lang.Specification

class CompactionPlannerSpec extends Specification {

    CompactionPlanner planner = new CompactionPlanner()

    def "returns empty list when no sealed segments"() {
        expect:
        planner.selectDirtyWindow([], -1L, 10).isEmpty()
    }

    def "returns all segments when no checkpoint has been set (first run)"() {
        given:
        def segments = [seg(0L), seg(100L), seg(200L)]

        when:
        def window = planner.selectDirtyWindow(segments, -1L, 10)

        then:
        window*.baseOffset == [0L, 100L, 200L]
    }

    def "skips segments at or before the checkpoint"() {
        given:
        def segments = [seg(0L), seg(100L), seg(200L), seg(300L)]

        when:
        def window = planner.selectDirtyWindow(segments, 100L, 10)

        then:
        window*.baseOffset == [200L, 300L]
    }

    def "respects window size cap"() {
        given:
        def segments = [seg(0L), seg(100L), seg(200L), seg(300L), seg(400L)]

        when:
        def window = planner.selectDirtyWindow(segments, -1L, 3)

        then:
        window*.baseOffset == [0L, 100L, 200L]
    }

    def "returns segments sorted ascending by base offset regardless of input order"() {
        given:
        def segments = [seg(300L), seg(100L), seg(200L), seg(0L)]

        when:
        def window = planner.selectDirtyWindow(segments, -1L, 10)

        then:
        window*.baseOffset == [0L, 100L, 200L, 300L]
    }

    def "returns empty when all segments are already at or below checkpoint"() {
        given:
        def segments = [seg(0L), seg(100L)]

        expect:
        planner.selectDirtyWindow(segments, 100L, 10).isEmpty()
    }

    def "window size of 1 returns only the lowest dirty segment"() {
        given:
        def segments = [seg(0L), seg(100L), seg(200L)]

        when:
        def window = planner.selectDirtyWindow(segments, -1L, 1)

        then:
        window*.baseOffset == [0L]
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private Segment seg(long baseOffset) {
        def s = Mock(Segment)
        s.getBaseOffset() >> baseOffset
        return s
    }
}
