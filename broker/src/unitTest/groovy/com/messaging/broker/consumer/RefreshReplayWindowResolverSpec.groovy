package com.messaging.broker.consumer

import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant

/**
 * Resolver computes two things:
 *  - startOffset  : how far back to re-deliver (replay.window-hours; 0 = from earliest)
 *  - targetOffset : the READY gate — the last record OLDER than the ready-settle window (the
 *                   "settled" history); records inside the window are not required for READY.
 */
class RefreshReplayWindowResolverSpec extends Specification {

    static final long SIX_HOURS_MS = 6L * 3600 * 1000

    StorageEngine storage = Mock()

    // ── target (ready-settle window) ─────────────────────────────────────────

    def "settle window disabled (0) requires full catch-up to head"() {
        given:
        def resolver = new RefreshReplayWindowResolver(storage, 0L, 0L)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.getEarliestOffset("prices-v1", 0) >> 3L

        when:
        def window = resolver.resolve("prices-v1")

        then: "target is head; no created_time scan needed"
        window.startOffset() == 3L
        window.targetOffset() == 10L
        window.cutoff() == null
        0 * storage.read(_, _, _, _)
    }

    def "topic X/Y: target is the last record OLDER than the settle window (current-time record excluded)"() {
        given: "offsets 3..10; only offset 10 (MSG5) is within the last 6h, the rest are settled"
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.getEarliestOffset("prices-v1", 0) >> 3L
        storage.read("prices-v1", 0, 3L, 500) >> [
                record(3L, Instant.parse("2000-02-20T00:00:00Z")),
                record(9L, Instant.now().minusSeconds(48 * 3600)),  // yesterday-ish, settled
                record(10L, Instant.now())                           // current time, within window
        ]

        when:
        def window = resolver.resolve("prices-v1")

        then: "deliver MSG..9 → READY; the current-time MSG at 10 is NOT required"
        window.startOffset() == 3L
        window.targetOffset() == 9L
        window.cutoff() != null
    }

    def "topic Z: no record within the window → target is head (deliver everything)"() {
        given: "every record is older than the window"
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.getEarliestOffset("prices-v1", 0) >> 3L
        storage.read("prices-v1", 0, 3L, 500) >> [
                record(3L, Instant.now().minusSeconds(48 * 3600)),
                record(10L, Instant.now().minusSeconds(24 * 3600))
        ]

        when:
        def window = resolver.resolve("prices-v1")

        then:
        window.targetOffset() == 10L
    }

    def "fast topic: every record is within the window → target -1 (nothing required, READY immediately)"() {
        given:
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.getEarliestOffset("prices-v1", 0) >> 3L
        storage.read("prices-v1", 0, 3L, 500) >> [
                record(3L, Instant.now().minusSeconds(60)),
                record(10L, Instant.now())
        ]

        when:
        def window = resolver.resolve("prices-v1")

        then:
        window.targetOffset() == -1L
    }

    def "LOCAL refresh ignores the settle window and targets the head (full catch-up)"() {
        given: "all-recent data that would resolve to target -1 for a non-LOCAL refresh"
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.getEarliestOffset("prices-v1", 0) >> 3L

        when:
        def window = resolver.resolve("prices-v1", "LOCAL")

        then: "LOCAL re-pushes local segments → consumers must catch up to head; no created_time scan"
        window.startOffset() == 3L
        window.targetOffset() == 10L
        0 * storage.read(_, _, _, _)
    }

    def "empty topic resolves to nothing to replay"() {
        given:
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> -1L

        when:
        def window = resolver.resolve("prices-v1")

        then:
        window.startOffset() == 0L
        window.targetOffset() == -1L
        0 * storage.read(_, _, _, _)
    }

    // ── start (replay.window-hours) ──────────────────────────────────────────

    def "positive replay window starts at the first record inside the replay cutoff"() {
        given: "settle disabled so target is head; replay window trims the start"
        def resolver = new RefreshReplayWindowResolver(storage, 24L, 0L)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.getEarliestOffset("prices-v1", 0) >> 4L
        storage.read("prices-v1", 0, 4L, 500) >> [
                record(4L, Instant.now().minusSeconds(48 * 3600)),
                record(5L, Instant.now().minusSeconds(1))
        ]

        when:
        def window = resolver.resolve("prices-v1")

        then:
        window.startOffset() == 5L
        window.targetOffset() == 10L
    }

    // ── settledTarget (dynamic re-evaluation against the live head) ──────────

    def "settledTarget returns the head cheaply when the head record is already settled (all-old)"() {
        given: "head record is 2 days old → everything is settled; no scan from earliest needed"
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.read("prices-v1", 0, 10L, 1) >> [record(10L, Instant.now().minusSeconds(48 * 3600))]

        when:
        def target = resolver.settledTarget("prices-v1")

        then:
        target == 10L
        0 * storage.getEarliestOffset(_, _)   // cheap path — no boundary scan
    }

    def "settledTarget excludes the recent tail when the head record is within the window"() {
        given: "head (10) is current; the settled boundary is at 9"
        def resolver = new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS)
        storage.getCurrentOffset("prices-v1", 0) >> 10L
        storage.read("prices-v1", 0, 10L, 1) >> [record(10L, Instant.now())]   // head is recent → scan
        storage.getEarliestOffset("prices-v1", 0) >> 3L
        storage.read("prices-v1", 0, 3L, 500) >> [
                record(9L, Instant.now().minusSeconds(48 * 3600)),
                record(10L, Instant.now())
        ]

        when:
        def target = resolver.settledTarget("prices-v1")

        then:
        target == 9L
    }

    def "settledTarget is -1 for an empty topic"() {
        given:
        storage.getCurrentOffset("empty", 0) >> -1L

        expect:
        new RefreshReplayWindowResolver(storage, 0L, SIX_HOURS_MS).settledTarget("empty") == -1L
    }

    def "settledTarget returns the head when the settle window is disabled"() {
        given:
        storage.getCurrentOffset("prices-v1", 0) >> 20L

        expect:
        new RefreshReplayWindowResolver(storage, 0L, 0L).settledTarget("prices-v1") == 20L
    }

    private static MessageRecord record(long offset, Instant createdAt) {
        new MessageRecord(offset, "prices-v1", 0, "key-${offset}", EventType.MESSAGE, "{}", createdAt)
    }
}
