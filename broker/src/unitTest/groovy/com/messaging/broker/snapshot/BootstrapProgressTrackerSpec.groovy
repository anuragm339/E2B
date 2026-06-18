package com.messaging.broker.snapshot

import spock.lang.Specification

class BootstrapProgressTrackerSpec extends Specification {

    BootstrapProgressTracker tracker = new BootstrapProgressTracker()

    def "percent is -1 (unknown) until a denominator is set"() {
        when:
        tracker.start("SNAPSHOT", BootstrapProgressTracker.Phase.DOWNLOADING)
        tracker.addNumerator(500)

        then:
        tracker.percent() == -1.0
        tracker.snapshot().percent == null
    }

    def "exact byte percent for the snapshot path"() {
        given:
        tracker.start("SNAPSHOT", BootstrapProgressTracker.Phase.DOWNLOADING)
        tracker.setDenominator(1000)

        when:
        tracker.addNumerator(250)

        then:
        tracker.percent() == 25.0
        tracker.snapshot().percent == 25.0
        tracker.snapshot().transferred == 250
        tracker.snapshot().total == 1000
    }

    def "percent is capped at 100 even if transferred overshoots"() {
        given:
        tracker.start("INCREMENTAL_PARENT", BootstrapProgressTracker.Phase.INGESTING)
        tracker.setDenominator(100)

        when:
        tracker.setNumerator(150)

        then:
        tracker.percent() == 100.0
    }

    def "start resets counters and records the source/phase"() {
        given:
        tracker.start("SNAPSHOT", BootstrapProgressTracker.Phase.DOWNLOADING)
        tracker.setDenominator(10)
        tracker.addNumerator(5)

        when:
        tracker.start("CLOUD", BootstrapProgressTracker.Phase.INGESTING)

        then:
        tracker.snapshot().source == "CLOUD"
        tracker.snapshot().phase == "INGESTING"
        tracker.snapshot().transferred == 0
        tracker.percent() == -1.0
    }

    def "done and failed set terminal phases"() {
        when:
        tracker.start("SNAPSHOT", BootstrapProgressTracker.Phase.DOWNLOADING)
        tracker.done()

        then:
        tracker.phase == BootstrapProgressTracker.Phase.DONE

        when:
        tracker.failed()

        then:
        tracker.phase == BootstrapProgressTracker.Phase.FAILED
    }
}
