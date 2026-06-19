package com.messaging.broker.snapshot

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class LocalStateCleanerSpec extends Specification {

    @TempDir
    Path tempDir

    LocalStateCleaner cleaner = new LocalStateCleaner()

    private void write(String relative, String content) {
        Path p = tempDir.resolve(relative)
        Files.createDirectories(p.parent)
        Files.writeString(p, content)
    }

    def "clearState removes ack-store and parent state files, keeps topology and topic data"() {
        given:
        write("ack-store/CURRENT", "rocks")
        write("consumer-offsets.properties", "c")
        write("pipe-offset.properties", "p")
        write("data-refresh-state.properties", "r")
        write("delivery-state.properties", "d")
        write("topology.properties", "t")
        write("prices-v1/segment_metadata.db", "data")

        when:
        cleaner.clearState(tempDir.toString())

        then: "parent-specific state gone"
        !Files.exists(tempDir.resolve("ack-store"))
        !Files.exists(tempDir.resolve("consumer-offsets.properties"))
        !Files.exists(tempDir.resolve("pipe-offset.properties"))
        !Files.exists(tempDir.resolve("data-refresh-state.properties"))
        !Files.exists(tempDir.resolve("delivery-state.properties"))

        and: "this node's own identity and topic data preserved"
        Files.exists(tempDir.resolve("topology.properties"))
        Files.exists(tempDir.resolve("prices-v1/segment_metadata.db"))
    }

    def "clearState is idempotent when nothing exists"() {
        when:
        cleaner.clearState(tempDir.toString())

        then:
        noExceptionThrown()
    }

    def "retainTopics removes topics not in the keep set, preserving kept topics + infra dirs"() {
        given:
        write("prices-v1/segment_metadata.db", "x")
        write("stale-topic/segment_metadata.db", "y")
        write("ack-store/CURRENT", "rocks")
        write("snapshots/latest.zip", "zip")

        when: "keep only prices-v1 (e.g. the snapshot manifest's topics)"
        cleaner.retainTopics(tempDir.toString(), ["prices-v1"] as Set)

        then:
        Files.exists(tempDir.resolve("prices-v1"))
        !Files.exists(tempDir.resolve("stale-topic"))
        Files.exists(tempDir.resolve("ack-store"))
        Files.exists(tempDir.resolve("snapshots"))
    }

    def "clearTopicData wipes topic folders but preserves ack-store and snapshots"() {
        given:
        write("prices-v1/segment_metadata.db", "d1")
        write("reference-data-v5/partition-0/0.log", "d2")
        write("ack-store/CURRENT", "rocks")
        write("snapshots/latest.zip", "zip")

        when:
        cleaner.clearTopicData(tempDir.toString())

        then: "topic folders gone"
        !Files.exists(tempDir.resolve("prices-v1"))
        !Files.exists(tempDir.resolve("reference-data-v5"))

        and: "infra dirs preserved"
        Files.exists(tempDir.resolve("ack-store/CURRENT"))
        Files.exists(tempDir.resolve("snapshots/latest.zip"))
    }
}
