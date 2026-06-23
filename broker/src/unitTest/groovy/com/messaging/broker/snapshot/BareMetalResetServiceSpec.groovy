package com.messaging.broker.snapshot

import io.micronaut.context.ApplicationContext
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class BareMetalResetServiceSpec extends Specification {

    @TempDir
    Path tempDir

    def "wipeContents deletes everything inside the dir but keeps the dir itself"() {
        given: "a data dir resembling a real one"
        Files.createDirectories(tempDir.resolve("prices-v1/partition-0"))
        Files.writeString(tempDir.resolve("prices-v1/segment_metadata.db"), "x")
        Files.writeString(tempDir.resolve("prices-v1/partition-0/0.log"), "y")
        Files.createDirectories(tempDir.resolve("ack-store"))
        Files.writeString(tempDir.resolve("ack-store/CURRENT"), "rocks")
        Files.writeString(tempDir.resolve("consumer-offsets.properties"), "c")
        Files.writeString(tempDir.resolve("events.db"), "big")
        Files.writeString(tempDir.resolve("broker.log"), "log")

        when:
        BareMetalResetService.wipeContents(tempDir)

        then: "the dir survives but is empty"
        Files.isDirectory(tempDir)
        try (var s = Files.list(tempDir)) {
            s.count() == 0
        }
    }

    def "wipeContents is a no-op on a missing dir"() {
        when:
        BareMetalResetService.wipeContents(tempDir.resolve("does-not-exist"))

        then:
        noExceptionThrown()
    }

    def "reset() runs stop -> wipe -> exit in order (seams overridden, no real exit)"() {
        given: "data present + a service with the stop/exit seams stubbed"
        Files.writeString(tempDir.resolve("a.txt"), "x")
        def events = []
        def service = new BareMetalResetService(Mock(ApplicationContext), tempDir.toString(), 70, 0L) {
            @Override protected void stopEverything() { events << "stop" }
            @Override protected void exit(int code) { events << "exit:${code}".toString() }
        }

        when:
        service.reset()
        // reset() runs on a background thread; wait briefly for it to finish
        def deadline = System.currentTimeMillis() + 3000
        while (events.size() < 2 && System.currentTimeMillis() < deadline) { Thread.sleep(20) }

        then: "stopped, wiped, then exited"
        events == ["stop", "exit:70"]
        try (var s = Files.list(tempDir)) {
            s.count() == 0
        }
    }
}
