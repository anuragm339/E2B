package com.messaging.broker.snapshot

import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.ZipFile

class SnapshotBuilderSpec extends Specification {

    @TempDir
    Path tempDir

    SnapshotBuilder builder = new SnapshotBuilder()
    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()

    private void write(String relative, String content) {
        Path p = tempDir.resolve(relative)
        Files.createDirectories(p.parent)
        Files.writeString(p, content)
    }

    def "snapshot includes topic data and the manifest, excludes ack-store, state files and logs"() {
        given: "a data dir mimicking the real layout"
        write("prices-v1/segment_metadata.db", "sqlite")
        write("prices-v1/partition-0/00000000000000000000.log", "logdata")
        write("prices-v1/partition-0/00000000000000000000.index", "idx")
        write("reference-data-v5/segment_metadata.db", "sqlite2")
        // excluded:
        write("ack-store/CURRENT", "rocks")
        write("ack-store/000001.sst", "sst")
        write("consumer-offsets.properties", "x=1")
        write("topology.properties", "y=2")
        write("pipe-offset.properties", "z=3")
        write("data-refresh-state.properties", "r=4")
        write("delivery-state.properties", "d=5")
        write("events.db", "huge")
        write("broker.log", "logs")
        write(".DS_Store", "junk")

        Path outZip = tempDir.resolve("snapshots/snap.zip")
        def heads = ["prices-v1": 41L, "reference-data-v5": -1L]

        when:
        def manifest = builder.build(tempDir, outZip, heads)

        then: "manifest captures the watermarks"
        manifest.topicHeads["prices-v1"] == 41L
        manifest.topicHeads["reference-data-v5"] == -1L
        Files.exists(outZip)

        and: "the ZIP contains exactly the topic data + manifest"
        def entries = zipEntryNames(outZip)
        entries.contains("manifest.json")
        entries.contains("prices-v1/segment_metadata.db")
        entries.contains("prices-v1/partition-0/00000000000000000000.log")
        entries.contains("prices-v1/partition-0/00000000000000000000.index")
        entries.contains("reference-data-v5/segment_metadata.db")

        and: "nothing excluded leaked in"
        entries.findAll { it.startsWith("ack-store/") }.isEmpty()
        !entries.contains("consumer-offsets.properties")
        !entries.contains("topology.properties")
        !entries.contains("pipe-offset.properties")
        !entries.contains("data-refresh-state.properties")
        !entries.contains("delivery-state.properties")
        !entries.contains("events.db")
        !entries.contains("broker.log")
        !entries.contains(".DS_Store")
    }

    def "manifest.json round-trips and carries the schema version + N* pipe offset"() {
        given:
        write("prices-v1/segment_metadata.db", "sqlite")
        Path outZip = tempDir.resolve("snap.zip")

        when:
        builder.build(tempDir, outZip, ["prices-v1": 7L], 4242L)
        def json = readEntry(outZip, "manifest.json")
        def parsed = mapper.readValue(json, SnapshotManifest)

        then:
        parsed.schemaVersion == SnapshotManifest.SCHEMA_VERSION
        parsed.createdAtMs > 0
        parsed.topicHeads["prices-v1"] == 7L
        parsed.pipeOffset == 4242L
    }

    def "a legacy manifest without pipeOffset parses with N* = -1"() {
        when:
        def parsed = mapper.readValue('{"schemaVersion":1,"createdAtMs":5,"topicHeads":{"t":3}}', SnapshotManifest)

        then:
        parsed.pipeOffset == -1L
        parsed.topicHeads["t"] == 3L
    }

    private List<String> zipEntryNames(Path zip) {
        new ZipFile(zip.toFile()).withCloseable { zf ->
            return zf.entries().collect { it.name }
        }
    }

    private byte[] readEntry(Path zip, String name) {
        new ZipFile(zip.toFile()).withCloseable { zf ->
            return zf.getInputStream(zf.getEntry(name)).bytes
        }
    }
}
