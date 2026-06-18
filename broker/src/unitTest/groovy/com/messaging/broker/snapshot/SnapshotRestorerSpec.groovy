package com.messaging.broker.snapshot

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class SnapshotRestorerSpec extends Specification {

    @TempDir
    Path tempDir

    SnapshotBuilder builder = new SnapshotBuilder()
    SnapshotRestorer restorer = new SnapshotRestorer()

    private void write(Path root, String relative, String content) {
        Path p = root.resolve(relative)
        Files.createDirectories(p.parent)
        Files.writeString(p, content)
    }

    def "restore lays down topic data from a snapshot, replacing stale topic data"() {
        given: "a source data dir snapshotted into a zip"
        Path src = tempDir.resolve("src")
        write(src, "prices-v1/segment_metadata.db", "fresh-sqlite")
        write(src, "prices-v1/partition-0/0.log", "fresh-log")
        Path zip = tempDir.resolve("snap.zip")
        builder.build(src, zip, ["prices-v1": 9L])

        and: "a target data dir with STALE prices-v1 data and an unrelated ack-store"
        Path dst = tempDir.resolve("dst")
        write(dst, "prices-v1/segment_metadata.db", "stale-sqlite")
        write(dst, "prices-v1/partition-0/old.log", "stale-old")
        write(dst, "ack-store/CURRENT", "rocks")

        when:
        def manifest = restorer.restore(zip, dst)

        then: "manifest returned"
        manifest.topicHeads["prices-v1"] == 9L

        and: "fresh topic data is in place"
        Files.readString(dst.resolve("prices-v1/segment_metadata.db")) == "fresh-sqlite"
        Files.readString(dst.resolve("prices-v1/partition-0/0.log")) == "fresh-log"

        and: "the stale segment file that is not in the snapshot was removed (topic dir swapped)"
        !Files.exists(dst.resolve("prices-v1/partition-0/old.log"))

        and: "ack-store (the child's own, not in the snapshot) is left untouched"
        Files.exists(dst.resolve("ack-store/CURRENT"))

        and: "staging dir is cleaned up"
        !Files.exists(dst.resolve(".snapshot-staging"))
    }

    def "restore fails cleanly when the zip has no manifest"() {
        given: "a zip without a manifest (a bare file zipped)"
        Path dst = tempDir.resolve("dst2")
        Files.createDirectories(dst)
        Path zip = tempDir.resolve("bad.zip")
        zip.toFile().withOutputStream { os ->
            new java.util.zip.ZipOutputStream(os).withCloseable { zos ->
                zos.putNextEntry(new java.util.zip.ZipEntry("prices-v1/segment_metadata.db"))
                zos.write("x".bytes)
                zos.closeEntry()
            }
        }

        when:
        restorer.restore(zip, dst)

        then:
        def e = thrown(com.messaging.common.exception.DataRefreshException)
        e.errorCode == com.messaging.common.exception.ErrorCode.DATA_REFRESH_SNAPSHOT_RESTORE_FAILED
    }
}
