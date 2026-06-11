package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant

/**
 * Power-cut / torn-write recovery scenarios. The broker must reopen damaged segments by
 * truncating the unrecoverable tail rather than refusing to start (lost records are
 * re-fetched from the parent because the pipe offset only advances after append succeeds).
 */
class SegmentCrashRecoverySpec extends Specification {

    private static final int FILE_HEADER_SIZE = 6
    private static final int INDEX_ENTRY_SIZE = 16

    @TempDir
    Path tempDir

    def "B7-3: empty index with non-empty log truncates the log and starts usable"() {
        given: 'a segment with two records, closed cleanly'
        def logPath = tempDir.resolve('00000000000000000000.log')
        def indexPath = tempDir.resolve('00000000000000000000.index')
        def segment = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)
        segment.append(record(0L, 'key-0', 'data-0'))
        segment.append(record(1L, 'key-1', 'data-1'))
        segment.close()

        and: 'a crash left the index truncated back to just its header'
        truncate(indexPath, FILE_HEADER_SIZE)

        when: 'the segment is reopened'
        def reopened = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)

        then: 'orphaned log bytes were truncated and the segment is empty but usable'
        Files.size(logPath) == FILE_HEADER_SIZE
        reopened.nextOffset == 0L
        reopened.recordCount == 0L

        and: 'new appends work'
        reopened.append(record(0L, 'key-0', 'data-0-again')) == 0L
        reopened.read(0L).data == 'data-0-again'

        cleanup:
        reopened?.close()
    }

    def "B7-3: zero-length index file with non-empty log reopens instead of failing startup"() {
        given:
        def logPath = tempDir.resolve('00000000000000000000.log')
        def indexPath = tempDir.resolve('00000000000000000000.index')
        def segment = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)
        segment.append(record(0L, 'key-0', 'data-0'))
        segment.close()

        and: 'a crash before the index header write left a zero-length index file'
        truncate(indexPath, 0)

        when:
        def reopened = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)

        then: 'the index header was re-initialised and the orphaned log bytes truncated'
        notThrown(Exception)
        reopened.recordCount == 0L
        Files.size(logPath) == FILE_HEADER_SIZE

        cleanup:
        reopened?.close()
    }

    def "B7-4: index entries beyond the log size are dropped, valid prefix survives"() {
        given: 'a segment with three records'
        def logPath = tempDir.resolve('00000000000000000000.log')
        def indexPath = tempDir.resolve('00000000000000000000.index')
        def segment = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)
        segment.append(record(0L, 'key-0', 'data-0'))
        segment.append(record(1L, 'key-1', 'data-1'))
        segment.append(record(2L, 'key-2', 'data-2'))
        segment.close()

        and: 'the log lost its last record (index made it to disk, log bytes did not)'
        long fullLogSize = Files.size(logPath)
        long indexSize = Files.size(indexPath)
        // each record here has identical size: (logSize - header) / 3
        long recordBytes = (fullLogSize - FILE_HEADER_SIZE).intdiv(3)
        truncate(logPath, fullLogSize - recordBytes)

        when:
        def reopened = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)

        then: 'the dangling index entry was truncated away'
        Files.size(indexPath) == indexSize - INDEX_ENTRY_SIZE
        reopened.recordCount == 2L
        reopened.nextOffset == 2L

        and: 'the surviving records are readable'
        reopened.read(0L).data == 'data-0'
        reopened.read(1L).data == 'data-1'
        reopened.read(2L) == null

        cleanup:
        reopened?.close()
    }

    def "B7-2: orphaned log bytes beyond the last indexed record are truncated on reopen"() {
        given:
        def logPath = tempDir.resolve('00000000000000000000.log')
        def indexPath = tempDir.resolve('00000000000000000000.index')
        def segment = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)
        segment.append(record(0L, 'key-0', 'data-0'))
        segment.close()

        and: 'a partial record write landed in the log without an index entry'
        long indexedLogSize = Files.size(logPath)
        appendGarbage(logPath, 17)

        when:
        def reopened = new Segment(logPath, indexPath, 0L, 1024 * 1024L, 'prices-v1', 0)

        then:
        Files.size(logPath) == indexedLogSize
        reopened.recordCount == 1L
        reopened.read(0L).data == 'data-0'

        cleanup:
        reopened?.close()
    }

    private static void truncate(Path path, long size) {
        FileChannel.open(path, StandardOpenOption.WRITE).withCloseable { it.truncate(size) }
    }

    private static void appendGarbage(Path path, int bytes) {
        Files.write(path, new byte[bytes], StandardOpenOption.APPEND)
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'prices-v1', 0, key, EventType.MESSAGE, data, Instant.now())
    }
}
