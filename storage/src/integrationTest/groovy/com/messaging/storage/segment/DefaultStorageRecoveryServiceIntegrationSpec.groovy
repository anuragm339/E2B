package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

@MicronautTest(startApplication = false)
class DefaultStorageRecoveryServiceIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    @Inject DefaultStorageRecoveryService recoveryService

    def "recoverSegments loads all valid segments and marks the last one active"() {
        given:
        def first = new Segment(tempDir.resolve('00000000000000000000.log'),
            tempDir.resolve('00000000000000000000.index'),
            0L,
            1024 * 1024L,
            'prices-v1',
            0)
        def second = new Segment(tempDir.resolve('00000000000000000100.log'),
            tempDir.resolve('00000000000000000100.index'),
            100L,
            1024 * 1024L,
            'prices-v1',
            0)
        3.times { i -> first.append(record(i, "a-${i}", "data-${i}")) }
        3.times { i -> second.append(record(100L + i, "b-${i}", "data-${i}")) }
        first.close()
        second.close()

        when:
        def result = recoveryService.recoverSegments(tempDir, 'prices-v1', 0, 1024 * 1024L)

        then:
        result.totalSegments == 2
        result.hasActiveSegment()
        result.activeSegment.baseOffset == 100L
        result.segments*.baseOffset == [0L, 100L]

        cleanup:
        result?.segments?.each { it.close() }
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'prices-v1', 0, key, EventType.MESSAGE, data, Instant.now())
    }
}
