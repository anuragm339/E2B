package com.messaging.storage.segment

import com.messaging.common.exception.StorageException
import com.messaging.storage.segment.StorageRecoveryService
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class DefaultStorageRecoveryServiceSpec extends Specification {

    @TempDir
    Path tempDir

    StorageRecoveryService recoveryService

    def setup() {
        recoveryService = new DefaultStorageRecoveryService()
    }

    def "should extract offset from valid filename"() {
        when:
        def offset = recoveryService.extractOffsetFromFilename("00000000000000000100.log")

        then:
        offset == 100L
    }

    def "should extract zero offset from filename"() {
        when:
        def offset = recoveryService.extractOffsetFromFilename("00000000000000000000.log")

        then:
        offset == 0L
    }

    def "should extract large offset from filename"() {
        when:
        def offset = recoveryService.extractOffsetFromFilename("00000000012345678900.log")

        then:
        offset == 12345678900L
    }

    def "should throw exception for invalid filename"() {
        when:
        recoveryService.extractOffsetFromFilename("invalid.log")

        then:
        thrown(StorageException)
    }

    def "should throw exception for missing extension"() {
        when:
        recoveryService.extractOffsetFromFilename("00000000000000000100")

        then:
        thrown(StorageException)
    }

    def "should recover empty directory"() {
        when:
        def result = recoveryService.recoverSegments(tempDir, "test-topic", 0, 1073741824L)

        then:
        result != null
        result.totalSegments == 0
        !result.hasActiveSegment()
    }

    def "should handle corrupted segment files gracefully"() {
        given:
        // Create files that look like segments but don't have valid headers
        // The recovery service should log errors but not crash
        Files.createFile(tempDir.resolve("00000000000000000000.log"))
        Files.createFile(tempDir.resolve("corrupted.log")) // Invalid filename
        Files.write(tempDir.resolve("00000000000000000000.log"), new byte[100])
        Files.write(tempDir.resolve("corrupted.log"), new byte[100])

        when:
        def result = recoveryService.recoverSegments(tempDir, "test-topic", 0, 1073741824L)

        then:
        // Should handle errors gracefully and return empty result
        result != null
        result.totalSegments == 0  // No valid segments loaded
        !result.hasActiveSegment()
    }

    def "should ignore non-log files"() {
        given:
        Files.createFile(tempDir.resolve("00000000000000000000.index"))
        Files.createFile(tempDir.resolve("metadata.db"))
        Files.createFile(tempDir.resolve("README.txt"))

        when:
        def result = recoveryService.recoverSegments(tempDir, "test-topic", 0, 1073741824L)

        then:
        // Should only attempt to load .log files (none present)
        result.totalSegments == 0
        !result.hasActiveSegment()
    }

    def "should throw exception for non-existent directory"() {
        given:
        def nonExistent = tempDir.resolve("does-not-exist")

        when:
        recoveryService.recoverSegments(nonExistent, "test-topic", 0, 1073741824L)

        then:
        thrown(StorageException)
    }
}
