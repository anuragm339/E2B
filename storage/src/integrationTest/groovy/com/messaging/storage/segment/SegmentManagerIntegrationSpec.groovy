package com.messaging.storage.segment

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.storage.metadata.SegmentMetadataStore
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant

@MicronautTest(startApplication = false)
class SegmentManagerIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    def "segment manager persists data and recovers it after reopen"() {
        given:
        def topicDir = tempDir.resolve('prices-v1')
        def partitionDir = topicDir.resolve('partition-0')
        def metadataStore = new SegmentMetadataStore(topicDir)
        def manager = new SegmentManager('prices-v1', 0, partitionDir, 1024 * 1024L, metadataStore)
        3.times { i ->
            manager.append(record(i, "key-${i}", "data-${i}"))
        }
        manager.close()
        metadataStore.close()

        when:
        def reopenedStore = new SegmentMetadataStore(topicDir)
        def reopened = new SegmentManager('prices-v1', 0, partitionDir, 1024 * 1024L, reopenedStore)
        def records = reopened.read(0L, 10)

        then:
        reopened.currentOffset == 2L
        records*.msgKey == ['key-0', 'key-1', 'key-2']
        reopened.maxOffsetFromMetadata == 2L

        cleanup:
        reopened?.close()
        reopenedStore?.close()
    }

    private static MessageRecord record(long offset, String key, String data) {
        new MessageRecord(offset, 'prices-v1', 0, key, EventType.MESSAGE, data, Instant.now())
    }
}
