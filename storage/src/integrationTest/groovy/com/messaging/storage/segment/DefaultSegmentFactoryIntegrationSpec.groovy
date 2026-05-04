package com.messaging.storage.segment

import com.messaging.storage.metadata.SegmentMetadataStore
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

@MicronautTest(startApplication = false)
class DefaultSegmentFactoryIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    @Inject DefaultSegmentFactory factory

    def "createSegment uses zero-padded filenames and returns an active segment"() {
        given:
        def metadataStore = new SegmentMetadataStore(tempDir)

        when:
        def segment = factory.createSegment(tempDir, 'prices-v1', 0, 123L, metadataStore)

        then:
        segment.baseOffset == 123L
        segment.active
        segment.logPath.fileName.toString() == '00000000000000000123.log'
        segment.indexPath.fileName.toString() == '00000000000000000123.index'

        cleanup:
        segment?.close()
        metadataStore?.close()
    }
}
