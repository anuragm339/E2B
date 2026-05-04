package com.messaging.storage.metadata

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class SegmentMetadataStoreFactorySpec extends Specification {

    @TempDir
    Path tempDir

    def "getStoreForTopic caches one store per topic"() {
        given:
        def factory = new SegmentMetadataStoreFactory(tempDir.toString())

        when:
        def first = factory.getStoreForTopic('prices-v1')
        def second = factory.getStoreForTopic('prices-v1')

        then:
        first.is(second)

        cleanup:
        factory.closeAll()
    }

    def "closeAll clears cached stores so later lookup creates a new instance"() {
        given:
        def factory = new SegmentMetadataStoreFactory(tempDir.toString())
        def first = factory.getStoreForTopic('prices-v1')

        when:
        factory.closeAll()
        def second = factory.getStoreForTopic('prices-v1')

        then:
        !first.is(second)

        cleanup:
        factory.closeAll()
    }
}
