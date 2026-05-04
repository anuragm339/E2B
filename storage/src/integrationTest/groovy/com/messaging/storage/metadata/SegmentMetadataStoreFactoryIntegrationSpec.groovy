package com.messaging.storage.metadata

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

@MicronautTest(startApplication = false)
class SegmentMetadataStoreFactoryIntegrationSpec extends Specification implements TestPropertyProvider {

    @Shared
    Path dataDir = Files.createTempDirectory('segment-store-factory-it-').toAbsolutePath()

    @Inject SegmentMetadataStoreFactory factory

    @Override
    Map<String, String> getProperties() {
        [
            'broker.storage.data-dir': dataDir.toString()
        ]
    }

    def cleanup() {
        factory?.closeAll()
    }

    def "factory bean returns cached stores per topic and isolates topics from each other"() {
        when:
        def pricesA = factory.getStoreForTopic('prices-v1')
        def pricesB = factory.getStoreForTopic('prices-v1')
        def orders = factory.getStoreForTopic('orders-v1')

        then:
        pricesA.is(pricesB)
        !pricesA.is(orders)
    }
}
