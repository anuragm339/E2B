package com.messaging.broker.consumer

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.nio.file.Files

@MicronautTest
class ConsumerOffsetTrackerIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-consumer-offsets-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19099',
            'micronaut.server.port'  : '18089',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject ConsumerOffsetTracker offsetTracker

    def "offset tracker stores resets and isolates offsets by consumer key"() {
        when:
        offsetTracker.updateOffset('g1:topic-a', 100L)
        offsetTracker.updateOffset('g2:topic-a', 200L)
        offsetTracker.updateOffset('g1:topic-b', 300L)
        offsetTracker.resetOffset('reset-group:reset-topic', -1L)

        then:
        offsetTracker.getOffset('g1:topic-a') == 100L
        offsetTracker.getOffset('g2:topic-a') == 200L
        offsetTracker.getOffset('g1:topic-b') == 300L
        offsetTracker.getOffset('reset-group:reset-topic') == -1L
        offsetTracker.getOffset('unknown-group:unknown-topic') == 0L
    }

    def "offset tracker overwrites existing values"() {
        given:
        offsetTracker.updateOffset('ot-group:ot-topic', 10L)

        when:
        offsetTracker.updateOffset('ot-group:ot-topic', 99L)

        then:
        offsetTracker.getOffset('ot-group:ot-topic') == 99L
    }
}
