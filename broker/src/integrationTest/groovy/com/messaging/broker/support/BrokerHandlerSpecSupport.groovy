package com.messaging.broker.support

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.common.api.StorageEngine
import io.micronaut.context.annotation.Value
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.nio.file.Files

abstract class BrokerHandlerSpecSupport extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-handler-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19096',
            'micronaut.server.port'  : '18086',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject StorageEngine storage
    @Inject ConsumerRegistry remoteConsumers
    @Inject ConsumerOffsetTracker offsetTracker

    @Value('${broker.network.port}')
    int tcpPort

    protected static final ObjectMapper MAPPER = new ObjectMapper()

    ModernConsumerClient consumer

    def setup() {
        consumer = ModernConsumerClient.connect('127.0.0.1', tcpPort)
    }

    def cleanup() {
        consumer?.close()
    }

    protected static String toJson(Map<String, ?> map) {
        MAPPER.writeValueAsString(map)
    }
}
