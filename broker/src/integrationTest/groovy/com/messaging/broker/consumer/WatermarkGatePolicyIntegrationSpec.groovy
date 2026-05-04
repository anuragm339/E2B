package com.messaging.broker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.support.ModernConsumerClient
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import io.micronaut.context.annotation.Value
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.file.Files

@MicronautTest
class WatermarkGatePolicyIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-watermark-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19097',
            'micronaut.server.port'  : '18087',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject WatermarkGatePolicy watermarkGate
    @Inject ConsumerRegistry remoteConsumers
    @Inject StorageEngine storage

    @Value('${broker.network.port}')
    int tcpPort

    private static final ObjectMapper MAPPER = new ObjectMapper()

    def "watermark gate allows delivery when storage is ahead of the consumer offset"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def client = ModernConsumerClient.connect('127.0.0.1', tcpPort)

        when:
        client.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 5001L,
            MAPPER.writeValueAsBytes([msg_key: 'wm-key', event_type: 'MESSAGE', data: [v: 1], topic: 'wm-topic'])))
        conditions.eventually { assert storage.getCurrentOffset('wm-topic', 0) >= 0 }
        client.subscribe('wm-topic', 'wm-group')
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any { it.topic == 'wm-topic' && it.group == 'wm-group' }
        }
        def consumer = remoteConsumers.getAllConsumers().find { it.topic == 'wm-topic' && it.group == 'wm-group' }

        then:
        watermarkGate.shouldDeliver(consumer).allowed

        cleanup:
        client?.close()
    }
}
