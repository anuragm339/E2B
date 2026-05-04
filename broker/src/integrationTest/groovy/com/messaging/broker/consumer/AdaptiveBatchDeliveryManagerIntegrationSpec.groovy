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
class AdaptiveBatchDeliveryManagerIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-adaptive-delivery-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19095',
            'micronaut.server.port'  : '18085',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject AdaptiveBatchDeliveryManager deliveryManager
    @Inject StorageEngine storage

    @Value('${broker.network.port}')
    int tcpPort

    private static final ObjectMapper MAPPER = new ObjectMapper()

    def "adaptive delivery start is idempotent and delivers after READY_ACK"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def client = ModernConsumerClient.connect('127.0.0.1', tcpPort)

        when:
        deliveryManager.start()
        deliveryManager.start()
        client.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 6001L,
            MAPPER.writeValueAsBytes([msg_key: 'adm-key', event_type: 'MESSAGE', data: [v: 1], topic: 'adm-topic'])))
        conditions.eventually { assert storage.getCurrentOffset('adm-topic', 0) >= 0 }
        client.subscribe('adm-topic', 'adm-group')
        conditions.eventually {
            assert client.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        client.sendReadyAck('adm-topic', 'adm-group')

        then:
        conditions.eventually {
            assert client.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        cleanup:
        client?.close()
    }
}
