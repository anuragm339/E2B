package com.messaging.broker.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.support.ModernConsumerHarness
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@MicronautTest
class BrokerServiceIntegrationSpec extends Specification {

    @Inject StorageEngine storage
    @Inject ConsumerRegistry remoteConsumers
    @Inject ConsumerOffsetTracker offsetTracker
    @Inject RefreshCoordinator dataRefreshCoordinator
    @Inject EmbeddedServer embeddedServer

    @Value('${broker.network.port}')
    int tcpPort

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    ModernConsumerHarness consumer

    def setup() {
        consumer = ModernConsumerHarness.connect('127.0.0.1', tcpPort)
    }

    def cleanup() {
        consumer?.close()
    }

    def "app starts and accepts DATA messages over TCP"() {
        when:
        def payload = OBJECT_MAPPER.writeValueAsString([
            msg_key   : 'key-1',
            event_type: 'MESSAGE',
            data      : [value: 'v1'],
            topic     : 'topic-1'
        ])
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1001L,
            payload.getBytes(StandardCharsets.UTF_8)))

        then:
        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1001L }
        }
        conditions.eventually {
            def records = storage.read('topic-1', 0, 0, 10)
            assert records.size() == 1
            assert records[0].msgKey == 'key-1'
            assert records[0].eventType == EventType.MESSAGE
        }
    }

    def "SUBSCRIBE and COMMIT_OFFSET persist state via running app"() {
        given: "seed data in topic-2 so COMMIT_OFFSET validation passes (storageHead >= 0)"
        def seedPayload = OBJECT_MAPPER.writeValueAsString([
            msg_key   : 'seed-key',
            event_type: 'MESSAGE',
            data      : [value: 'seed'],
            topic     : 'topic-2'
        ])
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1999L,
            seedPayload.getBytes(StandardCharsets.UTF_8)))
        def seedConditions = new PollingConditions(timeout: 3)
        seedConditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1999L }
        }

        when:
        consumer.subscribe('topic-2', 'group-2')

        then:
        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK }
        }
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any { it.topic == 'topic-2' }
        }

        when:
        def commitPayload = OBJECT_MAPPER.writeValueAsString([
            topic : 'topic-2',
            group : 'group-2',
            offset: 0
        ])
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 2002L,
            commitPayload.getBytes(StandardCharsets.UTF_8)))

        then:
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2002L }
        }
        conditions.eventually {
            assert offsetTracker.getOffset('group-2:topic-2') == 0L
        }
    }

    def "data refresh flow: RESET -> RESET_ACK -> READY -> READY_ACK"() {
        given: "subscribe and complete the startup READY handshake before starting the refresh"
        consumer.subscribe('topic-3', 'group-3')
        def conditions = new PollingConditions(timeout: 5)
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any { it.topic == 'topic-3' }
        }
        // The broker sends a startup READY after initial delivery. Consume it before triggering refresh,
        // otherwise the test would mistake the startup READY for the refresh READY and send READY_ACK too early.
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        consumer.sendReadyAck('topic-3', 'group-3')
        consumer.received.clear()

        when:
        dataRefreshCoordinator.startRefresh('topic-3').get(3, TimeUnit.SECONDS)

        then:
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.RESET }
        }
        conditions.eventually {
            assert dataRefreshCoordinator.getRefreshStatus('topic-3').state == RefreshState.RESET_SENT
        }

        when:
        consumer.sendResetAck('topic-3', 'group-3')

        then:
        conditions.eventually {
            assert dataRefreshCoordinator.getRefreshStatus('topic-3').state == RefreshState.REPLAYING
        }
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
        }

        when:
        consumer.sendReadyAck('topic-3', 'group-3')

        then:
        conditions.eventually {
            assert dataRefreshCoordinator.getRefreshStatus('topic-3') == null ||
                dataRefreshCoordinator.getRefreshStatus('topic-3').state == RefreshState.COMPLETED
        }
    }

    def "management endpoints are available"() {
        when:
        def httpClient = HttpClient.newHttpClient()
        def healthReq = HttpRequest.newBuilder()
            .uri(new URI("http://127.0.0.1:${embeddedServer.port}/health"))
            .timeout(java.time.Duration.ofSeconds(3))
            .GET()
            .build()
        def health = httpClient.send(healthReq, HttpResponse.BodyHandlers.ofString())

        then:
        health.statusCode() == 200
        health.body().contains('"status":"UP"')
    }
}
