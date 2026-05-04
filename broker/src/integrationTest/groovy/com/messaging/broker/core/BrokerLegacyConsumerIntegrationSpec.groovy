package com.messaging.broker.core

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.support.LegacyConsumerClient
import com.messaging.common.api.StorageEngine
import com.messaging.network.legacy.events.BatchEvent
import com.messaging.network.legacy.events.ReadyEvent
import com.messaging.network.legacy.events.ResetEvent
import io.micronaut.context.annotation.Value
import io.micronaut.test.support.TestPropertyProvider
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.file.Files

/**
 * Integration tests for legacy consumer protocol (Event-based binary protocol).
 *
 * Uses rebuildContext=true so each test has an isolated broker state.
 */
@MicronautTest(rebuildContext = true)
class BrokerLegacyConsumerIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-legacy-').toAbsolutePath().toString()
        return [
            'broker.network.port'    : '19094',
            'micronaut.server.port'  : '18084',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject StorageEngine storage
    @Inject ConsumerRegistry remoteConsumers
    @Inject ConsumerOffsetTracker offsetTracker
    @Inject RefreshCoordinator dataRefreshCoordinator

    @Value('${broker.network.port}')
    int tcpPort

    LegacyConsumerClient legacy

    def setup() {
        legacy = LegacyConsumerClient.connect('127.0.0.1', tcpPort, 'price-quote-service')
    }

    def cleanup() {
        legacy?.close()
    }

    def "legacy consumer receives merged batch and ACK advances offsets"() {
        given:
        def conditions = new PollingConditions(timeout: 8, delay: 0.1)

        and: "seed a message on prices-v1"
        def fakeModern = com.messaging.broker.support.ModernConsumerClient.connect('127.0.0.1', tcpPort)
        try {
            def payload = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString([
                msg_key: 'legacy-key-1', event_type: 'MESSAGE', data: [value: 'legacy-v1'], topic: 'prices-v1'
            ])
            fakeModern.send(new com.messaging.common.model.BrokerMessage(
                com.messaging.common.model.BrokerMessage.MessageType.DATA, 100L,
                payload.getBytes('UTF-8')))
            conditions.eventually { assert storage.getCurrentOffset('prices-v1', 0) >= 0 }
        } finally {
            fakeModern.close()
        }

        expect: "legacy registration creates subscriptions and broker sends startup READY"
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().findAll {
                it.legacy && it.group == 'price-quote-service'
            }.size() > 0
            assert legacy.received.any { it instanceof ReadyEvent }
        }

        when: "legacy consumer acknowledges startup READY, then reset offset to force re-delivery"
        legacy.sendAck()
        legacy.received.clear()
        offsetTracker.updateOffset('price-quote-service:prices-v1', -1L)

        def clientId = remoteConsumers.getAllConsumers()
            .find { it.legacy && it.group == 'price-quote-service' && it.topic == 'prices-v1' }
            .clientId

        then: "broker delivers a merged batch via adaptive delivery"
        conditions.eventually {
            assert legacy.errors.isEmpty()
            def batch = legacy.received.find { it instanceof BatchEvent } as BatchEvent
            assert batch != null
            assert batch.count() == 1
            assert batch.messages[0].key == 'legacy-key-1'
            assert batch.messages[0].content == '{"value":"legacy-v1"}'
        }

        when: "legacy consumer sends generic ACK for the batch"
        long storageHead = storage.getCurrentOffset('prices-v1', 0)
        legacy.sendAck()

        then: "offset advances to storage head"
        conditions.eventually {
            assert offsetTracker.getOffset('price-quote-service:prices-v1') == storageHead
        }
    }

    def "legacy refresh flow resolves RESET and READY through generic ACKs"() {
        given:
        def conditions = new PollingConditions(timeout: 8, delay: 0.1)

        and: "wait for startup READY and complete the handshake"
        conditions.eventually {
            assert legacy.received.any { it instanceof ReadyEvent }
        }
        legacy.sendAck()
        legacy.received.clear()

        when: "refresh is triggered for a legacy topic"
        dataRefreshCoordinator.startRefresh('prices-v1').get()

        then: "legacy consumer receives RESET and state becomes RESET_SENT"
        conditions.eventually {
            assert legacy.received.any { it instanceof ResetEvent }
            assert dataRefreshCoordinator.getRefreshStatus('prices-v1').state == RefreshState.RESET_SENT
        }

        when: "legacy consumer ACKs the RESET (generic ACK resolves RESET)"
        legacy.sendAck()

        then: "state transitions to REPLAYING and broker eventually sends READY"
        conditions.eventually {
            assert dataRefreshCoordinator.getRefreshStatus('prices-v1').state == RefreshState.REPLAYING
        }
        conditions.eventually {
            assert legacy.received.any { it instanceof ReadyEvent }
        }

        when: "legacy consumer ACKs the READY"
        legacy.sendAck()

        then: "refresh completes"
        conditions.eventually {
            assert dataRefreshCoordinator.getRefreshStatus('prices-v1') == null ||
                dataRefreshCoordinator.getRefreshStatus('prices-v1').state == RefreshState.COMPLETED
        }
    }
}
