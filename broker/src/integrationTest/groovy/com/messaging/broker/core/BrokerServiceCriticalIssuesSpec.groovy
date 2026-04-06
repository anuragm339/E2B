package com.messaging.broker.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.support.ModernConsumerHarness
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import io.micronaut.context.annotation.Value
import io.micronaut.test.support.TestPropertyProvider
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
 * Integration tests for critical broker issues:
 * - Issue #1: Startup delivery race condition
 * - Issue #2: COMMIT_OFFSET validation
 * - Issue #3: Error handling with connection closure
 *
 * Each test uses unique topic/group names to avoid shared-storage interference
 * within the single shared Micronaut context.
 */
@MicronautTest
class BrokerServiceCriticalIssuesSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-critical-').toAbsolutePath().toString()
        return [
            'broker.network.port'    : '19093',
            'micronaut.server.port'  : '18083',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject StorageEngine storage
    @Inject ConsumerRegistry remoteConsumers
    @Inject ConsumerOffsetTracker offsetTracker

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

    // ========================================================================
    // ISSUE #3: Error Handling - Connection Closure on Validation Errors
    // ========================================================================

    def "SUBSCRIBE with missing topic closes connection"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 1001L,
            toJson([group: 'err-group']).getBytes(StandardCharsets.UTF_8)))

        then:
        new PollingConditions(timeout: 3, delay: 0.1).eventually {
            assert !consumer.connected
        }

        and: "no ACK was sent before the close"
        consumer.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1001L } == null
    }

    def "SUBSCRIBE with null group closes connection"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 1002L,
            '{"topic":"err-topic","group":null}'.getBytes(StandardCharsets.UTF_8)))

        then:
        new PollingConditions(timeout: 3, delay: 0.1).eventually {
            assert !consumer.connected
        }

        and:
        consumer.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1002L } == null
    }

    def "COMMIT_OFFSET with missing topic closes connection"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 2001L,
            toJson([group: 'err-group', offset: 100]).getBytes(StandardCharsets.UTF_8)))

        then:
        new PollingConditions(timeout: 3, delay: 0.1).eventually {
            assert !consumer.connected
        }
    }

    def "DATA with missing topic closes connection"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3001L,
            toJson([msg_key: 'key-1', event_type: 'MESSAGE', data: [value: 'v1']]).getBytes(StandardCharsets.UTF_8)))

        then:
        new PollingConditions(timeout: 3, delay: 0.1).eventually {
            assert !consumer.connected
        }

        and:
        consumer.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 3001L } == null
    }

    // ========================================================================
    // ISSUE #2: COMMIT_OFFSET Validation
    // ========================================================================

    def "COMMIT_OFFSET with negative offset closes connection"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 4001L,
            toJson([msg_key: 'key-1', event_type: 'MESSAGE', data: [value: 'v1'], topic: 'neg-offset-topic'])
                .getBytes(StandardCharsets.UTF_8)))
        conditions.eventually { assert storage.getCurrentOffset('neg-offset-topic', 0) >= 0 }

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 4002L,
            toJson([topic: 'neg-offset-topic', group: 'neg-offset-group', offset: -5]).getBytes(StandardCharsets.UTF_8)))

        then:
        conditions.eventually { assert !consumer.connected }
    }

    def "COMMIT_OFFSET exceeding storage head is clamped to storage head"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 5001L,
            toJson([msg_key: 'key-1', event_type: 'MESSAGE', data: [value: 'v1'], topic: 'clamp-topic'])
                .getBytes(StandardCharsets.UTF_8)))
        conditions.eventually { assert storage.getCurrentOffset('clamp-topic', 0) >= 0 }
        long storageHead = storage.getCurrentOffset('clamp-topic', 0)

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 5002L,
            toJson([topic: 'clamp-topic', group: 'clamp-group', offset: storageHead + 1000])
                .getBytes(StandardCharsets.UTF_8)))

        then: "offset clamped; ACK still sent (no disconnect)"
        conditions.eventually {
            assert offsetTracker.getOffset('clamp-group:clamp-topic') == storageHead
        }
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 5002L }
        }
    }

    def "subscribe with persisted offset exceeding storage head clamps offset on registration"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 6001L,
            toJson([msg_key: 'key-1', event_type: 'MESSAGE', data: [value: 'v1'], topic: 'persist-clamp-topic'])
                .getBytes(StandardCharsets.UTF_8)))
        conditions.eventually { assert storage.getCurrentOffset('persist-clamp-topic', 0) >= 0 }
        long storageHead = storage.getCurrentOffset('persist-clamp-topic', 0)
        offsetTracker.updateOffset('persist-clamp-group:persist-clamp-topic', storageHead + 500)

        when:
        consumer.subscribe('persist-clamp-topic', 'persist-clamp-group')

        then: "consumer registered with offset clamped to storageHead"
        conditions.eventually {
            def rc = remoteConsumers.getAllConsumers().find {
                it.topic == 'persist-clamp-topic' && it.group == 'persist-clamp-group'
            }
            assert rc != null
            assert rc.currentOffset == storageHead
        }

        and: "persisted offset corrected"
        conditions.eventually {
            assert offsetTracker.getOffset('persist-clamp-group:persist-clamp-topic') == storageHead
        }
    }

    // ========================================================================
    // ISSUE #1: Startup Delivery Race Condition
    // ========================================================================

    def "broker does not deliver DATA before consumer sends READY_ACK"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 7001L,
            toJson([msg_key: 'key-1', event_type: 'MESSAGE', data: [value: 'v1'], topic: 'nodeliver-topic'])
                .getBytes(StandardCharsets.UTF_8)))
        conditions.eventually { assert storage.getCurrentOffset('nodeliver-topic', 0) >= 0 }

        when: "consumer subscribes but never sends READY_ACK"
        consumer.subscribe('nodeliver-topic', 'nodeliver-group')
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any { it.topic == 'nodeliver-topic' }
        }

        then: "no DATA message is delivered (adaptive delivery not started)"
        Thread.sleep(500)
        consumer.received.find { it.type == BrokerMessage.MessageType.DATA } == null
    }

    def "adaptive delivery starts immediately after READY_ACK"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 8001L,
            toJson([msg_key: 'key-1', event_type: 'MESSAGE', data: [value: 'v1'], topic: 'adaptive-topic'])
                .getBytes(StandardCharsets.UTF_8)))
        conditions.eventually { assert storage.getCurrentOffset('adaptive-topic', 0) >= 0 }

        consumer.subscribe('adaptive-topic', 'adaptive-group')
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
        }

        when:
        consumer.sendReadyAck('adaptive-topic', 'adaptive-group')

        then: "DATA delivered immediately after READY_ACK"
        new PollingConditions(timeout: 5, delay: 0.05).eventually {
            def batches = consumer.received.findAll { it.type == BrokerMessage.MessageType.DATA }
            assert batches.size() > 0
            def decoded = OBJECT_MAPPER.readValue(batches[0].payload, List)
            assert decoded.any { it.msgKey == 'key-1' }
        }
    }

    private static String toJson(Map<String, ?> value) {
        OBJECT_MAPPER.writeValueAsString(value)
    }
}
