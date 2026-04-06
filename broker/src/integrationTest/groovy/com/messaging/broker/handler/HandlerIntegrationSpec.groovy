package com.messaging.broker.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.support.ModernConsumerHarness
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import io.micronaut.context.annotation.Value
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
 * Integration tests for each message handler: DataHandler, SubscribeHandler,
 * CommitOffsetHandler, BatchAckHandler, and ClientDisconnectHandler.
 *
 * Each test uses a unique topic/group to avoid shared-storage interference.
 */
@MicronautTest
class HandlerIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-handler-').toAbsolutePath().toString()
        return [
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

    private static final ObjectMapper MAPPER = new ObjectMapper()

    ModernConsumerHarness consumer

    def setup() {
        consumer = ModernConsumerHarness.connect('127.0.0.1', tcpPort)
    }

    def cleanup() {
        consumer?.close()
    }

    // =========================================================================
    // DataHandler
    // =========================================================================

    def "DataHandler stores message and sends ACK"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1001L,
            toJson([msg_key: 'dh-key-1', event_type: 'MESSAGE',
                    data: [v: 1], topic: 'dh-store-topic']).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 1001L
            }
            def records = storage.read('dh-store-topic', 0, 0, 10)
            assert records.size() == 1
            assert records[0].msgKey == 'dh-key-1'
            assert records[0].eventType == EventType.MESSAGE
        }
    }

    def "DataHandler stores DELETE event with null data"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1002L,
            toJson([msg_key: 'dh-key-del', event_type: 'DELETE',
                    topic: 'dh-delete-topic']).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 1002L
            }
            def records = storage.read('dh-delete-topic', 0, 0, 10)
            assert records.size() == 1
            assert records[0].eventType == EventType.DELETE
            assert records[0].data == null
        }
    }

    def "DataHandler closes connection when topic field is missing"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 1003L,
            toJson([msg_key: 'dh-key-notopic', event_type: 'MESSAGE',
                    data: [v: 1]]).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert !consumer.connected
        }
    }

    // =========================================================================
    // SubscribeHandler
    // =========================================================================

    def "SubscribeHandler registers consumer and sends ACK + READY"() {
        when:
        consumer.subscribe('sh-topic', 'sh-group')

        then:
        new PollingConditions(timeout: 3).eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK
            }
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.READY
            }
            assert remoteConsumers.getAllConsumers().any {
                it.topic == 'sh-topic' && it.group == 'sh-group'
            }
        }
    }

    def "SubscribeHandler closes connection when topic is missing"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 2001L,
            toJson([group: 'sh-err-group']).bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert !consumer.connected
        }
    }

    def "SubscribeHandler closes connection when group is null"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 2002L,
            '{"topic":"sh-null-topic","group":null}'.bytes))

        then:
        new PollingConditions(timeout: 3).eventually {
            assert !consumer.connected
        }
    }

    def "SubscribeHandler duplicate subscription does not double-register"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.subscribe('sh-dup-topic', 'sh-dup-group')
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK }
        }
        int countBefore = remoteConsumers.getAllConsumers()
            .count { it.topic == 'sh-dup-topic' && it.group == 'sh-dup-group' }

        when:
        consumer.received.clear()
        consumer.subscribe('sh-dup-topic', 'sh-dup-group')

        then: "second SUBSCRIBE is accepted (ACK returned) but consumer count stays at 1"
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK }
        }
        remoteConsumers.getAllConsumers()
            .count { it.topic == 'sh-dup-topic' && it.group == 'sh-dup-group' } == countBefore
    }

    // =========================================================================
    // CommitOffsetHandler
    // =========================================================================

    def "CommitOffsetHandler persists valid offset and sends ACK"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        // Write a record so topic head >= 0
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3000L,
            toJson([msg_key: 'co-key', event_type: 'MESSAGE',
                    data: [v: 1], topic: 'co-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('co-topic', 0) >= 0 }

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3001L,
            toJson([topic: 'co-topic', group: 'co-group', offset: 0]).bytes))

        then:
        conditions.eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 3001L
            }
            assert offsetTracker.getOffset('co-group:co-topic') == 0L
        }
    }

    def "CommitOffsetHandler closes connection on negative offset"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3100L,
            toJson([msg_key: 'co-neg-key', event_type: 'MESSAGE',
                    data: [v: 1], topic: 'co-neg-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('co-neg-topic', 0) >= 0 }

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3101L,
            toJson([topic: 'co-neg-topic', group: 'co-neg-group', offset: -1]).bytes))

        then:
        conditions.eventually { assert !consumer.connected }
    }

    def "CommitOffsetHandler clamps offset exceeding storage head"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3200L,
            toJson([msg_key: 'co-clamp-key', event_type: 'MESSAGE',
                    data: [v: 1], topic: 'co-clamp-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('co-clamp-topic', 0) >= 0 }
        long head = storage.getCurrentOffset('co-clamp-topic', 0)

        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3201L,
            toJson([topic: 'co-clamp-topic', group: 'co-clamp-group',
                    offset: head + 9999]).bytes))

        then: "ACK sent and offset clamped to storage head"
        conditions.eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 3201L
            }
            assert offsetTracker.getOffset('co-clamp-group:co-clamp-topic') == head
        }
    }

    def "CommitOffsetHandler closes connection when topic field is missing"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 3300L,
            toJson([group: 'co-miss-group', offset: 0]).bytes))

        then:
        new PollingConditions(timeout: 3).eventually { assert !consumer.connected }
    }

    // =========================================================================
    // BatchAckHandler
    // =========================================================================

    def "BatchAckHandler processes modern BATCH_ACK after delivery"() {
        given:
        def conditions = new PollingConditions(timeout: 5)

        // Publish a record
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 4000L,
            toJson([msg_key: 'ba-key', event_type: 'MESSAGE',
                    data: [v: 1], topic: 'ba-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('ba-topic', 0) >= 0 }

        // Subscribe and complete startup READY handshake to enable delivery
        consumer.subscribe('ba-topic', 'ba-group')
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        consumer.sendReadyAck('ba-topic', 'ba-group')

        // Wait for DATA batch to arrive
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        when: "consumer sends BATCH_ACK"
        def topicBytes = 'ba-topic'.getBytes(StandardCharsets.UTF_8)
        def groupBytes = 'ba-group'.getBytes(StandardCharsets.UTF_8)
        def buf = ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length)
        buf.putInt(topicBytes.length)
        buf.put(topicBytes)
        buf.putInt(groupBytes.length)
        buf.put(groupBytes)
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.BATCH_ACK, 4001L, buf.array()))

        then: "offset advances after ACK is processed"
        conditions.eventually {
            assert offsetTracker.getOffset('ba-group:ba-topic') >= 0
        }
    }

    // =========================================================================
    // ClientDisconnectHandler
    // =========================================================================

    def "ClientDisconnectHandler unregisters consumer on disconnect"() {
        given:
        def conditions = new PollingConditions(timeout: 3)
        consumer.subscribe('cd-topic', 'cd-group')
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any {
                it.topic == 'cd-topic' && it.group == 'cd-group'
            }
        }

        when:
        consumer.close()
        consumer = null  // prevent double-close in cleanup()

        then: "consumer unregistered after disconnect"
        conditions.eventually {
            assert !remoteConsumers.getAllConsumers().any {
                it.topic == 'cd-topic' && it.group == 'cd-group'
            }
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static String toJson(Map<String, ?> map) {
        MAPPER.writeValueAsString(map)
    }
}
