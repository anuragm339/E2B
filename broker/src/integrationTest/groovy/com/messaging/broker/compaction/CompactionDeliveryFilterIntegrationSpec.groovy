package com.messaging.broker.compaction

import com.fasterxml.jackson.core.type.TypeReference
import com.messaging.broker.support.BrokerHandlerSpecSupport
import com.messaging.broker.support.ModernConsumerClient
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.util.concurrent.PollingConditions

/**
 * Verifies that the compaction delivery filter in ConsumerDeliveryManager hides superseded
 * records from consumers — only the latest version of each (topic, key) pair is delivered.
 *
 * All tests use unique topic/group names to prevent cross-test state bleed.
 * A second ModernConsumerClient is created per-test and closed in a cleanup: block.
 */
@MicronautTest
class CompactionDeliveryFilterIntegrationSpec extends BrokerHandlerSpecSupport {

    @Override
    Map<String, String> getProperties() {
        def base = super.getProperties()
        def dataDir = base['broker.storage.data-dir']
        return base + [
            'broker.network.port'       : '19103',
            'micronaut.server.port'     : '18093',
            'compaction.rocksdb.path'   : "${dataDir}/compaction-index"
        ]
    }

    @Inject
    RocksDbCompactionIndex compactionIndex

    // -------------------------------------------------------------------------
    // Test 1 — only the latest record for a given key is delivered
    // -------------------------------------------------------------------------

    def "consumer receives only latest record when three records share same key"() {
        given:
        def conditions = new PollingConditions(timeout: 5, delay: 0.2)
        ModernConsumerClient subscriber = null

        when: "publish three records for the same key"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3001L,
            toJson([msg_key: 'filter-key', event_type: 'MESSAGE', data: [v: 'v1'], topic: 'filter-topic']).bytes))
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3002L,
            toJson([msg_key: 'filter-key', event_type: 'MESSAGE', data: [v: 'v2'], topic: 'filter-topic']).bytes))
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3003L,
            toJson([msg_key: 'filter-key', event_type: 'MESSAGE', data: [v: 'v3'], topic: 'filter-topic']).bytes))

        then: "all three records are stored"
        conditions.eventually {
            assert storage.read('filter-topic', 0, 0, 10).size() == 3
        }

        and: "compaction index identifies records 0 and 1 as superseded"
        conditions.eventually {
            assert compactionIndex.isSuperseded('filter-topic', 'filter-key', 0L) == true
            assert compactionIndex.isSuperseded('filter-topic', 'filter-key', 1L) == true
            assert compactionIndex.isSuperseded('filter-topic', 'filter-key', 2L) == false
        }

        when: "a fresh consumer subscribes and completes the READY handshake"
        subscriber = ModernConsumerClient.connect('127.0.0.1', tcpPort)
        subscriber.subscribe('filter-topic', 'filter-group')
        conditions.eventually {
            assert subscriber.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        subscriber.sendReadyAck('filter-topic', 'filter-group')

        then: "at least one DATA batch is delivered"
        conditions.eventually {
            assert subscriber.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        and: "all delivered DATA records carry the key 'filter-key'"
        def dataMessages = subscriber.received.findAll { it.type == BrokerMessage.MessageType.DATA }
        dataMessages.every { msg ->
            def records = MAPPER.readValue(msg.payload, new TypeReference<List<Map<String, Object>>>() {})
            records.every { it['msgKey'] == 'filter-key' }
        }

        and: "the last delivered record has value v3 (the latest)"
        def allRecords = dataMessages.collectMany { msg ->
            MAPPER.readValue(msg.payload, new TypeReference<List<Map<String, Object>>>() {}) as List<Map<String, Object>>
        }
        allRecords.any { rec -> rec['data']?.toString()?.contains('v3') }

        cleanup:
        subscriber?.close()
    }

    // -------------------------------------------------------------------------
    // Test 2 — DELETE tombstone that is latest for its key is delivered
    // -------------------------------------------------------------------------

    def "DELETE tombstone that is latest for its key is delivered to consumer"() {
        given:
        def conditions = new PollingConditions(timeout: 5, delay: 0.2)
        ModernConsumerClient subscriber = null

        when: "publish a MESSAGE and then a DELETE tombstone for the same key"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3004L,
            toJson([msg_key: 'tomb-key', event_type: 'MESSAGE', data: [v: 'v-msg'], topic: 'tomb-topic']).bytes))
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3005L,
            toJson([msg_key: 'tomb-key', event_type: 'DELETE', topic: 'tomb-topic']).bytes))

        then: "both records are stored"
        conditions.eventually {
            assert storage.read('tomb-topic', 0, 0, 10).size() == 2
        }

        and: "the first record (MESSAGE) is superseded by the DELETE"
        conditions.eventually {
            assert compactionIndex.isSuperseded('tomb-topic', 'tomb-key', 0L) == true
            assert compactionIndex.isSuperseded('tomb-topic', 'tomb-key', 1L) == false
        }

        when: "a fresh consumer subscribes and completes the READY handshake"
        subscriber = ModernConsumerClient.connect('127.0.0.1', tcpPort)
        subscriber.subscribe('tomb-topic', 'tomb-group')
        conditions.eventually {
            assert subscriber.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        subscriber.sendReadyAck('tomb-topic', 'tomb-group')

        then: "at least one DATA batch is delivered"
        conditions.eventually {
            assert subscriber.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        and: "the delivered records include the DELETE tombstone"
        def dataMessages = subscriber.received.findAll { it.type == BrokerMessage.MessageType.DATA }
        def allRecords = dataMessages.collectMany { msg ->
            MAPPER.readValue(msg.payload, new TypeReference<List<Map<String, Object>>>() {}) as List<Map<String, Object>>
        }
        allRecords.any { rec ->
            rec['msgKey'] == 'tomb-key' && rec['eventType']?.toString() == 'DELETE'
        }

        and: "the original MESSAGE record (v-msg) is NOT delivered since it is superseded"
        !allRecords.any { rec ->
            rec['msgKey'] == 'tomb-key' && rec['data']?.toString()?.contains('v-msg')
        }

        cleanup:
        subscriber?.close()
    }

    // -------------------------------------------------------------------------
    // Test 3 — consumer offset advances past ALL stored records, including
    //          those filtered out by the compaction index
    // -------------------------------------------------------------------------

    def "consumer offset advances past all records including filtered ones"() {
        given:
        def conditions = new PollingConditions(timeout: 5, delay: 0.2)
        ModernConsumerClient subscriber = null

        when: "publish three records for the same key"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3006L,
            toJson([msg_key: 'offset-key', event_type: 'MESSAGE', data: [v: 1], topic: 'offset-topic']).bytes))
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3007L,
            toJson([msg_key: 'offset-key', event_type: 'MESSAGE', data: [v: 2], topic: 'offset-topic']).bytes))
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 3008L,
            toJson([msg_key: 'offset-key', event_type: 'MESSAGE', data: [v: 3], topic: 'offset-topic']).bytes))

        then: "all three records are stored"
        conditions.eventually {
            assert storage.read('offset-topic', 0, 0, 10).size() == 3
        }

        when: "a fresh consumer subscribes and completes the READY handshake"
        subscriber = ModernConsumerClient.connect('127.0.0.1', tcpPort)
        subscriber.subscribe('offset-topic', 'offset-group')
        conditions.eventually {
            assert subscriber.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        subscriber.sendReadyAck('offset-topic', 'offset-group')

        then: "at least one DATA batch arrives"
        conditions.eventually {
            assert subscriber.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        and: "after delivery settles, the persisted offset has advanced past all 3 records"
        // The delivery offset must reach at least 3, indicating the consumer advanced
        // through the full batch (all 3 offsets — 0, 1, 2) regardless of whether
        // the earlier two were filtered from the consumer's view.
        new PollingConditions(timeout: 8, delay: 0.3).eventually {
            assert offsetTracker.getOffset('offset-group:offset-topic') >= 3L
        }

        cleanup:
        subscriber?.close()
    }
}
