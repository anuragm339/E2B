package com.messaging.broker.compaction

import com.messaging.broker.support.BrokerHandlerSpecSupport
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.util.concurrent.PollingConditions

/**
 * Verifies that DataHandler wires RocksDbCompactionIndex.updateKey() on every append,
 * and that the index correctly tracks the latest offset per (topic, msgKey).
 *
 * Each test uses a unique topic name to prevent state bleed across tests sharing the
 * same Micronaut application context.
 */
@MicronautTest
class CompactionIndexWiringIntegrationSpec extends BrokerHandlerSpecSupport {

    @Override
    Map<String, String> getProperties() {
        def base = super.getProperties()
        def dataDir = base['broker.storage.data-dir']
        return base + [
            'broker.network.port'       : '19102',
            'micronaut.server.port'     : '18092',
            'compaction.rocksdb.path'   : "${dataDir}/compaction-index"
        ]
    }

    @Inject
    RocksDbCompactionIndex compactionIndex

    // -------------------------------------------------------------------------
    // Test 1 — single record with a key updates the index
    // -------------------------------------------------------------------------

    def "handleMessage() updates compaction index when record has msgKey"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2001L,
            toJson([msg_key: 'ci-key-1', event_type: 'MESSAGE', data: [v: 1], topic: 'ci-topic']).bytes))

        then: "broker ACKs the message"
        new PollingConditions(timeout: 5, delay: 0.1).eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 2001L
            }
        }

        and: "record is stored in storage"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert storage.read('ci-topic', 0, 0, 10).size() == 1
        }

        and: "compaction index was updated for the key"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert compactionIndex.getLatestOffsetAndTimestamp('ci-topic', 'ci-key-1') != null
        }
    }

    // -------------------------------------------------------------------------
    // Test 2 — record without explicit msg_key still persists but has a
    //          broker-generated key; a null lookup returns null
    // -------------------------------------------------------------------------

    def "handleMessage() with null msgKey does not index under a null key"() {
        when: "send a record omitting the msg_key field entirely"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2002L,
            toJson([event_type: 'MESSAGE', data: [v: 2], topic: 'ci-null-topic']).bytes))

        then: "broker ACKs successfully"
        new PollingConditions(timeout: 5, delay: 0.1).eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 2002L
            }
        }

        and: "record is persisted in storage"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert storage.read('ci-null-topic', 0, 0, 10).size() == 1
        }

        and: "no index entry exists under a literal null key"
        // DataHandler assigns a generated key like 'key_<timestamp>' — the null slot is untouched.
        compactionIndex.getLatestOffsetAndTimestamp('ci-null-topic', null) == null
    }

    // -------------------------------------------------------------------------
    // Test 3 — second record for the same key supersedes the first
    // -------------------------------------------------------------------------

    def "second record for same key makes first record superseded"() {
        given:
        def conditions = new PollingConditions(timeout: 5, delay: 0.1)

        when: "send first record"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2003L,
            toJson([msg_key: 'ci-dup-key', event_type: 'MESSAGE', data: [v: 1], topic: 'ci-dup-topic']).bytes))
        conditions.eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 2003L
            }
        }

        and: "send second record for the same key"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2004L,
            toJson([msg_key: 'ci-dup-key', event_type: 'MESSAGE', data: [v: 2], topic: 'ci-dup-topic']).bytes))
        conditions.eventually {
            assert consumer.received.any {
                it.type == BrokerMessage.MessageType.ACK && it.messageId == 2004L
            }
        }

        then: "both records are in storage"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert storage.read('ci-dup-topic', 0, 0, 10).size() == 2
        }

        and: "offset 0 (first record) is superseded by the second"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert compactionIndex.isSuperseded('ci-dup-topic', 'ci-dup-key', 0L) == true
        }

        and: "offset 1 (second record) is the latest and not superseded"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert compactionIndex.isSuperseded('ci-dup-topic', 'ci-dup-key', 1L) == false
        }
    }
}
