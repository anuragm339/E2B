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
 * All tests use unique topic names to prevent state bleed.
 * Structure: send message in when:, assert everything (ACK + storage + index) in then:.
 */
@MicronautTest
class CompactionIndexWiringIntegrationSpec extends BrokerHandlerSpecSupport {

    @Override
    Map<String, String> getProperties() {
        def base = super.getProperties()
        def dataDir = base['broker.storage.data-dir']
        return base + [
            'broker.network.port'   : '19102',
            'micronaut.server.port' : '18092',
            'compaction.rocksdb.path': "${dataDir}/compaction-index"
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

        then: "ACK is returned and both storage and index are updated"
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2001L }
            assert storage.read('ci-topic', 0, 0, 10).size() == 1
            assert compactionIndex.getLatestOffsetAndTimestamp('ci-topic', 'ci-key-1') != null
        }
    }

    // -------------------------------------------------------------------------
    // Test 2 — record without explicit msg_key; null key must not be indexed
    // -------------------------------------------------------------------------

    def "handleMessage() with null msgKey does not index under a null key"() {
        when:
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2002L,
            toJson([event_type: 'MESSAGE', data: [v: 2], topic: 'ci-null-topic']).bytes))

        then: "ACK is returned and record is persisted"
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2002L }
            assert storage.read('ci-null-topic', 0, 0, 10).size() == 1
        }

        and: "no entry exists under a literal null key"
        compactionIndex.getLatestOffsetAndTimestamp('ci-null-topic', null) == null
    }

    // -------------------------------------------------------------------------
    // Test 3 — two records for same key: first becomes superseded
    // -------------------------------------------------------------------------

    def "second record for same key makes first record superseded"() {
        when: "send first record and wait for its ACK before sending the second"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2003L,
            toJson([msg_key: 'ci-dup-key', event_type: 'MESSAGE', data: [v: 1], topic: 'ci-dup-topic']).bytes))
        // Wait for first ACK before sending second — ensures ordered storage
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2003L }
        }

        and: "send second record for the same key"
        consumer.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 2004L,
            toJson([msg_key: 'ci-dup-key', event_type: 'MESSAGE', data: [v: 2], topic: 'ci-dup-topic']).bytes))

        then: "both records stored and second ACK received"
        new PollingConditions(timeout: 10, delay: 0.2).eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2004L }
            assert storage.read('ci-dup-topic', 0, 0, 10).size() == 2
        }

        and: "offset 0 (first record) is now superseded"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert compactionIndex.isSuperseded('ci-dup-topic', 'ci-dup-key', 0L) == true
        }

        and: "offset 1 (second record) is the latest — not superseded"
        new PollingConditions(timeout: 5, delay: 0.2).eventually {
            assert compactionIndex.isSuperseded('ci-dup-topic', 'ci-dup-key', 1L) == false
        }
    }
}
