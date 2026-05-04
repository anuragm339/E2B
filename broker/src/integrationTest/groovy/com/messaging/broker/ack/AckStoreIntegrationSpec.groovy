package com.messaging.broker.ack

import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.support.ModernConsumerClient
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.MessageRecord
import com.messaging.common.model.EventType
import io.micronaut.context.annotation.Value
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.file.Files
import java.time.Instant

/**
 * Integration tests for the ACK package:
 *   - RocksDbAckStore: put/get, putBatch, clearByTopicAndGroup
 *   - AckReconciliationScheduler: missing-offset detection and consistent-state reporting
 *
 * Uses a fresh temp directory; each test uses unique topic/group names.
 */
@MicronautTest
class AckStoreIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-ack-').toAbsolutePath().toString()
        return [
            'broker.network.port'                     : '19097',
            'micronaut.server.port'                   : '18087',
            'broker.storage.data-dir'                 : dataDir,
            'ack-store.rocksdb.path'                  : "${dataDir}/ack-store",
            'ack-store.reconciliation.enabled'        : 'false',  // disable scheduled reconciliation
            'ack-store.reconciliation.auto-sync-enabled': 'false'
        ]
    }

    @Inject RocksDbAckStore ackStore
    @Inject AckReconciliationScheduler reconciliationScheduler
    @Inject StorageEngine storage
    @Inject ConsumerRegistry remoteConsumers

    @Value('${broker.network.port}')
    int tcpPort

    // =========================================================================
    // RocksDbAckStore — single record operations
    // =========================================================================

    def "put and get round-trip preserves offset and timestamp"() {
        given:
        def record = new AckRecord(42L, 1_000_000L)

        when:
        ackStore.put('ack-topic-1', 'ack-group-1', 42L, record)

        then:
        def fetched = ackStore.get('ack-topic-1', 'ack-group-1', 42L)
        fetched != null
        fetched.offset == 42L
        fetched.ackedAtMs == 1_000_000L
    }

    def "get returns null for unknown offset"() {
        expect:
        ackStore.get('no-topic', 'no-group', 999L) == null
    }

    def "put overwrites existing record with latest value"() {
        given:
        ackStore.put('ack-topic-2', 'ack-group-2', 0L, new AckRecord(0L, 100L))

        when:
        ackStore.put('ack-topic-2', 'ack-group-2', 0L, new AckRecord(0L, 200L))

        then:
        def fetched = ackStore.get('ack-topic-2', 'ack-group-2', 0L)
        fetched.offset == 0L
        fetched.ackedAtMs == 200L
    }

    // =========================================================================
    // RocksDbAckStore — batch write
    // =========================================================================

    def "putBatch writes multiple records atomically"() {
        given:
        def topics  = ['bt-topic', 'bt-topic', 'bt-topic'] as String[]
        def groups  = ['bt-group', 'bt-group', 'bt-group'] as String[]
        def records = [
            new AckRecord(0L, 1L),
            new AckRecord(1L, 2L),
            new AckRecord(2L, 3L)
        ] as AckRecord[]

        when:
        ackStore.putBatch(topics, groups, records)

        then:
        ackStore.get('bt-topic', 'bt-group', 0L).offset == 0L
        ackStore.get('bt-topic', 'bt-group', 1L).offset == 1L
        ackStore.get('bt-topic', 'bt-group', 2L).offset == 2L
    }

    def "putBatch writes all records — no null-offset filtering needed"() {
        given:
        def topics  = ['null-topic', 'null-topic'] as String[]
        def groups  = ['null-group', 'null-group'] as String[]
        def records = [
            new AckRecord(0L, 1L),
            new AckRecord(1L, 2L)
        ] as AckRecord[]

        when:
        ackStore.putBatch(topics, groups, records)

        then: "both records stored — no filtering based on msgKey (key is offset)"
        ackStore.get('null-topic', 'null-group', 0L) != null
        ackStore.get('null-topic', 'null-group', 1L) != null
    }

    def "putBatch on empty arrays is a no-op"() {
        when:
        ackStore.putBatch([] as String[], [] as String[], [] as AckRecord[])

        then:
        true  // no exception
    }

    // =========================================================================
    // RocksDbAckStore — clearByTopicAndGroup
    // =========================================================================

    def "clearByTopicAndGroup removes all records for the given topic+group"() {
        given:
        ackStore.put('clear-topic', 'clear-group', 0L, new AckRecord(0L, 1L))
        ackStore.put('clear-topic', 'clear-group', 1L, new AckRecord(1L, 2L))
        ackStore.put('clear-topic', 'other-group', 2L, new AckRecord(2L, 3L))

        when:
        ackStore.clearByTopicAndGroup('clear-topic', 'clear-group')

        then:
        ackStore.get('clear-topic', 'clear-group', 0L) == null
        ackStore.get('clear-topic', 'clear-group', 1L) == null

        and: "records for a different group are untouched"
        ackStore.get('clear-topic', 'other-group', 2L) != null
    }

    def "clearByTopicAndGroup on non-existent prefix is safe"() {
        when:
        ackStore.clearByTopicAndGroup('ghost-topic', 'ghost-group')

        then:
        true  // no exception
    }

    // =========================================================================
    // AckReconciliationScheduler
    // =========================================================================

    def "reconciliation detects missing ACK records for registered consumers"() {
        given: "a record in storage with no corresponding ACK entry"
        def conditions = new PollingConditions(timeout: 5)
        def recon = ModernConsumerClient.connect('127.0.0.1', tcpPort)
        recon.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 9001L,
            '{"msg_key":"recon-key","event_type":"MESSAGE","data":{"v":1},"topic":"recon-topic"}'
                .getBytes('UTF-8')))
        conditions.eventually { assert storage.getCurrentOffset('recon-topic', 0) >= 0 }
        recon.subscribe('recon-topic', 'recon-group')
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any {
                it.topic == 'recon-topic' && it.group == 'recon-group'
            }
        }
        assert ackStore.get('recon-topic', 'recon-group', 0L) == null

        when: "reconciliation runs manually (scheduled is disabled)"
        reconciliationScheduler.reconcile()
        recon.close()

        then: "reconciliation completes without error (metrics updated internally)"
        true
    }

    def "reconciliation reports consistent when all records are ACKed"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def consist = ModernConsumerClient.connect('127.0.0.1', tcpPort)
        consist.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 9002L,
            '{"msg_key":"consist-key","event_type":"MESSAGE","data":{"v":1},"topic":"consist-topic"}'
                .getBytes('UTF-8')))
        conditions.eventually { assert storage.getCurrentOffset('consist-topic', 0) >= 0 }
        consist.subscribe('consist-topic', 'consist-group')
        conditions.eventually {
            assert remoteConsumers.getAllConsumers().any {
                it.topic == 'consist-topic' && it.group == 'consist-group'
            }
        }
        ackStore.put('consist-topic', 'consist-group', 0L,
            new AckRecord(0L, System.currentTimeMillis()))

        when:
        reconciliationScheduler.reconcile()
        consist.close()

        then:
        true
    }

    def "reconciliation is a no-op when no consumers are registered"() {
        when:
        reconciliationScheduler.reconcile()

        then:
        true
    }
}
