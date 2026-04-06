package com.messaging.broker.ack

import com.messaging.broker.consumer.ConsumerRegistrationService
import com.messaging.broker.consumer.RemoteConsumer
import com.messaging.broker.monitoring.BrokerMetrics
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant

class AckReconciliationSchedulerSpec extends Specification {

    ConsumerRegistrationService registrationService = Mock()
    StorageEngine storage = Mock()
    RocksDbAckStore ackStore = Mock()
    BrokerMetrics metrics = Mock()

    AckReconciliationScheduler scheduler

    def makeConsumer(String topic, String group) {
        def c = Mock(RemoteConsumer)
        c.getTopic() >> topic
        c.getGroup() >> group
        return c
    }

    def makeMsg(long offset, String topic, String msgKey) {
        new MessageRecord(offset, topic, 0, msgKey, null, null, Instant.now())
    }

    def setup() {
        scheduler = new AckReconciliationScheduler(
                registrationService, storage, ackStore, metrics, true, false)
    }

    def "reconcile counts missing keys correctly when some msgKeys absent from RocksDB"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        storage.getCurrentOffset("prices-v1", 0) >> 3L
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", "key-A"),
                makeMsg(1L, "prices-v1", "key-B"),
                makeMsg(2L, "prices-v1", "key-C")
        ]
        storage.read("prices-v1", 0, 3L, 500) >> []

        // key-B has no ACK record
        ackStore.get("prices-v1", "group1", "key-A") >> new AckRecord(0L, 1000L)
        ackStore.get("prices-v1", "group1", "key-B") >> null
        ackStore.get("prices-v1", "group1", "key-C") >> new AckRecord(2L, 1000L)

        when:
        scheduler.reconcile()

        then:
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 1L)
    }

    def "reconcile reports zero missing when all msgKeys are present"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        storage.getCurrentOffset("prices-v1", 0) >> 2L
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", "key-A"),
                makeMsg(1L, "prices-v1", "key-B")
        ]
        storage.read("prices-v1", 0, 2L, 500) >> []

        ackStore.get("prices-v1", "group1", "key-A") >> new AckRecord(0L, 1000L)
        ackStore.get("prices-v1", "group1", "key-B") >> new AckRecord(1L, 1001L)

        when:
        scheduler.reconcile()

        then:
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 0L)
    }

    def "reconcile skips active segment (stops at headOffset boundary)"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        storage.getCurrentOffset("prices-v1", 0) >> 2L  // headOffset = 2 (active segment starts here)

        // Only msgs with offset < 2 are in sealed segments; offset=2 is in active segment
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", "key-A"),
                makeMsg(1L, "prices-v1", "key-B"),
                makeMsg(2L, "prices-v1", "key-C")  // offset >= headOffset → stop
        ]

        ackStore.get("prices-v1", "group1", "key-A") >> new AckRecord(0L, 1000L)
        ackStore.get("prices-v1", "group1", "key-B") >> null

        when:
        scheduler.reconcile()

        then:
        // key-C should NOT be checked (it's in the active segment)
        0 * ackStore.get("prices-v1", "group1", "key-C")
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 1L)
    }

    def "reconcile is a no-op when enabled=false"() {
        given:
        scheduler = new AckReconciliationScheduler(
                registrationService, storage, ackStore, metrics, false, false)

        when:
        scheduler.reconcile()

        then:
        0 * registrationService.getAllConsumers()
        0 * metrics.updateReconciliationMissingKeys(*_)
    }

    def "reconcile skips null msgKeys without error"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        storage.getCurrentOffset("prices-v1", 0) >> 2L
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", null),   // null msgKey — skip
                makeMsg(1L, "prices-v1", "key-B")
        ]
        storage.read("prices-v1", 0, 2L, 500) >> []
        ackStore.get("prices-v1", "group1", "key-B") >> new AckRecord(1L, 1000L)

        when:
        scheduler.reconcile()

        then:
        noExceptionThrown()
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 0L)
    }
}
