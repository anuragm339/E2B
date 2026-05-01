package com.messaging.broker.ack

import com.messaging.broker.consumer.ConsumerOffsetTracker
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
    ConsumerOffsetTracker offsetTracker = Mock()

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
                registrationService, storage, ackStore, metrics, offsetTracker, true, false)
    }

    def "reconcile counts missing offsets correctly when some are absent from RocksDB"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        offsetTracker.getOffset("group1:prices-v1") >> 3L
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", "key-A"),
                makeMsg(1L, "prices-v1", "key-B"),
                makeMsg(2L, "prices-v1", "key-C")
        ]

        // offset 1 has no ACK entry
        ackStore.get("prices-v1", "group1", 0L) >> new AckRecord(0L, 1000L)
        ackStore.get("prices-v1", "group1", 1L) >> null
        ackStore.get("prices-v1", "group1", 2L) >> new AckRecord(2L, 1000L)

        when:
        scheduler.reconcile()

        then:
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 1L)
    }

    def "reconcile reports zero missing when all offsets have ACK entries"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        offsetTracker.getOffset("group1:prices-v1") >> 2L
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", "key-A"),
                makeMsg(1L, "prices-v1", "key-B")
        ]

        ackStore.get("prices-v1", "group1", 0L) >> new AckRecord(0L, 1000L)
        ackStore.get("prices-v1", "group1", 1L) >> new AckRecord(1L, 1001L)

        when:
        scheduler.reconcile()

        then:
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 0L)
    }

    def "reconcile stops at committedOffset boundary (does not check in-flight records)"() {
        given:
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        offsetTracker.getOffset("group1:prices-v1") >> 2L  // committedOffset = 2

        // Storage returns records at 0, 1, 2 — but offset 2 is at/beyond committedOffset
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", "key-A"),
                makeMsg(1L, "prices-v1", "key-B"),
                makeMsg(2L, "prices-v1", "key-C")  // offset >= committedOffset → stop
        ]

        ackStore.get("prices-v1", "group1", 0L) >> new AckRecord(0L, 1000L)
        ackStore.get("prices-v1", "group1", 1L) >> null

        when:
        scheduler.reconcile()

        then:
        // offset 2 should NOT be checked (it's at/beyond committedOffset)
        0 * ackStore.get("prices-v1", "group1", 2L)
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 1L)
    }

    def "reconcile is a no-op when enabled=false"() {
        given:
        scheduler = new AckReconciliationScheduler(
                registrationService, storage, ackStore, metrics, offsetTracker, false, false)

        when:
        scheduler.reconcile()

        then:
        0 * registrationService.getAllConsumers()
        0 * metrics.updateReconciliationMissingKeys(*_)
    }

    def "reconcile checks all records by offset regardless of msgKey"() {
        given: "a null-msgKey record at offset 0 is now checked (no null-key skip in offset-based schema)"
        registrationService.getAllConsumers() >> [makeConsumer("prices-v1", "group1")]
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        offsetTracker.getOffset("group1:prices-v1") >> 2L
        storage.read("prices-v1", 0, 0L, 500) >> [
                makeMsg(0L, "prices-v1", null),   // null msgKey — still checked by offset
                makeMsg(1L, "prices-v1", "key-B")
        ]
        ackStore.get("prices-v1", "group1", 0L) >> new AckRecord(0L, 999L)   // null-key record found
        ackStore.get("prices-v1", "group1", 1L) >> new AckRecord(1L, 1000L)  // key-B found

        when:
        scheduler.reconcile()

        then:
        noExceptionThrown()
        1 * metrics.updateReconciliationMissingKeys("prices-v1", "group1", 0L)
    }
}
