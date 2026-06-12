package com.messaging.broker.systemtest.journey

import com.messaging.broker.ack.RocksDbAckStore
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.LegacyConsumerClient
import com.messaging.network.legacy.events.BatchEvent
import com.messaging.network.legacy.events.ReadyEvent
import spock.util.concurrent.PollingConditions

/**
 * Journey: legacy client connects via the Event wire protocol (RegisterEvent + serviceName),
 * is auto-subscribed to all topics mapped to that service, receives a merged multi-topic
 * batch, and ACKs correctly.
 *
 * The broker auto-detects the legacy protocol via ProtocolDetectionDecoder (first byte is
 * EventType.REGISTER ordinal = 0, outside the modern BrokerMessage code range).  It then:
 *  1. Looks up the serviceName in legacy-clients.service-topics config.
 *  2. Registers one consumer per topic (all sharing the same group = serviceName).
 *  3. Sends a startup READY — the client ACKs to enter normal delivery mode.
 *
 * Delivery is handled by LegacyConsumerDeliveryManager (k-way merge across topics).
 * ACK is a generic AckEvent; LegacyConnectionState resolves it to the correct typed ACK
 * (BATCH_ACK) and advances offsets for all delivered topics.
 *
 * This spec uses LegacyConsumerClient directly (raw TCP, no Micronaut consumer context)
 * because the legacy code path completely bypasses ClientConsumerManager and the
 * @Consumer framework that TestRecordCollector hooks into.
 */
class LegacyConsumerJourneySpec extends BrokerSystemTestSupport {

    LegacyConsumerClient legacyClient

    @Override
    protected String defaultTopic() { '__unused_legacy_only__' }

    /**
     * Connect as 'price-quote' and complete the startup handshake (READY → ACK)
     * before each test so the client is in normal delivery mode from the start.
     */
    def setup() {
        legacyClient = LegacyConsumerClient.connect('127.0.0.1', brokerTcpPort, 'price-quote')
        // Broker sends a startup READY immediately after registration
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert legacyClient.received.any { it instanceof ReadyEvent }
        }
        legacyClient.sendAck()
        def registry = brokerCtx.getBean(ConsumerRegistry)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            def legacyConsumers = registry.getAllConsumers().findAll {
                it.legacy && it.group == 'price-quote'
            }
            assert !legacyConsumers.isEmpty()
            assert legacyConsumers.every { registry.isLegacyConsumerReady(it.clientId) }
        }
        legacyClient.clearReceived()
    }

    def cleanup() {
        legacyClient?.close()
    }

    // ── Scenario 1: Registration ──────────────────────────────────────────────

    def "legacy client is auto-subscribed to all topics mapped to its serviceName"() {
        // 'price-quote' maps to 6 topics in application.yml:
        //   prices-v1, reference-data-v5, non-promotable-products, prices-v4, minimum-price, deposit
        given:
        def registry = brokerCtx.getBean(ConsumerRegistry)

        expect: "broker created one consumer registration per mapped topic, all in the same group"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            def legacyConsumers = registry.getAllConsumers().findAll {
                it.legacy && it.group == 'price-quote'
            }
            assert legacyConsumers.size() >= 6
            def topics = legacyConsumers*.topic.toSet()
            assert topics.containsAll(['prices-v1', 'reference-data-v5', 'non-promotable-products',
                                       'prices-v4', 'minimum-price', 'deposit'])
        }

        and: "no wire errors on the legacy connection"
        legacyClient.errors.isEmpty()
    }

    // ── Scenario 2: Merged batch delivery across two topics ───────────────────

    def "legacy client receives batches from multiple topics and ACK advances offsets for all topics"() {
        given: "one message is enqueued on prices-v1 and one on reference-data-v5"
        cloudServer.enqueueMessages([
            [offset: 1L, topic: 'prices-v1', partition: 0,
             msgKey: 'legacy-price-1', eventType: 'MESSAGE', data: '{"price":100}'],
            [offset: 1L, topic: 'reference-data-v5', partition: 0,
             msgKey: 'legacy-ref-1', eventType: 'MESSAGE', data: '{"ref":"abc"}'],
        ])

        when: "broker polls, stores, and delivers merged batch to legacy consumer"
        // LegacyConsumerDeliveryManager k-way merges across all 6 subscribed topics

        then: "legacy client receives at least one BatchEvent"
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert legacyClient.received.any { it instanceof BatchEvent }
        }

        and: "ACKing each observed batch lets delivery continue until both topic messages arrive"
        int acknowledgedBatches = 0
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            def batches = legacyClient.received.findAll { it instanceof BatchEvent }
            while (acknowledgedBatches < batches.size()) {
                legacyClient.sendAck()
                acknowledgedBatches++
            }
            def allBatchKeys = batches
                .collectMany { (it as BatchEvent).messages*.key }
                .toSet()
            assert allBatchKeys.contains('legacy-price-1')
            assert allBatchKeys.contains('legacy-ref-1')
        }

        and: "no wire errors on the connection"
        legacyClient.errors.isEmpty()

        and: "offsets advance in ConsumerOffsetTracker for all delivered topics"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            def priceOffset = offsetTracker.getOffset('price-quote:prices-v1')
            def refOffset   = offsetTracker.getOffset('price-quote:reference-data-v5')
            assert priceOffset > 0
            assert refOffset > 0
        }

        and: "RocksDB has ACK entries for both delivered legacy topic offsets"
        def ackStore = brokerCtx.getBean(RocksDbAckStore)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert ackStore.get('prices-v1', 'price-quote', 1L) != null
            assert ackStore.get('reference-data-v5', 'price-quote', 1L) != null
        }
    }

    // ── Scenario 3: No re-delivery after ACK ─────────────────────────────────

    def "legacy client does not receive duplicate messages after ACK"() {
        given: "one message is enqueued and delivered"
        cloudServer.enqueueMessages([
            [offset: 2L, topic: 'prices-v1', partition: 0,
             msgKey: 'legacy-dedup-1', eventType: 'MESSAGE', data: '{"x":1}'],
        ])

        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert legacyClient.received.any { it instanceof BatchEvent }
        }
        def beforeAck = legacyClient.received
            .findAll { it instanceof BatchEvent }
            .collectMany { (it as BatchEvent).messages*.key }
            .count { it == 'legacy-dedup-1' }

        when: "ACK is sent and then no new messages are enqueued"
        legacyClient.received.findAll { it instanceof BatchEvent }.each { legacyClient.sendAck() }
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert offsetTracker.getOffset('price-quote:prices-v1') >= 2L
        }
        waitForStableValue(2000) {
            legacyClient.received
                .findAll { it instanceof BatchEvent }
                .collectMany { (it as BatchEvent).messages*.key }
                .count { it == 'legacy-dedup-1' }
        }

        then: "no additional BatchEvents are received after the ACK"
        def afterAck = legacyClient.received
            .findAll { it instanceof BatchEvent }
            .collectMany { (it as BatchEvent).messages*.key }
            .count { it == 'legacy-dedup-1' }
        afterAck == beforeAck  // count did not grow → no re-delivery

        and: "no wire errors"
        legacyClient.errors.isEmpty()
    }
}
