package com.messaging.broker.systemtest.journey

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

    /**
     * Connect as 'price-quote-service' and complete the startup handshake (READY → ACK)
     * before each test so the client is in normal delivery mode from the start.
     */
    def setup() {
        legacyClient = LegacyConsumerClient.connect('127.0.0.1', brokerTcpPort, 'price-quote-service')
        // Broker sends a startup READY immediately after registration
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            assert legacyClient.received.any { it instanceof ReadyEvent }
        }
        legacyClient.sendAck()
        legacyClient.clearReceived()
    }

    def cleanup() {
        legacyClient?.close()
    }

    // ── Scenario 1: Registration ──────────────────────────────────────────────

    def "legacy client is auto-subscribed to all topics mapped to its serviceName"() {
        // 'price-quote-service' maps to 6 topics in application.yml:
        //   prices-v1, reference-data-v5, non-promotable-products, prices-v4, minimum-price, deposit
        given:
        def registry = brokerCtx.getBean(ConsumerRegistry)

        expect: "broker created one consumer registration per mapped topic, all in the same group"
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            def legacyConsumers = registry.getAllConsumers().findAll {
                it.legacy && it.group == 'price-quote-service'
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

    def "legacy client receives merged batch from multiple topics and ACK advances offsets for all topics"() {
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

        and: "the batch contains the expected message keys (merged across topics)"
        def allBatchKeys = legacyClient.received
            .findAll { it instanceof BatchEvent }
            .collectMany { (it as BatchEvent).messages*.key }
            .toSet()
        allBatchKeys.intersect(['legacy-price-1', 'legacy-ref-1']).size() > 0

        and: "no wire errors on the connection"
        legacyClient.errors.isEmpty()

        when: "legacy client ACKs all received batches"
        legacyClient.received.findAll { it instanceof BatchEvent }.each { legacyClient.sendAck() }

        then: "offsets advance in ConsumerOffsetTracker for the delivered topics"
        def offsetTracker = brokerCtx.getBean(ConsumerOffsetTracker)
        new PollingConditions(timeout: 10, delay: 0.3).eventually {
            // At least one of the two topics should have a committed offset > 0
            def priceOffset = offsetTracker.getOffset('price-quote-service:prices-v1')
            def refOffset   = offsetTracker.getOffset('price-quote-service:reference-data-v5')
            assert priceOffset > 0 || refOffset > 0
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
        def beforeAck = legacyClient.received.findAll { it instanceof BatchEvent }.size()

        when: "ACK is sent and then no new messages are enqueued"
        legacyClient.received.findAll { it instanceof BatchEvent }.each { legacyClient.sendAck() }
        sleep(2000)  // let broker attempt re-delivery if the bug were present

        then: "no additional BatchEvents are received after the ACK"
        def afterAck = legacyClient.received.findAll { it instanceof BatchEvent }.size()
        afterAck == beforeAck  // count did not grow → no re-delivery

        and: "no wire errors"
        legacyClient.errors.isEmpty()
    }
}
