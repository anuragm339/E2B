package com.messaging.broker.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.support.ModernConsumerHarness
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import io.micronaut.context.annotation.Value
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration tests for consumer package internals:
 *   - ConsumerOffsetTracker  — updateOffset / getOffset / resetOffset / flush
 *   - RefreshStateMachine    — valid and invalid state transitions
 *   - WatermarkGatePolicy    — allow when new data, deny when at head
 *   - TopicFairScheduler     — per-topic semaphore, in-flight limits
 *   - AdaptiveBatchDeliveryManager — start idempotency, delivery enabled after READY_ACK
 */
@MicronautTest
class ConsumerInternalsIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-consumer-').toAbsolutePath().toString()
        return [
            'broker.network.port'    : '19099',
            'micronaut.server.port'  : '18089',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject ConsumerOffsetTracker offsetTracker
    @Inject RefreshStateMachine    stateMachine
    @Inject WatermarkGatePolicy    watermarkGate
    @Inject TopicFairScheduler     fairScheduler
    @Inject AdaptiveBatchDeliveryManager deliveryManager
    @Inject ConsumerRegistry       remoteConsumers
    @Inject StorageEngine          storage

    @Value('${broker.network.port}')
    int tcpPort

    private static final ObjectMapper MAPPER = new ObjectMapper()

    // =========================================================================
    // ConsumerOffsetTracker
    // =========================================================================

    def "updateOffset and getOffset round-trip"() {
        when:
        offsetTracker.updateOffset('ot-group:ot-topic', 42L)

        then:
        offsetTracker.getOffset('ot-group:ot-topic') == 42L
    }

    def "getOffset returns 0 for unknown consumer"() {
        expect:
        offsetTracker.getOffset('unknown-group:unknown-topic') == 0L
    }

    def "updateOffset overwrites previous value"() {
        given:
        offsetTracker.updateOffset('ot-group2:ot-topic2', 10L)

        when:
        offsetTracker.updateOffset('ot-group2:ot-topic2', 99L)

        then:
        offsetTracker.getOffset('ot-group2:ot-topic2') == 99L
    }

    def "resetOffset writes value and triggers immediate flush"() {
        when:
        offsetTracker.resetOffset('reset-group:reset-topic', -1L)

        then: "value is immediately readable (flush completed synchronously)"
        offsetTracker.getOffset('reset-group:reset-topic') == -1L
    }

    def "multiple consumers tracked independently"() {
        when:
        offsetTracker.updateOffset('g1:topic-a', 100L)
        offsetTracker.updateOffset('g2:topic-a', 200L)
        offsetTracker.updateOffset('g1:topic-b', 300L)

        then:
        offsetTracker.getOffset('g1:topic-a') == 100L
        offsetTracker.getOffset('g2:topic-a') == 200L
        offsetTracker.getOffset('g1:topic-b') == 300L
    }

    // =========================================================================
    // RefreshStateMachine
    // =========================================================================

    def "valid forward transitions are allowed"() {
        expect:
        stateMachine.isValidTransition(RefreshState.RESET_SENT,  RefreshState.REPLAYING)
        stateMachine.isValidTransition(RefreshState.REPLAYING,   RefreshState.READY_SENT)
        stateMachine.isValidTransition(RefreshState.READY_SENT,  RefreshState.COMPLETED)
    }

    def "any state can transition to ABORTED"() {
        expect:
        stateMachine.isValidTransition(RefreshState.RESET_SENT, RefreshState.ABORTED)
        stateMachine.isValidTransition(RefreshState.REPLAYING,  RefreshState.ABORTED)
        stateMachine.isValidTransition(RefreshState.READY_SENT, RefreshState.ABORTED)
    }

    def "idempotent self-transitions are valid"() {
        expect:
        stateMachine.isValidTransition(RefreshState.RESET_SENT, RefreshState.RESET_SENT)
        stateMachine.isValidTransition(RefreshState.REPLAYING,  RefreshState.REPLAYING)
        stateMachine.isValidTransition(RefreshState.READY_SENT, RefreshState.READY_SENT)
        stateMachine.isValidTransition(RefreshState.COMPLETED,  RefreshState.COMPLETED)
        stateMachine.isValidTransition(RefreshState.ABORTED,    RefreshState.ABORTED)
    }

    def "invalid backward and skip transitions are rejected"() {
        expect:
        !stateMachine.isValidTransition(RefreshState.REPLAYING,  RefreshState.RESET_SENT)
        !stateMachine.isValidTransition(RefreshState.READY_SENT, RefreshState.REPLAYING)
        !stateMachine.isValidTransition(RefreshState.COMPLETED,  RefreshState.RESET_SENT)
        !stateMachine.isValidTransition(RefreshState.ABORTED,    RefreshState.COMPLETED)
        !stateMachine.isValidTransition(RefreshState.RESET_SENT, RefreshState.COMPLETED) // skip REPLAYING
    }

    def "COMPLETED and ABORTED are terminal states"() {
        expect:
        stateMachine.isTerminalState(RefreshState.COMPLETED)
        stateMachine.isTerminalState(RefreshState.ABORTED)
        !stateMachine.isTerminalState(RefreshState.RESET_SENT)
        !stateMachine.isTerminalState(RefreshState.REPLAYING)
        !stateMachine.isTerminalState(RefreshState.READY_SENT)
    }

    def "getNextState follows the workflow sequence"() {
        expect:
        stateMachine.getNextState(RefreshState.RESET_SENT) == RefreshState.REPLAYING
        stateMachine.getNextState(RefreshState.REPLAYING)  == RefreshState.READY_SENT
        stateMachine.getNextState(RefreshState.READY_SENT) == RefreshState.COMPLETED
    }

    def "transition() returns success for valid transitions"() {
        when:
        def result = stateMachine.transition(RefreshState.RESET_SENT, RefreshState.REPLAYING)

        then:
        result.isSuccess()
    }

    def "transition() returns failure for invalid transitions"() {
        when:
        def result = stateMachine.transition(RefreshState.COMPLETED, RefreshState.RESET_SENT)

        then:
        !result.isSuccess()
    }

    // =========================================================================
    // WatermarkGatePolicy (exercised via live storage + registered consumer)
    // =========================================================================

    def "WatermarkGate allows delivery when storage is ahead of consumer"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def client = ModernConsumerHarness.connect('127.0.0.1', tcpPort)
        try {
            // Write a record so storageOffset >= 0
            client.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 5001L,
                toJson([msg_key: 'wm-key', event_type: 'MESSAGE',
                        data: [v: 1], topic: 'wm-topic']).bytes))
            conditions.eventually { assert storage.getCurrentOffset('wm-topic', 0) >= 0 }

            // Subscribe so consumer is registered at offset 0
            client.subscribe('wm-topic', 'wm-group')
            conditions.eventually {
                assert remoteConsumers.getAllConsumers().any {
                    it.topic == 'wm-topic' && it.group == 'wm-group'
                }
            }

            def consumer = remoteConsumers.getAllConsumers()
                .find { it.topic == 'wm-topic' && it.group == 'wm-group' }

            // Consumer starts below storage head → gate should allow
            expect:
            watermarkGate.shouldDeliver(consumer).isAllowed()
        } finally {
            client.close()
        }
    }

    // =========================================================================
    // TopicFairScheduler
    // =========================================================================

    def "TopicFairScheduler executes tasks and releases permits"() {
        given:
        def latch = new CountDownLatch(3)

        when:
        3.times { i ->
            fairScheduler.schedule("fs-topic-${i}", { latch.countDown() }, 0, TimeUnit.MILLISECONDS)
        }

        then:
        latch.await(3, TimeUnit.SECONDS)
    }

    def "TopicFairScheduler getAvailablePermits returns max when topic is idle"() {
        expect: "unknown topic returns maxInFlightPerTopic (default=1)"
        fairScheduler.getAvailablePermits('idle-topic') >= 1
    }

    def "TopicFairScheduler getInFlightCount returns 0 when idle"() {
        expect:
        fairScheduler.getInFlightCount('never-scheduled-topic') == 0
    }

    def "TopicFairScheduler tasks for different topics run concurrently"() {
        given:
        def counter = new AtomicInteger(0)
        def latch   = new CountDownLatch(4)

        when: "schedule tasks on 4 distinct topics simultaneously"
        ['ts-a', 'ts-b', 'ts-c', 'ts-d'].each { topic ->
            fairScheduler.schedule(topic, {
                counter.incrementAndGet()
                latch.countDown()
            }, 0, TimeUnit.MILLISECONDS)
        }

        then: "all 4 tasks complete"
        latch.await(5, TimeUnit.SECONDS)
        counter.get() == 4
    }

    // =========================================================================
    // AdaptiveBatchDeliveryManager
    // =========================================================================

    def "AdaptiveBatchDeliveryManager start is idempotent"() {
        when: "calling start twice does not throw"
        deliveryManager.start()
        deliveryManager.start()

        then:
        true
    }

    def "AdaptiveBatchDeliveryManager delivers data after READY_ACK"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def client = ModernConsumerHarness.connect('127.0.0.1', tcpPort)

        // Publish a record
        client.send(new BrokerMessage(BrokerMessage.MessageType.DATA, 6001L,
            toJson([msg_key: 'adm-key', event_type: 'MESSAGE',
                    data: [v: 1], topic: 'adm-topic']).bytes))
        conditions.eventually { assert storage.getCurrentOffset('adm-topic', 0) >= 0 }

        // Subscribe and wait for startup READY
        client.subscribe('adm-topic', 'adm-group')
        conditions.eventually {
            assert client.received.any { it.type == BrokerMessage.MessageType.READY }
        }

        when: "consumer sends READY_ACK to enable adaptive delivery"
        client.sendReadyAck('adm-topic', 'adm-group')

        then: "DATA batch delivered by adaptive delivery manager"
        conditions.eventually {
            assert client.received.any { it.type == BrokerMessage.MessageType.DATA }
        }

        cleanup:
        client.close()
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static String toJson(Map<String, ?> map) {
        MAPPER.writeValueAsString(map)
    }
}
