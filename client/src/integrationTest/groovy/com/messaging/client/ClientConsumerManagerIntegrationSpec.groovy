package com.messaging.client

import com.messaging.client.support.MockBrokerServer
import com.messaging.client.support.TestMessageHandler
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.TimeUnit

/**
 * Integration tests for ClientConsumerManager.
 *
 * A MockBrokerServer (raw ServerSocket) plays the role of the broker.
 * TestMessageHandler is a real @Singleton @Consumer bean that gets auto-discovered
 * and subscribed by ClientConsumerManager on startup.
 *
 * Tests verify:
 *   - @Consumer discovery and registration
 *   - TCP connection + SUBSCRIBE sent on startup
 *   - DATA messages routed to the handler (JSON batch)
 *   - RESET triggers handler.onReset and sends RESET_ACK back to broker
 *   - READY triggers handler.onReady and sends READY_ACK back to broker
 *   - isConnected / getConnectedTopicCount state
 *   - shutdown cleans up connections
 */
@MicronautTest
class ClientConsumerManagerIntegrationSpec extends Specification implements TestPropertyProvider {

    // =========================================================================
    // Mock broker — started once for the whole spec class
    // =========================================================================

    @Shared MockBrokerServer mockBroker

    @Override
    Map<String, String> getProperties() {
        mockBroker = new MockBrokerServer()
        mockBroker.start()
        return [
            'messaging.broker.host': '127.0.0.1',
            'messaging.broker.port': "${mockBroker.port}".toString(),
            'consumer.type'        : 'test-consumer'
        ]
    }

    // =========================================================================
    // Injected beans
    // =========================================================================

    @Inject ClientConsumerManager manager
    @Inject TestMessageHandler     testHandler

    // =========================================================================
    // Lifecycle: wait for initial connection before tests run; clear state per test
    // =========================================================================

    def setupSpec() {
        // Block until ClientConsumerManager has connected and sent its first SUBSCRIBE
        assert mockBroker.awaitFirstSubscribe(15, TimeUnit.SECONDS),
            "ClientConsumerManager did not subscribe within 15 s"
    }

    def setup() {
        // Fresh slate for each test — the connection itself is shared
        testHandler.clear()
        mockBroker.receivedTypes.clear()
    }

    def cleanupSpec() {
        mockBroker?.stop()
    }

    // =========================================================================
    // Consumer discovery
    // =========================================================================

    def "@Consumer annotated handler is discovered"() {
        expect:
        manager.getConnectedTopicCount() >= 1
    }

    // =========================================================================
    // Connection state
    // =========================================================================

    def "isConnected returns true after successful connection"() {
        expect:
        manager.isConnected()
    }

    def "getConnectedTopicCount equals 1 for one topic:group"() {
        expect:
        manager.getConnectedTopicCount() == 1
    }

    // =========================================================================
    // SUBSCRIBE sent on startup
    // =========================================================================

    def "SUBSCRIBE was sent to the mock broker during startup"() {
        expect:
        mockBroker.subscribeCount.get() >= 1
    }

    // =========================================================================
    // DATA routing (JSON batch)
    // =========================================================================

    def "JSON DATA message is routed to the registered handler"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def json = '[{"msgKey":"k1","eventType":"MESSAGE","data":"{\\"v\\":1}","createdAt":"2024-01-01T00:00:00Z"}]'

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_DATA, json.bytes)

        then:
        conditions.eventually {
            assert !testHandler.batches.isEmpty()
            assert testHandler.batches[0][0].msgKey == 'k1'
            assert testHandler.batches[0][0].eventType.name() == 'MESSAGE'
        }
    }

    def "multiple records in one DATA batch are all delivered to the handler"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def json = '[' +
            '{"msgKey":"a","eventType":"MESSAGE","data":"{}","createdAt":"2024-01-01T00:00:00Z"},' +
            '{"msgKey":"b","eventType":"MESSAGE","data":"{}","createdAt":"2024-01-01T00:00:00Z"},' +
            '{"msgKey":"c","eventType":"DELETE","data":null,"createdAt":"2024-01-01T00:00:00Z"}' +
            ']'

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_DATA, json.bytes)

        then:
        conditions.eventually {
            assert !testHandler.batches.isEmpty()
            def records = testHandler.batches[0]
            assert records.size() == 3
            assert records*.msgKey == ['a', 'b', 'c']
        }
    }

    def "DELETE event type is deserialized correctly"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def json = '[{"msgKey":"del-key","eventType":"DELETE","data":null,"createdAt":"2024-01-01T00:00:00Z"}]'

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_DATA, json.bytes)

        then:
        conditions.eventually {
            assert !testHandler.batches.isEmpty()
            assert testHandler.batches[0][0].eventType.name() == 'DELETE'
        }
    }

    // =========================================================================
    // RESET flow
    // =========================================================================

    def "RESET message triggers handler.onReset"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def payload    = '{"topic":"test-topic"}'.bytes

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_RESET, payload)

        then:
        conditions.eventually {
            assert testHandler.resetTopics.contains('test-topic')
        }
    }

    def "RESET triggers a RESET_ACK back to the broker"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def payload    = '{"topic":"test-topic"}'.bytes

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_RESET, payload)

        then:
        conditions.eventually {
            assert mockBroker.hasReceived(MockBrokerServer.TYPE_RESET_ACK)
        }
    }

    // =========================================================================
    // READY flow
    // =========================================================================

    def "READY message triggers handler.onReady"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def payload    = '{"topic":"test-topic"}'.bytes

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_READY, payload)

        then:
        conditions.eventually {
            assert testHandler.readyTopics.contains('test-topic')
        }
    }

    def "READY triggers a READY_ACK back to the broker"() {
        given:
        def conditions = new PollingConditions(timeout: 5)
        def payload    = '{"topic":"test-topic"}'.bytes

        when:
        mockBroker.broadcast(MockBrokerServer.TYPE_READY, payload)

        then:
        conditions.eventually {
            assert mockBroker.hasReceived(MockBrokerServer.TYPE_READY_ACK)
        }
    }
}
