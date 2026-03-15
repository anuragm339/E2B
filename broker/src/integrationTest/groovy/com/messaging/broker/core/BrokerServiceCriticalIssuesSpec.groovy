package com.messaging.broker.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import com.messaging.network.tcp.NettyTcpClient
import io.micronaut.context.ApplicationContext
import spock.lang.Specification
import spock.lang.TempDir
import spock.util.concurrent.PollingConditions

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

/**
 * Integration tests for critical broker issues:
 * - Issue #1: Startup delivery race condition
 * - Issue #2: COMMIT_OFFSET validation
 * - Issue #3: Error handling with connection closure
 */
class BrokerServiceCriticalIssuesSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @TempDir
    Path tempDir

    // ========================================================================
    // ISSUE #3 TESTS: Error Handling - Connection Closure on Validation Errors
    // ========================================================================

    def "SUBSCRIBE with missing topic closes connection"() {
        given: "a running broker"
        def harness = AppHarness.start(tempDir)

        when: "client sends SUBSCRIBE with missing topic field"
        def payload = toJson([
            group: "test-group"
            // missing topic field
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 1001L, payload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        then: "connection is closed (no ACK received)"
        def conditions = new PollingConditions(timeout: 3, delay: 0.1)
        conditions.eventually {
            // Connection should be closed, so sending another message should fail
            def shouldFail = false
            try {
                def testMsg = new BrokerMessage(BrokerMessage.MessageType.HEARTBEAT, 9999L, new byte[0])
                harness.connection.send(testMsg).get(500, TimeUnit.MILLISECONDS)
            } catch (Exception e) {
                shouldFail = true
            }
            assert shouldFail || !harness.connection.isConnected()
        }

        and: "no ACK is sent"
        assert harness.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1001L } == null

        cleanup:
        harness.shutdown()
    }

    def "SUBSCRIBE with null group closes connection"() {
        given: "a running broker"
        def harness = AppHarness.start(tempDir)

        when: "client sends SUBSCRIBE with null group field"
        def payload = '{"topic":"test-topic","group":null}'
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 1002L, payload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        then: "connection is closed"
        def conditions = new PollingConditions(timeout: 3, delay: 0.1)
        conditions.eventually {
            assert !harness.connection.isConnected()
        }

        and: "no ACK is sent"
        assert harness.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1002L } == null

        cleanup:
        harness.shutdown()
    }

    def "COMMIT_OFFSET with missing topic closes connection"() {
        given: "a running broker with a subscribed consumer"
        def harness = AppHarness.start(tempDir)

        when: "client sends COMMIT_OFFSET with missing topic field"
        def payload = toJson([
            group : "test-group",
            offset: 100
            // missing topic field
        ])
        def commit = new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 2001L, payload.getBytes(StandardCharsets.UTF_8))
        harness.send(commit)

        then: "connection is closed"
        def conditions = new PollingConditions(timeout: 3, delay: 0.1)
        conditions.eventually {
            assert !harness.connection.isConnected()
        }

        cleanup:
        harness.shutdown()
    }

    def "DATA with missing topic closes connection"() {
        given: "a running broker"
        def harness = AppHarness.start(tempDir)

        when: "client sends DATA with missing topic field"
        def payload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"]
            // missing topic field
        ])
        def message = new BrokerMessage(BrokerMessage.MessageType.DATA, 3001L, payload.getBytes(StandardCharsets.UTF_8))
        harness.send(message)

        then: "connection is closed"
        def conditions = new PollingConditions(timeout: 3, delay: 0.1)
        conditions.eventually {
            assert !harness.connection.isConnected()
        }

        and: "no ACK is sent"
        assert harness.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 3001L } == null

        cleanup:
        harness.shutdown()
    }

    // ========================================================================
    // ISSUE #2 TESTS: COMMIT_OFFSET Validation
    // ========================================================================

    def "COMMIT_OFFSET with negative offset closes connection"() {
        given: "a running broker with data"
        def harness = AppHarness.start(tempDir)

        // Store some data first
        def dataPayload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"],
            topic     : "test-topic"
        ])
        def dataMsg = new BrokerMessage(BrokerMessage.MessageType.DATA, 4001L, dataPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(dataMsg)

        // Wait for storage
        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.storage.getCurrentOffset("test-topic", 0) > 0
        }

        when: "client sends COMMIT_OFFSET with negative offset"
        def commitPayload = toJson([
            topic : "test-topic",
            group : "test-group",
            offset: -5
        ])
        def commit = new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 4002L, commitPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(commit)

        then: "connection is closed"
        conditions.eventually {
            assert !harness.connection.isConnected()
        }

        cleanup:
        harness.shutdown()
    }

    def "COMMIT_OFFSET exceeding storage head is clamped"() {
        given: "a running broker with data"
        def harness = AppHarness.start(tempDir)

        // Store some data
        def dataPayload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"],
            topic     : "test-topic"
        ])
        def dataMsg = new BrokerMessage(BrokerMessage.MessageType.DATA, 5001L, dataPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(dataMsg)

        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.storage.getCurrentOffset("test-topic", 0) > 0
        }

        when: "client sends COMMIT_OFFSET with offset exceeding storage head"
        long storageHead = harness.storage.getCurrentOffset("test-topic", 0)
        long impossibleOffset = storageHead + 1000

        def commitPayload = toJson([
            topic : "test-topic",
            group : "test-group",
            offset: impossibleOffset
        ])
        def commit = new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 5002L, commitPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(commit)

        then: "offset is clamped to storage head"
        conditions.eventually {
            def persistedOffset = harness.offsetTracker.getOffset("test-group:test-topic")
            assert persistedOffset == storageHead
        }

        and: "ACK is still sent (no connection closure)"
        conditions.eventually {
            assert harness.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 5002L } != null
        }

        cleanup:
        harness.shutdown()
    }

    def "registerConsumer with restored offset exceeding storage clamps to storage head"() {
        given: "a broker with persisted offset exceeding storage"
        def harness = AppHarness.start(tempDir)

        // Store one message
        def dataPayload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"],
            topic     : "test-topic"
        ])
        def dataMsg = new BrokerMessage(BrokerMessage.MessageType.DATA, 6001L, dataPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(dataMsg)

        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.storage.getCurrentOffset("test-topic", 0) > 0
        }

        long storageHead = harness.storage.getCurrentOffset("test-topic", 0)

        // Manually corrupt the offset file with impossible offset
        harness.offsetTracker.updateOffset("test-group:test-topic", storageHead + 500)

        when: "consumer subscribes (triggers offset restoration)"
        def subscribePayload = toJson([
            topic: "test-topic",
            group: "test-group"
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 6002L, subscribePayload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        then: "offset is clamped to storage head during registration"
        conditions.eventually {
            def registry = harness.context.getBean(ConsumerRegistry)
            def consumer = registry.getAllConsumers().find {
                it.clientId == harness.connection.remoteAddress && it.topic == "test-topic" && it.group == "test-group"
            }
            assert consumer != null
            assert consumer.currentOffset == storageHead
        }

        and: "corrected offset is persisted"
        conditions.eventually {
            assert harness.offsetTracker.getOffset("test-group:test-topic") == storageHead
        }

        cleanup:
        harness.shutdown()
    }

    // ========================================================================
    // ISSUE #1 TESTS: Startup Delivery Race Condition
    // ========================================================================

    def "registerConsumer does not start adaptive delivery before READY_ACK"() {
        given: "a running broker with data"
        def harness = AppHarness.start(tempDir)

        // Store some data
        def dataPayload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"],
            topic     : "test-topic"
        ])
        def dataMsg = new BrokerMessage(BrokerMessage.MessageType.DATA, 7001L, dataPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(dataMsg)

        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.storage.getCurrentOffset("test-topic", 0) > 0
        }

        when: "consumer subscribes but does NOT send READY_ACK"
        def subscribePayload = toJson([
            topic: "test-topic",
            group: "test-group"
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 7002L, subscribePayload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        // Wait for SUBSCRIBE ACK
        conditions.eventually {
            assert harness.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 7002L } != null
        }

        then: "no DATA messages are delivered (adaptive delivery not started)"
        // Wait a reasonable time to ensure no delivery happens
        Thread.sleep(500)
        assert harness.received.find { it.type == BrokerMessage.MessageType.DATA } == null

        cleanup:
        harness.shutdown()
    }

    def "markConsumerReady starts adaptive delivery immediately"() {
        given: "a running broker with data and a subscribed consumer"
        def harness = AppHarness.start(tempDir)

        // Store data
        def dataPayload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"],
            topic     : "test-topic"
        ])
        def dataMsg = new BrokerMessage(BrokerMessage.MessageType.DATA, 8001L, dataPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(dataMsg)

        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.storage.getCurrentOffset("test-topic", 0) > 0
        }

        // Subscribe
        def subscribePayload = toJson([
            topic: "test-topic",
            group: "test-group"
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 8002L, subscribePayload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        conditions.eventually {
            assert harness.received.find { it.type == BrokerMessage.MessageType.ACK && it.messageId == 8002L } != null
        }

        when: "consumer sends READY_ACK"
        def readyAck = new BrokerMessage(BrokerMessage.MessageType.READY_ACK, 8003L, new byte[0])
        harness.send(readyAck)

        then: "DATA is delivered immediately (no exponential backoff delay)"
        def deliveryConditions = new PollingConditions(timeout: 2, delay: 0.05)
        deliveryConditions.eventually {
            def dataMessages = harness.received.findAll { it.type == BrokerMessage.MessageType.DATA }
            assert dataMessages.size() > 0
            // Verify it contains our stored message
            def decoded = OBJECT_MAPPER.readValue(dataMessages[0].payload, List)
            assert decoded[0].msgKey == "key-1"
        }

        cleanup:
        harness.shutdown()
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    private static String toJson(Map<String, ?> value) {
        return OBJECT_MAPPER.writeValueAsString(value)
    }

    private static int findFreePort() {
        def socket = new java.net.ServerSocket(0)
        try {
            return socket.localPort
        } finally {
            socket.close()
        }
    }

    private static class AppHarness {
        final ApplicationContext context
        final NettyTcpClient client
        final NettyTcpClient.TcpConnection connection
        final ConcurrentLinkedQueue<BrokerMessage> received
        final int port
        final com.messaging.common.api.StorageEngine storage
        final com.messaging.broker.consumer.ConsumerOffsetTracker offsetTracker

        private AppHarness(ApplicationContext context, int port) {
            this.context = context
            this.port = port
            this.client = new NettyTcpClient()
            this.received = new ConcurrentLinkedQueue<>()
            this.connection = client.connect("localhost", port) { msg ->
                received.add(msg)
            }.get(5, TimeUnit.SECONDS)
            this.storage = context.getBean(com.messaging.common.api.StorageEngine)
            this.offsetTracker = context.getBean(com.messaging.broker.consumer.ConsumerOffsetTracker)
        }

        static AppHarness start(Path tempDir, Map<String, String> extraConfig = [:]) {
            def port = findFreePort()
            def config = [
                'broker.nodeId'                                     : 'test-broker',
                'broker.network.port'                               : port.toString(),
                'broker.storage.dataDir'                            : tempDir.toString(),
                'micronaut.server.port'                             : findFreePort().toString(),
                'broker.registry.url'                               : 'http://localhost:9999',
                'broker.registry.enabled'                           : 'false',
                'broker.pipe.enabled'                               : 'false',
                'broker.consumer.max-message-size-per-consumer'     : '1048576',
                'broker.consumer.ack-timeout'                       : '5000',
                'data-refresh.enabled'                              : 'false'
            ]
            config.putAll(extraConfig)

            def context = ApplicationContext.builder()
                .properties(config)
                .build()
                .start()

            return new AppHarness(context, port)
        }

        void send(BrokerMessage message) {
            connection.send(message).get(3, TimeUnit.SECONDS)
        }

        void shutdown() {
            try {
                connection?.close()
                client?.shutdown()
                context?.close()
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
}
