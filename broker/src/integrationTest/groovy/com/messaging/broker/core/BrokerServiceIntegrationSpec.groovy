package com.messaging.broker.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.Application
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.EventType
import com.messaging.network.tcp.NettyTcpClient
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.Micronaut
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.Specification
import spock.lang.TempDir
import spock.util.concurrent.PollingConditions

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class BrokerServiceIntegrationSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @TempDir
    Path tempDir

    def "app starts and accepts DATA messages over TCP"() {
        given:
        def harness = AppHarness.start(tempDir)

        when:
        def payload = toJson([
            msg_key   : "key-1",
            event_type: "MESSAGE",
            data      : [value: "v1"],
            topic     : "topic-1"
        ])
        def message = new BrokerMessage(BrokerMessage.MessageType.DATA, 1001L, payload.getBytes(StandardCharsets.UTF_8))
        harness.send(message)

        then:
        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 1001L }
        }

        and:
        conditions.eventually {
            def records = harness.storage.read("topic-1", 0, 0, 10)
            assert records.size() == 1
            assert records[0].msgKey == "key-1"
            assert records[0].eventType == EventType.MESSAGE
            assert records[0].data == '{"value":"v1"}'
        }

        cleanup:
        harness.shutdown()
    }

    def "SUBSCRIBE and COMMIT_OFFSET persist state via running app"() {
        given:
        def harness = AppHarness.start(tempDir)

        when:
        def subscribePayload = toJson([
            topic: "topic-2",
            group: "group-2"
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 2001L, subscribePayload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        then:
        def conditions = new PollingConditions(timeout: 3)
        conditions.eventually {
            assert harness.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2001L }
        }
        conditions.eventually {
            assert harness.remoteConsumers.getAllConsumers().size() == 1
        }

        when:
        def commitPayload = toJson([
            topic : "topic-2",
            group : "group-2",
            offset: 42
        ])
        def commit = new BrokerMessage(BrokerMessage.MessageType.COMMIT_OFFSET, 2002L, commitPayload.getBytes(StandardCharsets.UTF_8))
        harness.send(commit)

        then:
        conditions.eventually {
            assert harness.received.any { it.type == BrokerMessage.MessageType.ACK && it.messageId == 2002L }
        }
        conditions.eventually {
            assert harness.offsetTracker.getOffset("group-2:topic-2") == 42L
        }

        cleanup:
        harness.shutdown()
    }

    def "data refresh flow: RESET -> RESET_ACK -> READY -> READY_ACK"() {
        given:
        def harness = AppHarness.start(tempDir)

        and:
        def subscribePayload = toJson([
            topic: "topic-3",
            group: "group-3"
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 3001L, subscribePayload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        def conditions = new PollingConditions(timeout: 5)
        conditions.eventually {
            assert harness.remoteConsumers.getAllConsumers().size() == 1
        }

        when:
        harness.dataRefreshCoordinator.startRefresh("topic-3").get(3, TimeUnit.SECONDS)

        then:
        conditions.eventually {
            assert harness.received.any { it.type == BrokerMessage.MessageType.RESET }
        }
        conditions.eventually {
            assert harness.dataRefreshCoordinator.getRefreshStatus("topic-3").state == RefreshState.RESET_SENT
        }

        when:
        def resetAckPayload = buildGroupTopicPayload("topic-3", "group-3")
        def resetAck = new BrokerMessage(BrokerMessage.MessageType.RESET_ACK, 3002L, resetAckPayload)
        harness.send(resetAck)

        then:
        conditions.eventually {
            assert harness.dataRefreshCoordinator.getRefreshStatus("topic-3").state == RefreshState.REPLAYING
        }

        and:
        conditions.eventually {
            assert harness.received.any { it.type == BrokerMessage.MessageType.READY }
        }

        when:
        def readyAckPayload = buildGroupTopicPayload("topic-3", "group-3")
        def readyAck = new BrokerMessage(BrokerMessage.MessageType.READY_ACK, 3003L, readyAckPayload)
        harness.send(readyAck)

        then:
        conditions.eventually {
            assert harness.dataRefreshCoordinator.getRefreshStatus("topic-3") == null ||
                harness.dataRefreshCoordinator.getRefreshStatus("topic-3").state == RefreshState.COMPLETED
        }

        cleanup:
        harness.shutdown()
    }

    def "management endpoints are available"() {
        given:
        def harness = AppHarness.start(tempDir)

        when:
        def health = harness.httpGet("/health")
        def prometheus = harness.httpGet("/prometheus")

        then:
        health.statusCode == 200
        health.body.contains("\"status\":\"UP\"")

        and:
        prometheus.statusCode == 200
        prometheus.body.contains("jvm") || prometheus.body.contains("process") || prometheus.body.contains("system")

        cleanup:
        harness.shutdown()
    }

    def "registry topology triggers pipe polling"() {
        given:
        def registry = RegistryStub.start()
        def harness = AppHarness.start(tempDir, [
            'broker.registry.url': registry.baseUrl
        ])
        def conditions = new PollingConditions(timeout: 6)

        expect:
        conditions.eventually {
            assert registry.registryRequests.get() > 0
        }
        conditions.eventually {
            assert registry.pipePollRequests.get() > 0
        }

        cleanup:
        harness.shutdown()
        registry.stop()
    }

    def "pipe poll stores data and delivers to subscribed consumer"() {
        given:
        def registry = RegistryStub.start()
        def record = [
            offset   : 1L,
            topic    : "pipe-topic",
            partition: 0,
            msgKey   : "pipe-key",
            eventType: "MESSAGE",
            data     : "{\"pipe\":1}",
            createdAt: Instant.now().toString()
        ]
        registry.enqueuePipePayload(OBJECT_MAPPER.writeValueAsBytes([record]))

        def harness = AppHarness.start(tempDir, [
            'broker.registry.url': registry.baseUrl
        ])

        and:
        def subscribePayload = toJson([
            topic: "pipe-topic",
            group: "group-pipe"
        ])
        def subscribe = new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, 4001L, subscribePayload.getBytes(StandardCharsets.UTF_8))
        harness.send(subscribe)

        def conditions = new PollingConditions(timeout: 8)

        expect:
        conditions.eventually {
            def stored = harness.storage.read("pipe-topic", 0, 1, 10)
            assert stored.size() == 1
            assert stored[0].msgKey == "pipe-key"
        }

        and:
        conditions.eventually {
            def dataMsg = harness.received.find { it.type == BrokerMessage.MessageType.DATA && it.payload?.length > 0 }
            assert dataMsg != null
            def decoded = OBJECT_MAPPER.readValue(dataMsg.payload, List)
            assert decoded[0].msgKey == "pipe-key"
            assert decoded[0].data == "{\"pipe\":1}"
        }

        and:
        conditions.eventually {
            assert harness.offsetTracker.getOffset("group-pipe:pipe-topic") >= 2L
        }

        cleanup:
        harness.shutdown()
        registry.stop()
    }

    private static String toJson(Map<String, ?> value) {
        return OBJECT_MAPPER.writeValueAsString(value)
    }

    private static byte[] buildGroupTopicPayload(String topic, String group) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8)
        byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8)
        ByteBuffer buffer = ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length)
        buffer.putInt(topicBytes.length)
        buffer.put(topicBytes)
        buffer.putInt(groupBytes.length)
        buffer.put(groupBytes)
        return buffer.array()
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
        final int httpPort
        final StorageEngine storage
        final ConsumerRegistry remoteConsumers
        final ConsumerOffsetTracker offsetTracker
        final RefreshCoordinator dataRefreshCoordinator
        final HttpClient httpClient

        static AppHarness start(Path dataDir, Map<String, Object> extraProps = [:]) {
            int port = findFreePort()
            Map<String, Object> props = [
                'broker.network.port'    : port,
                'broker.storage.data-dir': dataDir.toString(),
                'broker.storage.type'    : 'filechannel',
                'broker.network.type'    : 'tcp',
                'broker.registry.url'    : '',
                'micronaut.server.port'  : 0
            ]
            props.putAll(extraProps)

            ApplicationContext context = Micronaut.build()
                .properties(props)
                .mainClass(Application)
                .start()

            def client = new NettyTcpClient()
            def connection = client.connect('127.0.0.1', port).get(5, TimeUnit.SECONDS) as NettyTcpClient.TcpConnection

            def received = new ConcurrentLinkedQueue<BrokerMessage>()
            connection.onMessage { msg -> received.add(msg) }

            def storage = context.getBean(StorageEngine)
            def remoteConsumers = context.getBean(ConsumerRegistry)
            def offsetTracker = context.getBean(ConsumerOffsetTracker)
            def dataRefreshCoordinator = context.getBean(RefreshCoordinator)
            def httpPort = context.getBean(EmbeddedServer).port
            def httpClient = HttpClient.newHttpClient()

            return new AppHarness(
                context,
                client,
                connection,
                received,
                port,
                httpPort,
                storage,
                remoteConsumers,
                offsetTracker,
                dataRefreshCoordinator,
                httpClient
            )
        }

        AppHarness(
            ApplicationContext context,
            NettyTcpClient client,
            NettyTcpClient.TcpConnection connection,
            ConcurrentLinkedQueue<BrokerMessage> received,
            int port,
            int httpPort,
            StorageEngine storage,
            ConsumerRegistry remoteConsumers,
            ConsumerOffsetTracker offsetTracker,
            RefreshCoordinator dataRefreshCoordinator,
            HttpClient httpClient
        ) {
            this.context = context
            this.client = client
            this.connection = connection
            this.received = received
            this.port = port
            this.httpPort = httpPort
            this.storage = storage
            this.remoteConsumers = remoteConsumers
            this.offsetTracker = offsetTracker
            this.dataRefreshCoordinator = dataRefreshCoordinator
            this.httpClient = httpClient
        }

        void send(BrokerMessage message) {
            connection.send(message).get(3, TimeUnit.SECONDS)
        }

        HttpResult httpGet(String path) {
            def request = HttpRequest.newBuilder()
                .uri(new URI("http://127.0.0.1:${httpPort}${path}"))
                .timeout(java.time.Duration.ofSeconds(3))
                .GET()
                .build()
            def response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            return new HttpResult(response.statusCode(), response.body())
        }

        void shutdown() {
            try {
                if (context != null) {
                    context.close()
                }
            } finally {
                if (client != null) {
                    client.shutdown()
                }
            }
        }
    }

    private static class HttpResult {
        final int statusCode
        final String body

        HttpResult(int statusCode, String body) {
            this.statusCode = statusCode
            this.body = body
        }
    }

    private static class RegistryStub {
        final HttpServer server
        final int port
        final String baseUrl
        final AtomicInteger registryRequests = new AtomicInteger(0)
        final AtomicInteger pipePollRequests = new AtomicInteger(0)
        final ConcurrentLinkedQueue<byte[]> pipePayloads = new ConcurrentLinkedQueue<>()

        static RegistryStub start() {
            def port = findFreePort()
            def server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0)
            def stub = new RegistryStub(server, port)
            stub.registerHandlers()
            server.start()
            return stub
        }

        RegistryStub(HttpServer server, int port) {
            this.server = server
            this.port = port
            this.baseUrl = "http://127.0.0.1:${port}"
        }

        void registerHandlers() {
            server.createContext("/registry/topology") { HttpExchange exchange ->
                registryRequests.incrementAndGet()
                def response = OBJECT_MAPPER.writeValueAsBytes([
                    nodeId         : "node-1",
                    role           : "L2",
                    requestToFollow: [baseUrl],
                    topologyVersion: "test-1",
                    topics         : []
                ])
                exchange.getResponseHeaders().add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, response.length)
                exchange.responseBody.withStream { it.write(response) }
                exchange.close()
            }

            server.createContext("/pipe/poll") { HttpExchange exchange ->
                int pollCount = pipePollRequests.incrementAndGet()
                def payload = pipePayloads.peek()
                if (payload == null) {
                    exchange.sendResponseHeaders(204, -1)
                    exchange.close()
                    return
                }
                if (pollCount <= 1) {
                    // First poll can race with handler registration; skip once to avoid dropping data.
                    exchange.sendResponseHeaders(204, -1)
                    exchange.close()
                    return
                }
                pipePayloads.poll()
                exchange.getResponseHeaders().add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, payload.length)
                exchange.responseBody.withStream { it.write(payload) }
                exchange.close()
            }
        }

        void enqueuePipePayload(byte[] payload) {
            pipePayloads.add(payload)
        }

        void stop() {
            server.stop(0)
        }
    }
}
