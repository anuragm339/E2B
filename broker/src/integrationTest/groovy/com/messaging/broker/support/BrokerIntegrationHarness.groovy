package com.messaging.broker.support

import com.messaging.broker.Application
import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.messaging.network.tcp.NettyTcpClient
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.Micronaut
import io.micronaut.runtime.server.EmbeddedServer

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

class BrokerIntegrationHarness {

    final ApplicationContext context
    final NettyTcpClient client
    final NettyTcpClient.TcpConnection connection
    final ConcurrentLinkedQueue<BrokerMessage> received
    final List<AutoCloseable> managedClients
    final int port
    final int httpPort
    final StorageEngine storage
    final ConsumerRegistry remoteConsumers
    final ConsumerOffsetTracker offsetTracker
    final RefreshCoordinator dataRefreshCoordinator
    final HttpClient httpClient
    final Map<String, String> previousSystemProperties

    private BrokerIntegrationHarness(
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
        HttpClient httpClient,
        Map<String, String> previousSystemProperties
    ) {
        this.context = context
        this.client = client
        this.connection = connection
        this.received = received
        this.managedClients = new CopyOnWriteArrayList<>()
        this.port = port
        this.httpPort = httpPort
        this.storage = storage
        this.remoteConsumers = remoteConsumers
        this.offsetTracker = offsetTracker
        this.dataRefreshCoordinator = dataRefreshCoordinator
        this.httpClient = httpClient
        this.previousSystemProperties = previousSystemProperties
    }

    static BrokerIntegrationHarness start(Path dataDir, Map<String, ?> extraProps = [:]) {
        int port = findFreePort()
        String rootDir = dataDir.toString()
        String ackStoreDir = dataDir.resolve("ack-store").toString()
        Map<String, String> previousSystemProperties = captureSystemProperties('broker.storage.dataDir', 'DATA_DIR')

        System.setProperty('broker.storage.dataDir', rootDir)
        System.setProperty('DATA_DIR', rootDir)

        Map<String, Object> props = [
            'DATA_DIR'                                : rootDir,
            'broker.nodeId'                           : 'test-broker',
            'broker.network.port'                     : port,
            'broker.storage.type'                     : 'filechannel',
            'broker.storage.dataDir'                  : rootDir,
            'broker.storage.data-dir'                 : rootDir,
            'broker.network.type'                     : 'tcp',
            'broker.registry.url'                     : 'http://127.0.0.1:65534',
            'micronaut.server.port'                   : 0,
            'ack-store.rocksdb.path'                  : ackStoreDir,
            'ack-store.reconciliation.enabled'        : false,
            'ack-store.reconciliation.auto-sync-enabled': false
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
        def httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build()

        new BrokerIntegrationHarness(
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
            httpClient,
            previousSystemProperties
        )
    }

    void send(BrokerMessage message) {
        connection.send(message).get(3, TimeUnit.SECONDS)
    }

    void sendData(String topic, String key, Object data) {
        String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString([
            msg_key   : key,
            event_type: 'MESSAGE',
            data      : data,
            topic     : topic
        ])
        send(new BrokerMessage(BrokerMessage.MessageType.DATA, System.nanoTime(), json.getBytes(StandardCharsets.UTF_8)))
    }

    ModernConsumerHarness connectModernConsumer() {
        def consumer = ModernConsumerHarness.connect('127.0.0.1', port)
        managedClients.add(consumer)
        consumer
    }

    LegacyConsumerHarness connectLegacyConsumer(String serviceName) {
        def consumer = LegacyConsumerHarness.connect('127.0.0.1', port, serviceName)
        managedClients.add(consumer)
        consumer
    }

    HttpResult httpGet(String path) {
        def request = HttpRequest.newBuilder()
            .uri(new URI("http://127.0.0.1:${httpPort}${path}"))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build()
        def response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        new HttpResult(response.statusCode(), response.body())
    }

    static byte[] buildGroupTopicPayload(String topic, String group) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8)
        byte[] groupBytes = group.getBytes(StandardCharsets.UTF_8)
        ByteBuffer buffer = ByteBuffer.allocate(4 + topicBytes.length + 4 + groupBytes.length)
        buffer.putInt(topicBytes.length)
        buffer.put(topicBytes)
        buffer.putInt(groupBytes.length)
        buffer.put(groupBytes)
        buffer.array()
    }

    void shutdown() {
        try {
            managedClients.reverseEach { client ->
                try {
                    client.close()
                } catch (Exception ignored) {
                }
            }
        } finally {
            try {
                connection?.disconnect()
            } catch (Exception ignored) {
            }
            try {
                client?.shutdown()
            } catch (Exception ignored) {
            }
            try {
                context?.close()
            } catch (Exception ignored) {
            }
            restoreSystemProperties(previousSystemProperties)
        }
    }

    static int findFreePort() {
        def socket = new java.net.ServerSocket(0)
        try {
            socket.localPort
        } finally {
            socket.close()
        }
    }

    static class HttpResult {
        final int statusCode
        final String body

        HttpResult(int statusCode, String body) {
            this.statusCode = statusCode
            this.body = body
        }
    }

    private static Map<String, String> captureSystemProperties(String... names) {
        Map<String, String> values = [:]
        names.each { name ->
            values[name] = System.getProperty(name)
        }
        values
    }

    private static void restoreSystemProperties(Map<String, String> values) {
        values.each { name, value ->
            if (value == null) {
                System.clearProperty(name)
            } else {
                System.setProperty(name, value)
            }
        }
    }
}
