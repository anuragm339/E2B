package com.messaging.broker.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.support.ModernConsumerHarness
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.BrokerMessage
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.micronaut.context.annotation.Value
import io.micronaut.test.support.TestPropertyProvider
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration tests for the registry + pipe topology.
 *
 * Uses TestPropertyProvider to inject a dynamic registry stub URL so the
 * broker's TopologyManager and HttpPipeConnector can talk to it.
 */
@MicronautTest
class BrokerRegistryIntegrationSpec extends Specification implements TestPropertyProvider {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    /** Started once before the context; shared across all tests in this spec. */
    @Shared
    static RegistryStub registryStub

    @Override
    Map<String, String> getProperties() {
        if (registryStub == null) {
            registryStub = RegistryStub.start()
        }
        def dataDir = Files.createTempDirectory('broker-registry-').toAbsolutePath().toString()
        return [
            'broker.network.port'             : '19095',
            'micronaut.server.port'           : '18085',
            'broker.storage.data-dir'         : dataDir,
            'ack-store.rocksdb.path'          : "${dataDir}/ack-store",
            'broker.registry.url'             : registryStub.baseUrl,
            'broker.pipe.min-poll-interval-ms': '200',
            'broker.pipe.max-poll-interval-ms': '1000',
            'broker.pipe.poll-limit'          : '5'
        ]
    }

    def cleanupSpec() {
        registryStub?.stop()
        registryStub = null
    }

    @Inject StorageEngine storage

    @Value('${broker.network.port}')
    int tcpPort

    ModernConsumerHarness consumer

    def setup() {
        consumer = ModernConsumerHarness.connect('127.0.0.1', tcpPort)
        registryStub.reset()
    }

    def cleanup() {
        consumer?.close()
    }

    def "broker polls registry for topology on startup"() {
        expect:
        new PollingConditions(timeout: 6).eventually {
            assert registryStub.registryRequests.get() > 0
        }
    }

    def "broker polls pipe after receiving topology"() {
        expect:
        new PollingConditions(timeout: 6).eventually {
            assert registryStub.pipePollRequests.get() > 0
        }
    }

    def "pipe poll stores received records in storage"() {
        given:
        def record = [
            offset   : 1L,
            topic    : 'pipe-topic',
            partition: 0,
            msgKey   : 'pipe-key-1',
            eventType: 'MESSAGE',
            data     : '{"pipe":1}',
            createdAt: Instant.now().toString()
        ]
        registryStub.enqueuePipePayload(OBJECT_MAPPER.writeValueAsBytes([record]))

        expect:
        new PollingConditions(timeout: 8).eventually {
            def stored = storage.read('pipe-topic', 0, 1, 10)
            assert stored.size() == 1
            assert stored[0].msgKey == 'pipe-key-1'
        }
    }

    def "pipe data is pushed to a subscribed consumer"() {
        given:
        def record = [
            offset   : 1L,
            topic    : 'push-topic',
            partition: 0,
            msgKey   : 'push-key-1',
            eventType: 'MESSAGE',
            data     : '{"push":1}',
            createdAt: Instant.now().toString()
        ]
        registryStub.enqueuePipePayload(OBJECT_MAPPER.writeValueAsBytes([record]))

        and: "subscribe and complete the startup READY handshake"
        consumer.subscribe('push-topic', 'push-group')
        def conditions = new PollingConditions(timeout: 8)
        conditions.eventually {
            assert consumer.received.any { it.type == BrokerMessage.MessageType.READY }
        }
        consumer.sendReadyAck('push-topic', 'push-group')
        consumer.received.clear()

        expect: "piped record delivered to subscriber"
        conditions.eventually {
            def dataMsg = consumer.received.find {
                it.type == BrokerMessage.MessageType.DATA && it.payload?.length > 0
            }
            assert dataMsg != null
            def decoded = OBJECT_MAPPER.readValue(dataMsg.payload, List)
            assert decoded.any { it.msgKey == 'push-key-1' }
        }
    }

    // -------------------------------------------------------------------------
    // Registry + pipe stub
    // -------------------------------------------------------------------------

    static class RegistryStub {
        final HttpServer server
        final int port
        final String baseUrl
        final AtomicInteger registryRequests = new AtomicInteger()
        final AtomicInteger pipePollRequests = new AtomicInteger()
        final ConcurrentLinkedQueue<byte[]> pipePayloads = new ConcurrentLinkedQueue<>()

        private RegistryStub(HttpServer server, int port) {
            this.server = server
            this.port = port
            this.baseUrl = "http://127.0.0.1:${port}"
        }

        static RegistryStub start() {
            int p = findFreePort()
            def srv = HttpServer.create(new InetSocketAddress('127.0.0.1', p), 0)
            def stub = new RegistryStub(srv, p)
            stub.registerHandlers()
            srv.start()
            stub
        }

        void registerHandlers() {
            server.createContext('/registry/topology') { HttpExchange ex ->
                registryRequests.incrementAndGet()
                def body = OBJECT_MAPPER.writeValueAsBytes([
                    nodeId         : 'node-1',
                    role           : 'L2',
                    requestToFollow: [baseUrl],
                    topologyVersion: 'test-1',
                    topics         : []
                ])
                ex.responseHeaders.add('Content-Type', 'application/json')
                ex.sendResponseHeaders(200, body.length)
                ex.responseBody.withStream { it.write(body) }
                ex.close()
            }

            server.createContext('/pipe/poll') { HttpExchange ex ->
                int n = pipePollRequests.incrementAndGet()
                def payload = pipePayloads.peek()
                if (payload == null || n <= 1) {
                    // Skip first poll to let handler register; skip empty queue
                    ex.sendResponseHeaders(204, -1)
                    ex.close()
                    return
                }
                pipePayloads.poll()
                ex.responseHeaders.add('Content-Type', 'application/json')
                ex.sendResponseHeaders(200, payload.length)
                ex.responseBody.withStream { it.write(payload) }
                ex.close()
            }
        }

        void enqueuePipePayload(byte[] payload) {
            pipePayloads.add(payload)
        }

        void reset() {
            registryRequests.set(0)
            pipePollRequests.set(0)
            pipePayloads.clear()
        }

        void stop() {
            server.stop(0)
        }

        private static int findFreePort() {
            def s = new java.net.ServerSocket(0)
            try { s.localPort } finally { s.close() }
        }
    }
}
