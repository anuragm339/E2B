package com.messaging.broker.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.common.model.TopologyResponse
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.micronaut.http.client.HttpClient
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Shared
import spock.lang.Specification

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletionException

/**
 * Integration tests for CloudRegistryClient.
 *
 * A lightweight in-process stub HTTP server (com.sun.net.httpserver.HttpServer) acts
 * as the Cloud Registry.  CloudRegistryClient is instantiated directly (not via @Inject)
 * with a standalone Micronaut HttpClient that targets the stub's URL — this avoids
 * the @Client("/") binding to the Micronaut embedded server.
 */
@MicronautTest
class CloudRegistryClientIntegrationSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    @Shared
    static RegistryStub stub

    // Standalone client + registry client created once per spec
    @Shared
    static HttpClient standaloneHttpClient

    @Shared
    static CloudRegistryClient client

    def setupSpec() {
        stub = RegistryStub.start()
        standaloneHttpClient = HttpClient.create(new URL(stub.baseUrl))
        client = new CloudRegistryClient(standaloneHttpClient)
    }

    def cleanupSpec() {
        standaloneHttpClient?.close()
        stub?.stop()
    }

    def setup() {
        stub.reset()
    }

    // =========================================================================
    // Success path
    // =========================================================================

    def "getTopology returns populated TopologyResponse on HTTP 200"() {
        given:
        stub.enqueueResponse(200, OBJECT_MAPPER.writeValueAsString([
            nodeId         : 'node-1',
            role           : 'L2',
            requestToFollow: [stub.baseUrl],
            topologyVersion: 'v1',
            topics         : ['prices-v1', 'reference-data']
        ]))

        when:
        def topology = client.getTopology(stub.baseUrl, 'node-1').get()

        then:
        topology != null
        topology.getNodeId() == 'node-1'
        topology.getRole() == TopologyResponse.NodeRole.L2
        !topology.getRequestToFollow().isEmpty()
    }

    def "getTopology forwards nodeId in request URL"() {
        given:
        stub.enqueueResponse(200, OBJECT_MAPPER.writeValueAsString([
            nodeId: 'my-node', role: 'L2', requestToFollow: [], topologyVersion: 'v1', topics: []
        ]))

        when:
        client.getTopology(stub.baseUrl, 'my-node').get()

        then:
        stub.lastRequestUri?.contains('nodeId=my-node')
    }

    def "each successful call increments the stub request counter"() {
        given:
        stub.enqueueResponse(200, OBJECT_MAPPER.writeValueAsString([
            nodeId: 'n', role: 'L2', requestToFollow: [], topologyVersion: 'x', topics: []
        ]))

        when:
        client.getTopology(stub.baseUrl, 'n').get()

        then:
        stub.requestCount >= 1
    }

    // =========================================================================
    // Error paths
    // =========================================================================

    def "getTopology throws when registry returns HTTP 500"() {
        given:
        stub.enqueueResponse(500, '{"error":"internal server error"}')

        when:
        client.getTopology(stub.baseUrl, 'node-err').join()

        then:
        thrown(Exception)
    }

    def "getTopology throws when registry returns invalid JSON"() {
        given:
        stub.enqueueResponse(200, 'not-valid-json}}}')

        when:
        client.getTopology(stub.baseUrl, 'node-badjson').join()

        then:
        thrown(Exception)
    }

    def "getTopology throws when registry returns empty body"() {
        given:
        stub.enqueueEmptyBody()

        when:
        client.getTopology(stub.baseUrl, 'node-empty').join()

        then:
        thrown(Exception)
    }

    // =========================================================================
    // Stub
    // =========================================================================

    static class RegistryStub {
        final HttpServer server
        final String baseUrl
        volatile int requestCount = 0
        volatile String lastRequestUri = null
        private final java.util.concurrent.ConcurrentLinkedQueue<Response> queue =
            new java.util.concurrent.ConcurrentLinkedQueue<>()

        private static final class Response {
            final int status
            final String body  // null → empty body
            Response(int status, String body) { this.status = status; this.body = body }
        }

        private RegistryStub(HttpServer server, int port) {
            this.server  = server
            this.baseUrl = "http://127.0.0.1:${port}"
        }

        static RegistryStub start() {
            int port = freePort()
            def srv  = HttpServer.create(new InetSocketAddress('127.0.0.1', port), 0)
            def stub = new RegistryStub(srv, port)
            srv.createContext('/registry/topology') { HttpExchange ex ->
                stub.requestCount++
                stub.lastRequestUri = ex.requestURI.toString()
                def resp = stub.queue.poll()
                if (resp == null || resp.body == null) {
                    // Return 200 with literally empty body (no Content-Type header)
                    ex.sendResponseHeaders(200, 0)
                    ex.responseBody.close()
                    ex.close()
                    return
                }
                byte[] bytes = resp.body.getBytes(StandardCharsets.UTF_8)
                ex.responseHeaders.add('Content-Type', 'application/json')
                ex.sendResponseHeaders(resp.status, bytes.length)
                ex.responseBody.withStream { it.write(bytes) }
                ex.close()
            }
            srv.start()
            stub
        }

        void enqueueResponse(int status, String body) { queue.add(new Response(status, body)) }
        void enqueueEmptyBody() { queue.add(new Response(200, null)) }

        void reset() {
            requestCount    = 0
            lastRequestUri  = null
            queue.clear()
        }

        void stop() { server.stop(0) }

        private static int freePort() {
            def s = new java.net.ServerSocket(0)
            try { s.localPort } finally { s.close() }
        }
    }
}
