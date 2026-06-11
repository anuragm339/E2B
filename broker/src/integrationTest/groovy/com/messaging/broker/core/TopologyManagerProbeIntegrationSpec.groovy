package com.messaging.broker.core

import com.messaging.common.api.PipeConnector
import com.messaging.common.model.MessageRecord
import com.messaging.common.model.TopologyResponse
import com.sun.net.httpserver.HttpServer
import spock.lang.Specification
import spock.lang.TempDir

import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration coverage for Fix 1 — exercises {@code TopologyManager}'s real HTTP
 * {@code /health} probe (not the stubbed predicate from the unit spec) against a
 * live embedded HTTP server.
 *
 * <p>This catches regressions in the probe wiring itself (URL composition,
 * timeout handling, 2xx interpretation) which a predicate-stub bypasses.
 */
class TopologyManagerProbeIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    HttpServer healthyServer
    HttpServer unhealthyServer  // returns 500 from /health
    int healthyPort
    int unhealthyPort
    AtomicInteger healthyHits

    def setup() {
        healthyHits = new AtomicInteger()
        healthyServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        healthyServer.createContext("/health", { exch ->
            healthyHits.incrementAndGet()
            def body = '{"status":"UP"}'.bytes
            exch.sendResponseHeaders(200, body.length)
            exch.responseBody.write(body)
            exch.close()
        })
        healthyServer.start()
        healthyPort = healthyServer.address.port

        unhealthyServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        unhealthyServer.createContext("/health", { exch ->
            exch.sendResponseHeaders(500, -1)
            exch.close()
        })
        unhealthyServer.start()
        unhealthyPort = unhealthyServer.address.port
    }

    def cleanup() {
        healthyServer?.stop(0)
        unhealthyServer?.stop(0)
    }

    def "probe accepts 2xx /health and proceeds with the switch"() {
        given:
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(
                Mock(CloudRegistryClient), pipeConnector,
                "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord r -> true })

        when: 'topology requests a switch to the healthy server'
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:${healthyPort}".toString()])
        waitFor { pipeConnector.connectCalls == 1 }

        then: '/health was probed and the switch went through'
        healthyHits.get() == 1
        manager.getCurrentParentUrl() == "http://127.0.0.1:${healthyPort}"
    }

    def "probe rejects 5xx /health and leaves the live connection untouched"() {
        given:
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(
                Mock(CloudRegistryClient), pipeConnector,
                "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord r -> true })

        and: 'broker is already connected to a healthy parent'
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:${healthyPort}".toString()])
        waitFor { pipeConnector.connectCalls == 1 }
        Thread.sleep(50)

        when: 'topology now points at the unhealthy server'
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:${unhealthyPort}".toString()])
        Thread.sleep(150)

        then: 'no disconnect and no new connect'
        pipeConnector.disconnectCalls == 0
        pipeConnector.connectCalls == 1
        manager.getCurrentParentUrl() == "http://127.0.0.1:${healthyPort}"
    }

    def "probe rejects an unroutable URL (no listener) and leaves the live connection untouched"() {
        given:
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(
                Mock(CloudRegistryClient), pipeConnector,
                "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord r -> true })

        and: 'broker is connected to a healthy parent'
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:${healthyPort}".toString()])
        waitFor { pipeConnector.connectCalls == 1 }
        Thread.sleep(50)

        when: 'topology returns a deliberately invalid host:port'
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:1"])  // port 1 — refused
        Thread.sleep(200)

        then:
        pipeConnector.disconnectCalls == 0
        pipeConnector.connectCalls == 1
        manager.getCurrentParentUrl() == "http://127.0.0.1:${healthyPort}"
    }

    def "probe can succeed but connect still fail"() {
        given:
        def secondHealthyHits = new AtomicInteger()
        def secondHealthyServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        secondHealthyServer.createContext("/health", { exch ->
            secondHealthyHits.incrementAndGet()
            exch.sendResponseHeaders(200, -1)
            exch.close()
        })
        secondHealthyServer.start()
        def secondHealthyPort = secondHealthyServer.address.port

        and:
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(
                Mock(CloudRegistryClient), pipeConnector,
                "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord r -> true })

        and: 'broker is already connected to a healthy parent'
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:${healthyPort}".toString()])
        waitFor { pipeConnector.connectCalls == 1 }
        Thread.sleep(50)

        and: 'next connect attempt will fail after the probe succeeds'
        pipeConnector.failOnConnectCall = 2

        when:
        invokeHandleTopologyUpdate(manager, ["http://127.0.0.1:${secondHealthyPort}".toString()])
        waitFor { pipeConnector.connectCalls == 2 }
        Thread.sleep(100)

        then: 'probe succeeded but the broker has no active parent'
        secondHealthyHits.get() == 1
        pipeConnector.disconnectCalls == 1
        manager.getCurrentParentUrl() == null

        cleanup:
        secondHealthyServer?.stop(0)
    }

    private static void waitFor(Closure cond) {
        long deadline = System.currentTimeMillis() + 3000
        while (!cond() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
    }

    private static void invokeHandleTopologyUpdate(TopologyManager manager, List<String> parents) {
        def topology = new TopologyResponse()
        topology.setNodeId("node-1")
        topology.setRequestToFollow(parents)
        topology.setRole(TopologyResponse.NodeRole.L2)
        topology.setTopologyVersion("1.0")
        def method = TopologyManager.class.getDeclaredMethod("handleTopologyUpdate", TopologyResponse.class)
        method.setAccessible(true)
        method.invoke(manager, topology)
    }

    private static class FakePipeConnector implements PipeConnector {
        int connectCalls = 0
        int disconnectCalls = 0
        Integer failOnConnectCall

        @Override
        CompletableFuture<PipeConnection> connectToParent(String parentUrl) {
            connectCalls++
            if (failOnConnectCall != null && connectCalls == failOnConnectCall) {
                return CompletableFuture.failedFuture(new RuntimeException("synthetic connect failure"))
            }
            return CompletableFuture.completedFuture([
                isConnected: { true },
                getParentUrl: { parentUrl },
                getLastReceivedOffset: { 0L }
            ] as PipeConnection)
        }

        @Override void onDataReceived(java.util.function.Function<MessageRecord, Boolean> handler) {}
        @Override CompletableFuture<Void> sendAck(long offset) { CompletableFuture.completedFuture(null) }
        @Override PipeHealth getHealth() { PipeHealth.HEALTHY }
        @Override void reconnect() {}
        @Override void pausePipeCalls() {}
        @Override void resumePipeCalls() {}
        @Override void disconnect() { disconnectCalls++ }
    }
}
