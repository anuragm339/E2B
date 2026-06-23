package com.messaging.broker.core

import com.messaging.common.api.PipeConnector
import com.messaging.common.model.MessageRecord
import com.messaging.common.model.TopologyResponse
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

class TopologyManagerSpec extends Specification {

    @TempDir
    Path tempDir

    def "connects to parent when requested"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord record -> })

        when:
        invokeConnectToParent(manager, "http://parent-1")

        then:
        pipeConnector.connectCalls == 1
        pipeConnector.dataHandlerSet
        manager.getCurrentParentUrl() == "http://parent-1"
    }

    def "resolveTopologyNow returns promptly once the first registry query completes"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def topology = new TopologyResponse()
        topology.setRequestToFollow(["http://parent-1"])
        registryClient.getTopology(_, _) >> CompletableFuture.completedFuture(topology)
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())
        manager.parentReachableProbe = { url -> true }  // skip real HTTP probe
        manager.onMessageReceived({ MessageRecord record -> })

        when: "start schedules the initial query; resolveTopologyNow blocks only until it lands"
        manager.start()
        def start = System.currentTimeMillis()
        manager.resolveTopologyNow(5000)
        def elapsed = System.currentTimeMillis() - start

        then: "it unblocks on the first resolution well before the timeout"
        elapsed < 4000

        cleanup:
        manager.shutdown()
    }

    def "resolveTopologyNow returns after the timeout when the registry never responds"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        registryClient.getTopology(_, _) >> new CompletableFuture<TopologyResponse>() // never completes
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())

        when:
        manager.start()
        def start = System.currentTimeMillis()
        manager.resolveTopologyNow(300)
        def elapsed = System.currentTimeMillis() - start

        then: "it does not hang; returns around the timeout with no parent resolved"
        elapsed >= 250
        manager.getCurrentParentUrl() == null

        cleanup:
        manager.shutdown()
    }

    def "disconnects from parent"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())
        invokeConnectToParent(manager, "http://parent-1")

        when:
        invokeDisconnectFromParent(manager)

        then:
        pipeConnector.disconnectCalls == 1
        manager.getCurrentParentUrl() == null
    }

    def "switches parent by disconnecting then reconnecting"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())
        invokeConnectToParent(manager, "http://parent-1")

        when:
        invokeDisconnectFromParent(manager)
        invokeConnectToParent(manager, "http://parent-2")

        then:
        pipeConnector.disconnectCalls == 1
        pipeConnector.connectCalls == 2
        manager.getCurrentParentUrl() == "http://parent-2"
    }

    def "topology switch is skipped when the new parent is unreachable"() {
        given: "broker connected to parent-1; probe returns false for parent-2"
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())
        manager.parentReachableProbe = { String url -> url.contains('parent-1') } as java.util.function.Predicate

        when: "first topology brings parent-1 up successfully"
        invokeHandleTopologyUpdate(manager, ["http://parent-1"])
        waitForConnect(pipeConnector, 1)
        Thread.sleep(50)

        and: "then registry returns an unreachable parent-2"
        invokeHandleTopologyUpdate(manager, ["http://parent-2"])
        Thread.sleep(50)

        then: "broker is still connected to parent-1"
        pipeConnector.disconnectCalls == 0
        pipeConnector.connectCalls == 1
        manager.getCurrentParentUrl() == 'http://parent-1'
    }

    private static void waitForConnect(FakePipeConnector p, int target) {
        long deadline = System.currentTimeMillis() + 2000
        while (p.connectCalls < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
    }

    private static void invokeHandleTopologyUpdate(TopologyManager manager, List<String> parents) {
        def topology = new com.messaging.common.model.TopologyResponse()
        topology.setNodeId("node-1")
        topology.setRequestToFollow(parents)
        topology.setRole(com.messaging.common.model.TopologyResponse.NodeRole.L2)
        topology.setTopologyVersion("1.0")
        def method = TopologyManager.class.getDeclaredMethod("handleTopologyUpdate",
                com.messaging.common.model.TopologyResponse.class)
        method.setAccessible(true)
        method.invoke(manager, topology)
    }

    private static void invokeConnectToParent(TopologyManager manager, String parentUrl) {
        def method = TopologyManager.class.getDeclaredMethod("connectToParent", String.class)
        method.setAccessible(true)
        method.invoke(manager, parentUrl)
    }

    private static void invokeDisconnectFromParent(TopologyManager manager) {
        def method = TopologyManager.class.getDeclaredMethod("disconnectFromParent")
        method.setAccessible(true)
        method.invoke(manager)
    }

    private static class FakePipeConnector implements PipeConnector {
        int connectCalls = 0
        int disconnectCalls = 0
        boolean dataHandlerSet = false
        String lastParentUrl
        @Override
        CompletableFuture<PipeConnection> connectToParent(String parentUrl) {
            connectCalls++
            lastParentUrl = parentUrl
            return CompletableFuture.completedFuture([
                isConnected: { true },
                getParentUrl: { parentUrl },
                getLastReceivedOffset: { 0L }
            ] as PipeConnection)
        }

        @Override
        void onDataReceived(java.util.function.Function<MessageRecord, Boolean> handler) {
            dataHandlerSet = true
        }

        @Override
        CompletableFuture<Void> sendAck(long offset) {
            return CompletableFuture.completedFuture(null)
        }

        @Override
        PipeHealth getHealth() { PipeHealth.HEALTHY }

        @Override
        void reconnect() { }

        @Override
        void pausePipeCalls() { }

        @Override
        void resumePipeCalls() { }

        @Override
        void disconnect() {
            disconnectCalls++
        }
    }
}
