package com.messaging.broker.core

import com.messaging.broker.consistency.PipeLineageStore
import com.messaging.common.api.PipeConnector
import com.messaging.common.model.MessageRecord
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
        def lineage = new PipeLineageStore(tempDir)
        def manager = new TopologyManager(registryClient, pipeConnector, lineage, "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord record -> })

        when:
        invokeConnectToParent(manager, "http://parent-1")

        then:
        pipeConnector.connectCalls == 1
        pipeConnector.dataHandlerSet
        manager.getCurrentParentUrl() == "http://parent-1"

        cleanup:
        lineage?.close()
    }

    def "disconnects from parent"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def lineage = new PipeLineageStore(tempDir)
        def manager = new TopologyManager(registryClient, pipeConnector, lineage, "http://registry", "node-1", tempDir.toString())
        invokeConnectToParent(manager, "http://parent-1")

        when:
        invokeDisconnectFromParent(manager)

        then:
        pipeConnector.disconnectCalls == 1
        manager.getCurrentParentUrl() == null

        cleanup:
        lineage?.close()
    }

    def "switches parent by disconnecting then reconnecting"() {
        given:
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def lineage = new PipeLineageStore(tempDir)
        def manager = new TopologyManager(registryClient, pipeConnector, lineage, "http://registry", "node-1", tempDir.toString())
        invokeConnectToParent(manager, "http://parent-1")

        when:
        invokeDisconnectFromParent(manager)
        invokeConnectToParent(manager, "http://parent-2")

        then:
        pipeConnector.disconnectCalls == 1
        pipeConnector.connectCalls == 2
        manager.getCurrentParentUrl() == "http://parent-2"

        cleanup:
        lineage?.close()
    }

    def "parent switch records lineage using the live pipe cursor"() {
        given: "broker starts with cursor=0 and probe always succeeds"
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        pipeConnector.currentOffsetValue = 0L
        def lineage = new PipeLineageStore(tempDir)
        def manager = new TopologyManager(registryClient, pipeConnector, lineage, "http://registry", "node-1", tempDir.toString())
        // Stub the parent-reachable probe so we don't hit real HTTP in the test.
        manager.parentReachableProbe = { String url -> true } as java.util.function.Predicate

        when: "topology assigns parent-1 (cursor=0), cursor advances to 500, then switches to parent-2"
        invokeHandleTopologyUpdate(manager, ["http://parent-1"])
        waitForConnect(pipeConnector, 1)
        pipeConnector.currentOffsetValue = 500L
        invokeHandleTopologyUpdate(manager, ["http://parent-2"])
        waitForConnect(pipeConnector, 2)
        // Lineage update is chained onto the connect-success future; give it a moment to land.
        Thread.sleep(50)

        then: "lineage shows parent-1 [0,500), parent-2 [500,...)"
        def all = lineage.allEntries()
        all.size() == 2
        all[0].parentUrl == 'http://parent-1'
        all[0].offsetStart == 0L
        all[0].offsetEndExclusive == 500L
        all[1].parentUrl == 'http://parent-2'
        all[1].offsetStart == 500L
        all[1].open

        cleanup:
        lineage?.close()
    }

    def "topology switch is skipped when the new parent is unreachable"() {
        given: "broker connected to parent-1; probe returns false for parent-2"
        def registryClient = Mock(CloudRegistryClient)
        def pipeConnector = new FakePipeConnector()
        def lineage = new PipeLineageStore(tempDir)
        def manager = new TopologyManager(registryClient, pipeConnector, lineage, "http://registry", "node-1", tempDir.toString())
        manager.parentReachableProbe = { String url -> url.contains('parent-1') } as java.util.function.Predicate

        when: "first topology brings parent-1 up successfully"
        invokeHandleTopologyUpdate(manager, ["http://parent-1"])
        waitForConnect(pipeConnector, 1)
        Thread.sleep(50)

        and: "then registry returns an unreachable parent-2"
        invokeHandleTopologyUpdate(manager, ["http://parent-2"])
        Thread.sleep(50)

        then: "broker is still connected to parent-1; no disconnect, no new lineage row"
        pipeConnector.disconnectCalls == 0
        pipeConnector.connectCalls == 1
        manager.getCurrentParentUrl() == 'http://parent-1'
        lineage.allEntries().size() == 1
        lineage.allEntries()[0].parentUrl == 'http://parent-1'
        lineage.allEntries()[0].open

        cleanup:
        lineage?.close()
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
        long currentOffsetValue = 0L

        @Override
        long getCurrentOffset() {
            return currentOffsetValue
        }

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
