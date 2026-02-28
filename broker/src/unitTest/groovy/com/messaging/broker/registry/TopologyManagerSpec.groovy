package com.messaging.broker.registry

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
        def manager = new TopologyManager(registryClient, pipeConnector, "http://registry", "node-1", tempDir.toString())
        manager.onMessageReceived({ MessageRecord record -> })

        when:
        invokeConnectToParent(manager, "http://parent-1")

        then:
        pipeConnector.connectCalls == 1
        pipeConnector.dataHandlerSet
        manager.getCurrentParentUrl() == "http://parent-1"
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
        void onDataReceived(java.util.function.Consumer<MessageRecord> handler) {
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
