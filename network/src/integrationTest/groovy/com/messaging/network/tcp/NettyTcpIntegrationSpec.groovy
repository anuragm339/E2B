package com.messaging.network.tcp

import com.messaging.common.api.NetworkClient
import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification
import spock.lang.Shared

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Integration tests for Netty TCP Server and Client
 * Tests real network communication over localhost
 */
class NettyTcpIntegrationSpec extends Specification {

    @Shared
    int testPort = 19092  // Use different port to avoid conflicts

    NettyTcpServer server
    List<BrokerMessage> serverReceivedMessages
    CountDownLatch serverMessageLatch

    def setup() {
        serverReceivedMessages = new CopyOnWriteArrayList<>()
    }

    def cleanup() {
        server?.shutdown()
    }

    def "network integration: server starts and accepts client connections"() {
        given: "a Netty TCP server"
        server = new NettyTcpServer(2, 8)  // bossThreads, workerThreads

        when: "starting the server"
        server.start(testPort)

        and: "creating a client connection"
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        then: "client connects successfully"
        connection != null
        connection.isAlive()

        cleanup:
        connection?.disconnect()
    }

    def "network integration: client sends message and server receives it"() {
        given: "a server with message handler"
        serverMessageLatch = new CountDownLatch(1)
        server = new NettyTcpServer(2, 8)
        server.registerHandler(new NetworkServer.MessageHandler() {
            @Override
            void handle(String clientId, BrokerMessage msg) {
                serverReceivedMessages.add(msg)
                serverMessageLatch.countDown()
            }
        })
        server.start(testPort)

        and: "a connected client"
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when: "client sends a DATA message"
        def message = new BrokerMessage()
        message.setType(BrokerMessage.MessageType.DATA)
        message.setMessageId(12345L)
        message.setPayload("test-payload".getBytes())
        connection.send(message).get(5, TimeUnit.SECONDS)

        and: "waiting for server to receive"
        def received = serverMessageLatch.await(5, TimeUnit.SECONDS)

        then: "server receives the message"
        received
        serverReceivedMessages.size() == 1
        serverReceivedMessages[0].getType() == BrokerMessage.MessageType.DATA
        serverReceivedMessages[0].getMessageId() == 12345L
        new String(serverReceivedMessages[0].getPayload()) == "test-payload"

        cleanup:
        connection?.disconnect()
    }

    def "network integration: server sends message and client receives it"() {
        given: "a server"
        def clientIdHolder = []
        server = new NettyTcpServer(2, 8)
        server.registerHandler(new NetworkServer.MessageHandler() {
            @Override
            void handle(String clientId, BrokerMessage msg) {
                // Capture clientId on first message
                if (!clientIdHolder) {
                    clientIdHolder << clientId
                }
            }
        })
        server.start(testPort)

        and: "a client with message handler"
        def clientReceivedMessages = new CopyOnWriteArrayList<BrokerMessage>()
        def clientMessageLatch = new CountDownLatch(1)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        connection.onMessage { msg ->
            clientReceivedMessages.add(msg)
            clientMessageLatch.countDown()
        }

        // Send a message to register clientId with server
        def regMessage = new BrokerMessage()
        regMessage.setType(BrokerMessage.MessageType.SUBSCRIBE)
        regMessage.setMessageId(1L)
        regMessage.setPayload("register".getBytes())
        connection.send(regMessage).get(5, TimeUnit.SECONDS)

        // Wait for clientId to be captured
        Thread.sleep(200)

        when: "server sends a message to client"
        def message = new BrokerMessage()
        message.setType(BrokerMessage.MessageType.ACK)
        message.setMessageId(99999L)
        message.setPayload("ack-payload".getBytes())
        server.send(clientIdHolder[0], message).get(5, TimeUnit.SECONDS)

        and: "waiting for client to receive"
        def received = clientMessageLatch.await(5, TimeUnit.SECONDS)

        then: "client receives the message"
        received
        clientReceivedMessages.size() == 1
        clientReceivedMessages[0].getType() == BrokerMessage.MessageType.ACK
        clientReceivedMessages[0].getMessageId() == 99999L
        new String(clientReceivedMessages[0].getPayload()) == "ack-payload"

        cleanup:
        connection?.disconnect()
    }

    def "network integration: bidirectional message exchange"() {
        given: "a server with message handler"
        serverMessageLatch = new CountDownLatch(3)
        def clientIdHolder = []
        server = new NettyTcpServer(2, 8)
        server.registerHandler(new NetworkServer.MessageHandler() {
            @Override
            void handle(String clientId, BrokerMessage msg) {
                if (!clientIdHolder) {
                    clientIdHolder << clientId
                }
                serverReceivedMessages.add(msg)
                serverMessageLatch.countDown()
            }
        })
        server.start(testPort)

        and: "a client with message handler"
        def clientReceivedMessages = new CopyOnWriteArrayList<BrokerMessage>()
        def clientMessageLatch = new CountDownLatch(2)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        connection.onMessage { msg ->
            clientReceivedMessages.add(msg)
            clientMessageLatch.countDown()
        }

        when: "client sends 3 messages to server"
        (1..3).each { i ->
            def message = new BrokerMessage()
            message.setType(BrokerMessage.MessageType.DATA)
            message.setMessageId(i)
            message.setPayload("client-msg-${i}".getBytes())
            connection.send(message).get(5, TimeUnit.SECONDS)
        }

        // Wait for server to receive and capture clientId
        Thread.sleep(300)

        and: "server sends 2 messages to client"
        (1..2).each { i ->
            def message = new BrokerMessage()
            message.setType(BrokerMessage.MessageType.DATA)  // Changed from ACK to DATA to avoid ACK handling
            message.setMessageId(100 + i)
            message.setPayload("server-msg-${i}".getBytes())
            server.send(clientIdHolder[0], message).get(5, TimeUnit.SECONDS)
        }

        and: "waiting for all messages"
        def serverReceived = serverMessageLatch.await(5, TimeUnit.SECONDS)
        def clientReceived = clientMessageLatch.await(5, TimeUnit.SECONDS)

        then: "server receives all 3 client messages"
        serverReceived
        serverReceivedMessages.size() == 3
        serverReceivedMessages[0].getMessageId() == 1
        serverReceivedMessages[1].getMessageId() == 2
        serverReceivedMessages[2].getMessageId() == 3

        and: "client receives all 2 server messages"
        clientReceived
        clientReceivedMessages.size() == 2
        clientReceivedMessages[0].getMessageId() == 101
        clientReceivedMessages[1].getMessageId() == 102

        cleanup:
        connection?.disconnect()
    }

    def "network integration: client reconnects after disconnect"() {
        given: "a server"
        server = new NettyTcpServer(2, 8)
        server.start(testPort)

        and: "a client"
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when: "client disconnects"
        connection.disconnect()
        Thread.sleep(200)

        then: "client is disconnected"
        !connection.isAlive()

        when: "client reconnects"
        def newConnection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        then: "reconnection succeeds"
        newConnection != null
        newConnection.isAlive()

        cleanup:
        newConnection?.disconnect()
    }

    def "network integration: multiple clients can connect simultaneously"() {
        given: "a server"
        def connectedClients = new CopyOnWriteArrayList<String>()
        server = new NettyTcpServer(2, 8)
        server.registerHandler(new NetworkServer.MessageHandler() {
            @Override
            void handle(String clientId, BrokerMessage msg) {
                if (!connectedClients.contains(clientId)) {
                    connectedClients.add(clientId)
                }
            }
        })
        server.start(testPort)

        when: "3 clients connect and send messages"
        def client1 = new NettyTcpClient()
        def client2 = new NettyTcpClient()
        def client3 = new NettyTcpClient()

        def conn1 = client1.connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        def conn2 = client2.connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        def conn3 = client3.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        // Each client sends a message to register
        [conn1, conn2, conn3].each { conn ->
            def msg = new BrokerMessage()
            msg.setType(BrokerMessage.MessageType.SUBSCRIBE)
            msg.setMessageId(1L)
            msg.setPayload("register".getBytes())
            conn.send(msg).get(5, TimeUnit.SECONDS)
        }

        // Wait for all connections to register
        Thread.sleep(300)

        then: "server tracks all 3 clients"
        connectedClients.size() == 3
        conn1.isAlive()
        conn2.isAlive()
        conn3.isAlive()

        cleanup:
        conn1?.disconnect()
        conn2?.disconnect()
        conn3?.disconnect()
    }

    def "network integration: large message (10KB) transmission"() {
        given: "a server with message handler"
        serverMessageLatch = new CountDownLatch(1)
        server = new NettyTcpServer(2, 8)
        server.registerHandler(new NetworkServer.MessageHandler() {
            @Override
            void handle(String clientId, BrokerMessage msg) {
                serverReceivedMessages.add(msg)
                serverMessageLatch.countDown()
            }
        })
        server.start(testPort)

        and: "a connected client"
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when: "client sends a large 10KB message"
        def largePayload = new byte[10 * 1024]  // 10KB
        Arrays.fill(largePayload, (byte) 'X')
        def message = new BrokerMessage()
        message.setType(BrokerMessage.MessageType.DATA)
        message.setMessageId(77777L)
        message.setPayload(largePayload)
        connection.send(message).get(5, TimeUnit.SECONDS)

        and: "waiting for server to receive"
        def received = serverMessageLatch.await(5, TimeUnit.SECONDS)

        then: "server receives the large message intact"
        received
        serverReceivedMessages.size() == 1
        serverReceivedMessages[0].getPayload().length == 10 * 1024
        serverReceivedMessages[0].getPayload()[0] == (byte) 'X'

        cleanup:
        connection?.disconnect()
    }
}
