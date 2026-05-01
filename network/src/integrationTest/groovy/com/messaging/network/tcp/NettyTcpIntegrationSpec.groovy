package com.messaging.network.tcp

import com.messaging.common.api.NetworkServer
import com.messaging.common.model.BrokerMessage
import com.messaging.common.model.DeliveryBatch
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Shared
import spock.lang.Specification

import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Integration tests for NettyTcpServer and NettyTcpClient.
 *
 * Server and client are created manually (not via DI) so they can be closed and
 * restarted per-test without affecting any shared Micronaut context.
 * @MicronautTest is present for project-wide consistency.
 *
 * A single free port is allocated once per spec class; tests run sequentially so
 * there is no port collision within the spec.
 */
@MicronautTest
class NettyTcpIntegrationSpec extends Specification {

    int testPort   // fresh free port per test
    NettyTcpServer server
    List<BrokerMessage> serverReceivedMessages
    CountDownLatch serverMessageLatch

    def setup() {
        testPort = allocateFreePort()
        serverReceivedMessages = new CopyOnWriteArrayList<>()
    }

    def cleanup() {
        server?.shutdown()
    }

    // =========================================================================
    // Basic connectivity
    // =========================================================================

    def "server starts and accepts client connections"() {
        given:
        server = new NettyTcpServer(2, 8)

        when:
        server.start(testPort)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        then:
        connection != null
        connection.isAlive()

        cleanup:
        connection?.disconnect()
    }

    def "client sends message and server receives it"() {
        given:
        serverMessageLatch = new CountDownLatch(1)
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            serverReceivedMessages.add(m)
            serverMessageLatch.countDown()
        } as NetworkServer.MessageHandler)
        server.start(testPort)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when:
        def outgoing = brokerMsg(BrokerMessage.MessageType.DATA, 12345L, "test-payload".getBytes())
        connection.send(outgoing).get(5, TimeUnit.SECONDS)
        def received = serverMessageLatch.await(5, TimeUnit.SECONDS)

        then:
        received
        serverReceivedMessages.size() == 1
        serverReceivedMessages[0].getType() == BrokerMessage.MessageType.DATA
        serverReceivedMessages[0].getMessageId() == 12345L
        new String(serverReceivedMessages[0].getPayload()) == "test-payload"

        cleanup:
        connection?.disconnect()
    }

    def "server sends message and client receives it"() {
        given:
        def capturedClientId = null
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            if (capturedClientId == null) capturedClientId = clientId
        } as NetworkServer.MessageHandler)
        server.start(testPort)

        def clientReceived = new CopyOnWriteArrayList<BrokerMessage>()
        def latch = new CountDownLatch(1)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        connection.onMessage { incoming ->
            clientReceived.add(incoming)
            latch.countDown()
        }
        connection.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 1L, "register".getBytes())).get(5, TimeUnit.SECONDS)
        Thread.sleep(200)

        when:
        server.send(capturedClientId, brokerMsg(BrokerMessage.MessageType.ACK, 99999L, "ack-payload".getBytes())).get(5, TimeUnit.SECONDS)
        def received = latch.await(5, TimeUnit.SECONDS)

        then:
        received
        clientReceived[0].getType() == BrokerMessage.MessageType.ACK
        clientReceived[0].getMessageId() == 99999L
        new String(clientReceived[0].getPayload()) == "ack-payload"

        cleanup:
        connection?.disconnect()
    }

    def "bidirectional message exchange"() {
        given:
        serverMessageLatch = new CountDownLatch(3)
        def capturedClientId = null
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            if (capturedClientId == null) capturedClientId = clientId
            serverReceivedMessages.add(m)
            serverMessageLatch.countDown()
        } as NetworkServer.MessageHandler)
        server.start(testPort)

        def clientReceived = new CopyOnWriteArrayList<BrokerMessage>()
        def clientLatch = new CountDownLatch(2)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        connection.onMessage { incoming ->
            clientReceived.add(incoming)
            clientLatch.countDown()
        }

        when:
        (1..3).each { i ->
            connection.send(brokerMsg(BrokerMessage.MessageType.DATA, i as long, "client-msg-${i}".getBytes())).get(5, TimeUnit.SECONDS)
        }
        Thread.sleep(300)
        (1..2).each { i ->
            server.send(capturedClientId, brokerMsg(BrokerMessage.MessageType.DATA, 100 + i as long, "server-msg-${i}".getBytes())).get(5, TimeUnit.SECONDS)
        }
        def serverOk = serverMessageLatch.await(5, TimeUnit.SECONDS)
        def clientOk = clientLatch.await(5, TimeUnit.SECONDS)

        then:
        serverOk && clientOk
        serverReceivedMessages.size() == 3
        clientReceived.size() == 2

        cleanup:
        connection?.disconnect()
    }

    def "client reconnects after disconnect"() {
        given:
        server = new NettyTcpServer(2, 8)
        server.start(testPort)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when:
        connection.disconnect()
        Thread.sleep(200)

        then:
        !connection.isAlive()

        when:
        def newConnection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        then:
        newConnection != null
        newConnection.isAlive()

        cleanup:
        newConnection?.disconnect()
    }

    def "multiple clients can connect simultaneously"() {
        given:
        def connectedClients = new CopyOnWriteArrayList<String>()
        serverMessageLatch = new CountDownLatch(3)
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            if (!connectedClients.contains(clientId)) connectedClients.add(clientId)
            serverMessageLatch.countDown()
        } as NetworkServer.MessageHandler)
        server.start(testPort)

        when:
        def conn1 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        def conn2 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        def conn3 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        [conn1, conn2, conn3].each { c ->
            c.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 1L, "ping".getBytes())).get(5, TimeUnit.SECONDS)
        }
        serverMessageLatch.await(5, TimeUnit.SECONDS)

        then:
        connectedClients.size() == 3
        conn1.isAlive() && conn2.isAlive() && conn3.isAlive()

        cleanup:
        [conn1, conn2, conn3].each { it?.disconnect() }
    }

    def "large message (10KB) is transmitted intact"() {
        given:
        serverMessageLatch = new CountDownLatch(1)
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            serverReceivedMessages.add(m)
            serverMessageLatch.countDown()
        } as NetworkServer.MessageHandler)
        server.start(testPort)
        def client = new NettyTcpClient()
        def connection = client.connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when:
        def largePayload = new byte[10 * 1024]
        Arrays.fill(largePayload, (byte) 'X')
        connection.send(brokerMsg(BrokerMessage.MessageType.DATA, 77777L, largePayload)).get(5, TimeUnit.SECONDS)
        def received = serverMessageLatch.await(5, TimeUnit.SECONDS)

        then:
        received
        serverReceivedMessages[0].getPayload().length == 10 * 1024

        cleanup:
        connection?.disconnect()
    }

    // =========================================================================
    // Broadcast
    // =========================================================================

    def "broadcast sends message to all connected clients"() {
        given:
        server = new NettyTcpServer(2, 8)
        server.start(testPort)
        def received1 = new CopyOnWriteArrayList<BrokerMessage>()
        def received2 = new CopyOnWriteArrayList<BrokerMessage>()
        def latch = new CountDownLatch(2)
        def conn1 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        def conn2 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        conn1.onMessage { m -> received1.add(m); latch.countDown() }
        conn2.onMessage { m -> received2.add(m); latch.countDown() }
        Thread.sleep(200)

        when:
        server.broadcast(brokerMsg(BrokerMessage.MessageType.DATA, 555L, "broadcast-payload".getBytes()))
        def allReceived = latch.await(5, TimeUnit.SECONDS)

        then:
        allReceived
        received1.size() == 1 && received1[0].getMessageId() == 555L
        received2.size() == 1 && received2[0].getMessageId() == 555L

        cleanup:
        conn1?.disconnect()
        conn2?.disconnect()
    }

    // =========================================================================
    // closeConnection / getConnectedClients
    // =========================================================================

    def "closeConnection disconnects a specific client from server side"() {
        given:
        def capturedClientId = null
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            if (capturedClientId == null) capturedClientId = clientId
        } as NetworkServer.MessageHandler)
        server.start(testPort)
        def conn = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        conn.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 1L, "reg".getBytes())).get(5, TimeUnit.SECONDS)
        Thread.sleep(200)

        when:
        server.closeConnection(capturedClientId)
        Thread.sleep(200)

        then:
        !server.getConnectedClients().contains(capturedClientId)

        cleanup:
        conn?.disconnect()
    }

    def "closeConnection on unknown clientId is a no-op"() {
        given:
        server = new NettyTcpServer(2, 8)
        server.start(testPort)

        expect:
        server.closeConnection("unknown-client-does-not-exist")
        // No exception — silently ignored
    }

    def "getConnectedClients reflects current connection state"() {
        given:
        serverMessageLatch = new CountDownLatch(2)
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m -> serverMessageLatch.countDown() } as NetworkServer.MessageHandler)
        server.start(testPort)
        def conn1 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        def conn2 = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        conn1.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 1L, "ping".getBytes())).get(5, TimeUnit.SECONDS)
        conn2.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 2L, "ping".getBytes())).get(5, TimeUnit.SECONDS)
        serverMessageLatch.await(5, TimeUnit.SECONDS)

        expect:
        server.getConnectedClients().size() == 2

        cleanup:
        conn1?.disconnect()
        conn2?.disconnect()
    }

    // =========================================================================
    // Disconnect handler
    // =========================================================================

    def "disconnect handler is notified when a client closes its connection"() {
        given:
        def disconnected = new CopyOnWriteArrayList<String>()
        def disconnectLatch = new CountDownLatch(1)
        def capturedClientId = null
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, m ->
            if (capturedClientId == null) capturedClientId = clientId
        } as NetworkServer.MessageHandler)
        server.registerDisconnectHandler({ clientId ->
            disconnected.add(clientId)
            disconnectLatch.countDown()
        } as NetworkServer.DisconnectHandler)
        server.start(testPort)
        def conn = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        conn.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 1L, "reg".getBytes())).get(5, TimeUnit.SECONDS)
        Thread.sleep(200)
        def knownId = capturedClientId

        when:
        conn.disconnect()
        def notified = disconnectLatch.await(5, TimeUnit.SECONDS)

        then:
        notified
        disconnected.contains(knownId)
    }

    // =========================================================================
    // Error paths
    // =========================================================================

    def "send to unknown clientId returns a failed future immediately"() {
        given:
        server = new NettyTcpServer(2, 8)
        server.start(testPort)

        when:
        def future = server.send("no-such-client", brokerMsg(BrokerMessage.MessageType.DATA, 1L, new byte[0]))

        then:
        future.isCompletedExceptionally()
    }

    def "TcpConnection.waitForAck returns true when server echoes ACK"() {
        given:
        serverMessageLatch = new CountDownLatch(1)
        def capturedClientId = null
        server = new NettyTcpServer(2, 8)
        server.registerHandler({ clientId, incoming ->
            if (capturedClientId == null) capturedClientId = clientId
            serverMessageLatch.countDown()
            server.send(clientId, brokerMsg(BrokerMessage.MessageType.ACK, incoming.getMessageId(), new byte[0]))
        } as NetworkServer.MessageHandler)
        server.start(testPort)
        def connection = (NettyTcpClient.TcpConnection) new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when:
        connection.send(brokerMsg(BrokerMessage.MessageType.DATA, 8888L, "payload".getBytes()))
        boolean received = serverMessageLatch.await(5, TimeUnit.SECONDS)
        boolean acked = connection.waitForAck(8888L, 5000L)

        then:
        received
        acked

        cleanup:
        connection?.disconnect()
    }

    def "TcpConnection.waitForAck returns false when ACK never arrives"() {
        given:
        server = new NettyTcpServer(2, 8)
        server.start(testPort)
        def connection = (NettyTcpClient.TcpConnection) new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)

        when:
        boolean acked = connection.waitForAck(9999L, 300L)  // 300ms — no ACK will come

        then:
        !acked

        cleanup:
        connection?.disconnect()
    }

    // =========================================================================
    // sendBatch — zero-copy batch delivery (server → client)
    // =========================================================================

    def "sendBatch delivers batch records and client responds with BATCH_ACK"() {
        given:
        def capturedClientId = null
        def registerLatch  = new CountDownLatch(1)
        def batchAckLatch  = new CountDownLatch(1)
        def receivedAcks   = new CopyOnWriteArrayList<BrokerMessage>()

        server = new NettyTcpServer(2, 8)
        // First handler: capture clientId on SUBSCRIBE message
        server.registerHandler({ clientId, incoming ->
            if (capturedClientId == null) {
                capturedClientId = clientId
                registerLatch.countDown()
            }
        } as NetworkServer.MessageHandler)
        // Second handler: capture the BATCH_ACK that the client sends back
        server.registerHandler({ clientId, incoming ->
            if (incoming.getType() == BrokerMessage.MessageType.BATCH_ACK) {
                receivedAcks.add(incoming)
                batchAckLatch.countDown()
            }
        } as NetworkServer.MessageHandler)
        server.start(testPort)

        def conn = new NettyTcpClient().connect("localhost", testPort).get(5, TimeUnit.SECONDS)
        conn.send(brokerMsg(BrokerMessage.MessageType.SUBSCRIBE, 1L, "reg".getBytes())).get(5, TimeUnit.SECONDS)
        registerLatch.await(5, TimeUnit.SECONDS)

        when:
        def batch = stubBatch("batch-topic", [
            [key: "k1", data: '{"v":1}'],
            [key: "k2", data: '{"v":2}']
        ])
        server.sendBatch(capturedClientId, "batch-group", batch).get(5, TimeUnit.SECONDS)
        boolean gotAck = batchAckLatch.await(5, TimeUnit.SECONDS)

        then:
        gotAck
        !receivedAcks.isEmpty()
        receivedAcks[0].getType() == BrokerMessage.MessageType.BATCH_ACK

        cleanup:
        conn?.disconnect()
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static BrokerMessage brokerMsg(BrokerMessage.MessageType type, long id, byte[] payload) {
        def m = new BrokerMessage()
        m.setType(type)
        m.setMessageId(id)
        m.setPayload(payload)
        return m
    }

    /**
     * Creates an in-memory DeliveryBatch in the "unified" binary format that
     * ZeroCopyBatchDecoder.decodeZeroCopyBatch() expects:
     *   [keyLen:4][key:var][eventType:1][dataLen:4][data:var][timestamp:8]
     */
    private static DeliveryBatch stubBatch(String topic, List<Map<String, String>> records) {
        def baos = new ByteArrayOutputStream()
        def dos  = new DataOutputStream(baos)
        records.each { r ->
            byte[] keyBytes  = r.key.getBytes('UTF-8')
            byte[] dataBytes = r.data.getBytes('UTF-8')
            dos.writeInt(keyBytes.length)
            dos.write(keyBytes)
            dos.writeByte((byte) 'M')   // EventType = MESSAGE
            dos.writeInt(dataBytes.length)
            dos.write(dataBytes)
            dos.writeLong(System.currentTimeMillis())
        }
        dos.flush()
        final byte[] bytes = baos.toByteArray()
        final int rc = records.size()

        return new DeliveryBatch() {
            @Override String  getTopic()       { topic }
            @Override int     getRecordCount() { rc }
            @Override long    getTotalBytes()  { bytes.length }
            @Override long    getFirstOffset() { 0L }
            @Override long    getLastOffset()  { rc - 1L }
            @Override boolean isEmpty()        { rc == 0 }
            @Override
            long transferTo(WritableByteChannel target, long position) throws IOException {
                def buf = ByteBuffer.wrap(bytes, (int) position, bytes.length - (int) position)
                return target.write(buf)
            }
            @Override void close() throws IOException {}
        }
    }

    private static int allocateFreePort() {
        def s = new ServerSocket(0)
        try { return s.localPort } finally { s.close() }
    }
}
