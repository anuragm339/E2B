package com.messaging.client.support

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Minimal raw TCP server that speaks the broker binary protocol:
 *   [Type:1B][MessageId:8B][PayloadLength:4B][Payload:variable]
 *
 * Used by ClientConsumerManagerIntegrationSpec as a stand-in for the real broker.
 * On receiving SUBSCRIBE it immediately sends back an ACK so ClientConsumerManager
 * can finish its startup sequence.
 */
class MockBrokerServer {

    // Protocol byte codes (mirrors BrokerMessage.MessageType)
    static final byte TYPE_DATA      = (byte) 0x01
    static final byte TYPE_ACK       = (byte) 0x02
    static final byte TYPE_SUBSCRIBE = (byte) 0x03
    static final byte TYPE_RESET     = (byte) 0x05
    static final byte TYPE_READY     = (byte) 0x06
    static final byte TYPE_RESET_ACK = (byte) 0x0B
    static final byte TYPE_READY_ACK = (byte) 0x0C

    ServerSocket serverSocket
    int port

    final List<Socket>       clients       = new CopyOnWriteArrayList<>()
    final List<Byte>         receivedTypes = new CopyOnWriteArrayList<>()
    final AtomicInteger      subscribeCount = new AtomicInteger(0)
    final CountDownLatch     firstSubscribe = new CountDownLatch(1)

    // =========================================================================
    // Lifecycle
    // =========================================================================

    void start() {
        serverSocket = new ServerSocket(0)
        port = serverSocket.localPort
        Thread t = new Thread({ acceptLoop() }, 'mock-broker-accept')
        t.daemon = true
        t.start()
    }

    void stop() {
        clients.each { try { it.close() } catch (Exception ignored) {} }
        try { serverSocket.close() } catch (Exception ignored) {}
    }

    // =========================================================================
    // Internal: accept + read loops
    // =========================================================================

    private void acceptLoop() {
        while (!serverSocket.closed) {
            try {
                Socket client = serverSocket.accept()
                clients.add(client)
                Thread t = new Thread({ readLoop(client) }, 'mock-broker-read')
                t.daemon = true
                t.start()
            } catch (Exception ignored) { break }
        }
    }

    private void readLoop(Socket client) {
        DataInputStream dis = new DataInputStream(client.inputStream)
        try {
            while (!client.closed && !serverSocket.closed) {
                byte typeCode      = dis.readByte()
                long messageId     = dis.readLong()
                int  payloadLen    = dis.readInt()
                byte[] payload     = payloadLen > 0 ? new byte[payloadLen] : new byte[0]
                if (payloadLen > 0) dis.readFully(payload)

                receivedTypes.add(typeCode)

                if (typeCode == TYPE_SUBSCRIBE) {
                    subscribeCount.incrementAndGet()
                    firstSubscribe.countDown()
                    sendMessage(client, TYPE_ACK, messageId, new byte[0])
                }
            }
        } catch (Exception ignored) {}
    }

    // =========================================================================
    // API: send to all connected clients
    // =========================================================================

    void broadcast(byte typeCode, byte[] payload) {
        clients.each { client ->
            sendMessage(client, typeCode, System.currentTimeMillis(), payload)
        }
    }

    private synchronized void sendMessage(Socket client, byte typeCode, long messageId, byte[] payload) {
        try {
            DataOutputStream dos = new DataOutputStream(client.outputStream)
            dos.writeByte(typeCode)
            dos.writeLong(messageId)
            dos.writeInt(payload.length)
            if (payload.length > 0) dos.write(payload)
            dos.flush()
        } catch (Exception ignored) {}
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    boolean awaitFirstSubscribe(long timeout = 10, TimeUnit unit = TimeUnit.SECONDS) {
        firstSubscribe.await(timeout, unit)
    }

    boolean hasReceived(byte typeCode) {
        receivedTypes.contains(typeCode)
    }
}
