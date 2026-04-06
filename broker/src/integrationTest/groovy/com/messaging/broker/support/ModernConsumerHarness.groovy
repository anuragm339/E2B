package com.messaging.broker.support

import com.messaging.common.model.BrokerMessage
import com.messaging.network.tcp.NettyTcpClient

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

class ModernConsumerHarness implements AutoCloseable {

    final NettyTcpClient client
    final NettyTcpClient.TcpConnection connection
    final ConcurrentLinkedQueue<BrokerMessage> received

    private ModernConsumerHarness(
        NettyTcpClient client,
        NettyTcpClient.TcpConnection connection,
        ConcurrentLinkedQueue<BrokerMessage> received
    ) {
        this.client = client
        this.connection = connection
        this.received = received
    }

    static ModernConsumerHarness connect(String host, int port) {
        def client = new NettyTcpClient()
        def connection = client.connect(host, port).get(5, TimeUnit.SECONDS) as NettyTcpClient.TcpConnection
        def received = new ConcurrentLinkedQueue<BrokerMessage>()
        connection.onMessage { msg -> received.add(msg) }
        new ModernConsumerHarness(client, connection, received)
    }

    void send(BrokerMessage message) {
        connection.send(message).get(3, TimeUnit.SECONDS)
    }

    void subscribe(String topic, String group) {
        def payload = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString([
            topic: topic,
            group: group
        ])
        send(new BrokerMessage(BrokerMessage.MessageType.SUBSCRIBE, System.nanoTime(), payload.getBytes(StandardCharsets.UTF_8)))
    }

    void sendReadyAck(String topic, String group) {
        send(new BrokerMessage(
            BrokerMessage.MessageType.READY_ACK,
            System.nanoTime(),
            BrokerIntegrationHarness.buildGroupTopicPayload(topic, group)
        ))
    }

    void sendResetAck(String topic, String group) {
        send(new BrokerMessage(
            BrokerMessage.MessageType.RESET_ACK,
            System.nanoTime(),
            BrokerIntegrationHarness.buildGroupTopicPayload(topic, group)
        ))
    }

    boolean isConnected() {
        connection.isAlive()
    }

    @Override
    void close() {
        try {
            connection?.disconnect()
        } finally {
            client?.shutdown()
        }
    }
}
