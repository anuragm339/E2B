package com.messaging.network.tcp

import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

class NettyTcpClientSpec extends Specification {

    def "waitForAck returns true when ack arrived before waiting"() {
        given:
        def connection = new NettyTcpClient.TcpConnection()

        when:
        connection.handleIncomingMessage(new BrokerMessage(BrokerMessage.MessageType.ACK, 99L, new byte[0]))

        then:
        connection.waitForAck(99L, 10L)
    }

    def "waitForAck returns false when no ack arrives before timeout"() {
        given:
        def connection = new NettyTcpClient.TcpConnection()

        expect:
        !connection.waitForAck(100L, 10L)
    }
}
