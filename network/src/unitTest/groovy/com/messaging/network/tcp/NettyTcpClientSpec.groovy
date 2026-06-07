package com.messaging.network.tcp

import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentMap

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

    def "ACK registration and arrival can race without a false timeout"() {
        given:
        def connection = new NettyTcpClient.TcpConnection()
        def executor = Executors.newFixedThreadPool(2)

        expect:
        (1L..200L).every { messageId ->
            def start = new CountDownLatch(1)
            def waiter = executor.submit({
                start.await()
                connection.waitForAck(messageId, 1000L)
            } as Callable<Boolean>)
            def ack = executor.submit({
                start.await()
                connection.handleIncomingMessage(
                        new BrokerMessage(BrokerMessage.MessageType.ACK, messageId, new byte[0]))
                null
            } as Callable<Void>)
            start.countDown()
            ack.get(1, TimeUnit.SECONDS)
            waiter.get(1, TimeUnit.SECONDS)
        }

        cleanup:
        executor.shutdownNow()
    }

    def "early ACK retention is bounded"() {
        given:
        def connection = new NettyTcpClient.TcpConnection()

        when:
        (1L..10_100L).each { messageId ->
            connection.handleIncomingMessage(
                    new BrokerMessage(BrokerMessage.MessageType.ACK, messageId, new byte[0]))
        }

        then:
        ((ConcurrentMap) getPrivateField(connection, "pendingAcks")).size() <= 10_000
    }

    private static Object getPrivateField(Object target, String fieldName) {
        def field = target.class.getDeclaredField(fieldName)
        field.setAccessible(true)
        return field.get(target)
    }
}
