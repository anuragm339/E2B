package com.messaging.client

import com.messaging.common.api.MessageHandler
import com.messaging.common.api.NetworkClient
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

/**
 * Focused routing coverage for #13: a DATA batch that arrives on one topic:group connection must
 * be delivered ONLY to that group's handler — never to a different group's handler for the same
 * topic. Drives the private routing directly (no broker / Micronaut context needed); the no-arg
 * constructor fully initialises the ObjectMapper used to parse the batch payload.
 *
 * Also covers #1 (ACK-after-process): the BATCH_ACK is sent only AFTER every handler's handleBatch
 * succeeds, and is withheld when a handler throws so the broker redelivers.
 */
class ClientConsumerManagerRoutingSpec extends Specification {

    private static Map handlersMap(ClientConsumerManager manager) {
        def f = ClientConsumerManager.getDeclaredField("topicGroupToHandlers")
        f.setAccessible(true)
        (Map) f.get(manager)
    }

    private static Map connectionsMap(ClientConsumerManager manager) {
        def f = ClientConsumerManager.getDeclaredField("connectionsPerTopicGroup")
        f.setAccessible(true)
        (Map) f.get(manager)
    }

    private static void invokeHandleData(ClientConsumerManager manager, String topicGroup, BrokerMessage msg) {
        def m = ClientConsumerManager.getDeclaredMethod("handleDataMessage", String, BrokerMessage)
        m.setAccessible(true)
        m.invoke(manager, topicGroup, msg)
    }

    private static BrokerMessage dataBatch() {
        // Payload starts with '[' so it is parsed as a JSON batch; an empty batch still drives one
        // handleBatch call (matches the #13 routing test) without any deserialization risk.
        new BrokerMessage(BrokerMessage.MessageType.DATA, System.currentTimeMillis(), '[]'.bytes)
    }

    def "DATA on a topic:group connection routes only to that group's handler (#13)"() {
        given: "two groups subscribed to the SAME topic, each with its own handler"
        def manager = new ClientConsumerManager()
        MessageHandler handlerA = Mock()
        MessageHandler handlerB = Mock()

        def mapField = ClientConsumerManager.getDeclaredField("topicGroupToHandlers")
        mapField.setAccessible(true)
        Map map = (Map) mapField.get(manager)
        map.put("prices-v1:group-a", [handlerA])
        map.put("prices-v1:group-b", [handlerB])

        def handleData = ClientConsumerManager.getDeclaredMethod("handleDataMessage", String, BrokerMessage)
        handleData.setAccessible(true)
        def dataMsg = new BrokerMessage(BrokerMessage.MessageType.DATA, System.currentTimeMillis(), '[]'.bytes)

        when: "a batch arrives on group-a's connection"
        handleData.invoke(manager, "prices-v1:group-a", dataMsg)

        then: "only group-a's handler is invoked; group-b must NOT receive group-a's data"
        1 * handlerA.handleBatch(_)
        0 * handlerB.handleBatch(_)
    }

    def "BATCH_ACK is sent AFTER the handler processes the batch (#1)"() {
        given: "a handler and a connection wired for one topic:group"
        def manager = new ClientConsumerManager()
        MessageHandler handler = Mock()
        NetworkClient.Connection conn = Mock()
        conn.send(_) >> CompletableFuture.completedFuture(null)

        handlersMap(manager).put("prices-v1:group-a", [handler])
        connectionsMap(manager).put("prices-v1:group-a", conn)

        when:
        invokeHandleData(manager, "prices-v1:group-a", dataBatch())

        then: "the handler ran, and exactly one BATCH_ACK was sent on that connection"
        1 * handler.handleBatch(_)
        1 * conn.send({ BrokerMessage m -> m.type == BrokerMessage.MessageType.BATCH_ACK })
    }

    def "BATCH_ACK is WITHHELD when the handler throws — broker must redeliver (#1)"() {
        given: "a handler that fails to process the batch"
        def manager = new ClientConsumerManager()
        MessageHandler handler = Mock()
        NetworkClient.Connection conn = Mock()

        handlersMap(manager).put("prices-v1:group-a", [handler])
        connectionsMap(manager).put("prices-v1:group-a", conn)

        when:
        invokeHandleData(manager, "prices-v1:group-a", dataBatch())

        then: "the handler was attempted but NO ack is sent — the offset is not committed"
        1 * handler.handleBatch(_) >> { throw new RuntimeException("processing failed") }
        0 * conn.send(_)
    }
}
