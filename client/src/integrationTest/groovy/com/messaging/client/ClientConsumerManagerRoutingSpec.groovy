package com.messaging.client

import com.messaging.common.api.MessageHandler
import com.messaging.common.model.BrokerMessage
import spock.lang.Specification

/**
 * Focused routing coverage for #13: a DATA batch that arrives on one topic:group connection must
 * be delivered ONLY to that group's handler — never to a different group's handler for the same
 * topic. Drives the private routing directly (no broker / Micronaut context needed); the no-arg
 * constructor fully initialises the ObjectMapper used to parse the batch payload.
 */
class ClientConsumerManagerRoutingSpec extends Specification {

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
}
