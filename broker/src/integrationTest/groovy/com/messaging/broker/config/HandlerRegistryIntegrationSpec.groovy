package com.messaging.broker.config

import com.messaging.broker.handler.BatchAckHandler
import com.messaging.broker.handler.CommitOffsetHandler
import com.messaging.broker.handler.DataHandler
import com.messaging.broker.handler.MessageHandlerRegistry
import com.messaging.broker.handler.ReadyAckHandler
import com.messaging.broker.handler.ResetAckHandler
import com.messaging.broker.handler.SubscribeHandler
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.nio.file.Files

@MicronautTest
class HandlerRegistryIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-config-handlers-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19100',
            'micronaut.server.port'  : '18090',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject MessageHandlerRegistry handlerRegistry

    def "handler registry contains all core broker message handlers"() {
        expect:
        handlerRegistry.hasHandler(messageType)
        handlerType.isInstance(handlerRegistry.getHandler(messageType))

        where:
        messageType                            || handlerType
        BrokerMessage.MessageType.DATA         || DataHandler
        BrokerMessage.MessageType.SUBSCRIBE    || SubscribeHandler
        BrokerMessage.MessageType.COMMIT_OFFSET|| CommitOffsetHandler
        BrokerMessage.MessageType.RESET_ACK    || ResetAckHandler
        BrokerMessage.MessageType.READY_ACK    || ReadyAckHandler
        BrokerMessage.MessageType.BATCH_ACK    || BatchAckHandler
    }

    def "handler registry returns null for unregistered types"() {
        expect:
        !handlerRegistry.hasHandler(BrokerMessage.MessageType.HEARTBEAT)
        handlerRegistry.getHandler(BrokerMessage.MessageType.HEARTBEAT) == null
    }
}
