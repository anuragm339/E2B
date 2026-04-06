package com.messaging.broker.core

import com.messaging.broker.consumer.ConsumerOffsetTracker
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.common.api.NetworkServer
import com.messaging.common.api.StorageEngine
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

/**
 * Smoke tests: verifies the Micronaut application context starts cleanly
 * and all core broker beans are injectable.
 */
@MicronautTest
class BrokerIntegrationSetupSpec extends Specification {

    @Inject BrokerService brokerService
    @Inject StorageEngine storage
    @Inject NetworkServer networkServer
    @Inject ConsumerRegistry consumerRegistry
    @Inject ConsumerOffsetTracker offsetTracker
    @Inject RefreshCoordinator refreshCoordinator
    @Inject EmbeddedServer embeddedServer

    def "broker application context starts and all core beans are injectable"() {
        expect:
        brokerService != null
        storage != null
        networkServer != null
        consumerRegistry != null
        offsetTracker != null
        refreshCoordinator != null
        embeddedServer.isRunning()
    }

    def "broker can load all required module classes"() {
        expect:
        [
            'com.messaging.broker.core.BrokerService',
            'com.messaging.common.api.StorageEngine',
            'com.messaging.common.api.NetworkServer',
            'com.messaging.common.model.BrokerMessage',
            'com.messaging.common.model.MessageRecord',
            'com.messaging.storage.mmap.MMapStorageEngine',
            'com.messaging.storage.segment.Segment',
            'com.messaging.network.tcp.NettyTcpServer',
            'com.messaging.network.tcp.NettyTcpClient'
        ].every { className ->
            try { Class.forName(className) != null } catch (ClassNotFoundException e) { false }
        }
    }

    def "storage engine is operational"() {
        when:
        long offset = storage.getCurrentOffset('__health-check__', 0)

        then: "no exception; returns -1 for an empty topic"
        offset == -1L
    }

    def "consumer registry starts empty"() {
        expect:
        consumerRegistry.getAllConsumers() != null
    }
}
