package com.messaging.broker.core

import spock.lang.Specification

/**
 * Integration test setup verification for Broker module
 * Verifies that integration test infrastructure works
 */
class BrokerIntegrationSetupSpec extends Specification {

    def "broker integration test setup verification"() {
        expect: "basic Spock integration test infrastructure works"
        true
    }

    def "broker can load required classes"() {
        when: "attempting to load broker classes"
        def brokerServiceClass = Class.forName("com.messaging.broker.core.BrokerService")
        def storageEngineInterface = Class.forName("com.messaging.common.api.StorageEngine")
        def networkServerInterface = Class.forName("com.messaging.common.api.NetworkServer")

        then: "all classes load successfully"
        brokerServiceClass != null
        storageEngineInterface != null
        networkServerInterface != null
    }

    def "broker dependencies are accessible"() {
        when: "checking broker module dependencies"
        def commonClasses = [
            "com.messaging.common.model.BrokerMessage",
            "com.messaging.common.model.MessageRecord",
            "com.messaging.common.api.StorageEngine"
        ]

        def storageClasses = [
            "com.messaging.storage.mmap.MMapStorageEngine",
            "com.messaging.storage.segment.Segment"
        ]

        def networkClasses = [
            "com.messaging.network.tcp.NettyTcpServer",
            "com.messaging.network.tcp.NettyTcpClient"
        ]

        then: "all dependency classes are accessible"
        commonClasses.every { className ->
            try {
                Class.forName(className) != null
            } catch (ClassNotFoundException e) {
                false
            }
        }

        storageClasses.every { className ->
            try {
                Class.forName(className) != null
            } catch (ClassNotFoundException e) {
                false
            }
        }

        networkClasses.every { className ->
            try {
                Class.forName(className) != null
            } catch (ClassNotFoundException e) {
                false
            }
        }
    }
}
