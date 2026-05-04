package com.messaging.broker.support

import io.micronaut.test.support.TestPropertyProvider
import spock.lang.Specification

import java.nio.file.Files

abstract class BrokerMonitoringSpecSupport extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-monitor-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19094',
            'micronaut.server.port'  : '18084',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }
}
