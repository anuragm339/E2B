package com.messaging.broker.config

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import jakarta.inject.Named
import spock.lang.Specification

import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

@MicronautTest
class ExecutorFactoryIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-config-executors-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19101',
            'micronaut.server.port'  : '18091',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject @Named('ackExecutor') ExecutorService ackExecutor
    @Inject @Named('storageExecutor') ExecutorService storageExecutor
    @Inject @Named('consumerScheduler') ScheduledExecutorService consumerScheduler
    @Inject @Named('dataRefreshScheduler') ScheduledExecutorService dataRefreshScheduler
    @Inject @Named('flushScheduler') ScheduledExecutorService flushScheduler

    def "all configured executors are live and accept work"() {
        expect:
        runOn(ackExecutor)
        runOn(storageExecutor)
        runOn(consumerScheduler)
        runOn(dataRefreshScheduler)
        runOn(flushScheduler)
    }

    def "configured executors are distinct instances"() {
        expect:
        !ackExecutor.is(storageExecutor)
        !consumerScheduler.is(dataRefreshScheduler)
        !consumerScheduler.is(flushScheduler)
        !dataRefreshScheduler.is(flushScheduler)
    }

    private static boolean runOn(ExecutorService executor) {
        def latch = new CountDownLatch(1)
        executor.execute { latch.countDown() }
        latch.await(3, TimeUnit.SECONDS) && !executor.shutdown
    }

    private static boolean runOn(ScheduledExecutorService executor) {
        def latch = new CountDownLatch(1)
        executor.schedule({ latch.countDown() }, 0, TimeUnit.MILLISECONDS)
        latch.await(3, TimeUnit.SECONDS) && !executor.shutdown
    }
}
