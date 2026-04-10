package com.messaging.broker.consumer

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@MicronautTest
class TopicFairSchedulerIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-fair-scheduler-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19096',
            'micronaut.server.port'  : '18086',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject TopicFairScheduler fairScheduler

    def "topic fair scheduler executes work and tracks idle state correctly"() {
        given:
        def latch = new CountDownLatch(3)

        when:
        3.times { i ->
            fairScheduler.schedule("fs-topic-${i}", { latch.countDown() }, 0, TimeUnit.MILLISECONDS)
        }

        then:
        latch.await(3, TimeUnit.SECONDS)
        fairScheduler.getAvailablePermits('idle-topic') >= 1
        fairScheduler.getInFlightCount('never-scheduled-topic') == 0
    }

    def "topic fair scheduler allows different topics to run concurrently"() {
        given:
        def counter = new AtomicInteger(0)
        def latch = new CountDownLatch(4)

        when:
        ['ts-a', 'ts-b', 'ts-c', 'ts-d'].each { topic ->
            fairScheduler.schedule(topic, {
                counter.incrementAndGet()
                latch.countDown()
            }, 0, TimeUnit.MILLISECONDS)
        }

        then:
        latch.await(5, TimeUnit.SECONDS)
        counter.get() == 4
    }
}
