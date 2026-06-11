package com.messaging.broker.config

import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit

class ExecutorFactorySpec extends Specification {

    def "bounded ACK executor rejects when its worker and queue are full"() {
        given:
        def executor = new ExecutorFactory().ackExecutor(1, 1)
        def running = new CountDownLatch(1)
        def release = new CountDownLatch(1)
        executor.execute {
            running.countDown()
            release.await()
        }
        assert running.await(2, TimeUnit.SECONDS)
        executor.execute { }

        when:
        executor.execute { }

        then:
        thrown(RejectedExecutionException)

        cleanup:
        release.countDown()
        executor.shutdownNow()
    }

    def "executor configuration rejects non-positive sizes"() {
        when:
        new ExecutorFactory().storageExecutor(0, 10)

        then:
        thrown(IllegalArgumentException)

        when:
        new ExecutorFactory().ackStorageExecutor(1, 0)

        then:
        thrown(IllegalArgumentException)
    }
}
