package com.messaging.broker.core

import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class ShutdownCoordinatorSpec extends Specification {

    ExecutorService ackExecutor
    ExecutorService storageExecutor
    ScheduledExecutorService consumerScheduler
    ScheduledExecutorService dataRefreshScheduler
    ScheduledExecutorService flushScheduler
    ShutdownCoordinator coordinator

    def setup() {
        ackExecutor = Mock(ExecutorService)
        storageExecutor = Mock(ExecutorService)
        consumerScheduler = Mock(ScheduledExecutorService)
        dataRefreshScheduler = Mock(ScheduledExecutorService)
        flushScheduler = Mock(ScheduledExecutorService)

        coordinator = new ShutdownCoordinator(ackExecutor, storageExecutor, consumerScheduler, dataRefreshScheduler, flushScheduler)
    }

    def "should shutdown all executors gracefully"() {
        given:
        consumerScheduler.awaitTermination(_, _) >> true
        dataRefreshScheduler.awaitTermination(_, _) >> true
        flushScheduler.awaitTermination(_, _) >> true
        ackExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true

        when:
        coordinator.shutdown()

        then:
        1 * dataRefreshScheduler.shutdown()
        1 * dataRefreshScheduler.awaitTermination(10, TimeUnit.SECONDS)

        1 * flushScheduler.shutdown()
        1 * flushScheduler.awaitTermination(10, TimeUnit.SECONDS)

        1 * ackExecutor.shutdown()
        1 * ackExecutor.awaitTermination(10, TimeUnit.SECONDS)
    }

    def "should force shutdown if graceful shutdown times out"() {
        given:
        consumerScheduler.awaitTermination(_, _) >> true
        dataRefreshScheduler.awaitTermination(10, TimeUnit.SECONDS) >> false
        dataRefreshScheduler.awaitTermination(5, TimeUnit.SECONDS) >> true
        dataRefreshScheduler.shutdownNow() >> []

        flushScheduler.awaitTermination(_, _) >> true
        ackExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true

        when:
        coordinator.shutdown()

        then:
        1 * dataRefreshScheduler.shutdown()
        1 * dataRefreshScheduler.shutdownNow()
    }

    def "should handle interrupted shutdown"() {
        given:
        consumerScheduler.awaitTermination(_, _) >> { throw new InterruptedException() }
        dataRefreshScheduler.awaitTermination(_, _) >> true
        flushScheduler.awaitTermination(_, _) >> true
        ackExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true

        when:
        coordinator.shutdown()

        then:
        1 * consumerScheduler.shutdown()
        1 * consumerScheduler.shutdownNow()
        Thread.currentThread().isInterrupted()

        cleanup:
        Thread.interrupted() // Clear interrupt flag
    }

    def "should log dropped tasks during forced shutdown"() {
        given:
        def droppedTasks = [Mock(Runnable), Mock(Runnable)]
        consumerScheduler.awaitTermination(_, _) >> true
        dataRefreshScheduler.awaitTermination(_, _) >> true
        flushScheduler.awaitTermination(_, _) >> true
        ackExecutor.awaitTermination(10, TimeUnit.SECONDS) >> false
        ackExecutor.awaitTermination(5, TimeUnit.SECONDS) >> true
        ackExecutor.shutdownNow() >> droppedTasks
        storageExecutor.awaitTermination(_, _) >> true

        when:
        coordinator.shutdown()

        then:
        1 * ackExecutor.shutdownNow() >> droppedTasks
    }
}
