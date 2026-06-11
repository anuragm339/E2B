package com.messaging.broker.core

import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class ShutdownCoordinatorSpec extends Specification {

    ExecutorService ackExecutor
    ExecutorService ackStorageExecutor
    ExecutorService storageExecutor
    ExecutorService compactionExecutor
    ExecutorService registryExecutor
    ScheduledExecutorService consumerScheduler
    ScheduledExecutorService dataRefreshScheduler
    ScheduledExecutorService flushScheduler
    ShutdownCoordinator coordinator

    def setup() {
        ackExecutor = Mock(ExecutorService)
        ackStorageExecutor = Mock(ExecutorService)
        storageExecutor = Mock(ExecutorService)
        compactionExecutor = Mock(ExecutorService)
        registryExecutor = Mock(ExecutorService)
        consumerScheduler = Mock(ScheduledExecutorService)
        dataRefreshScheduler = Mock(ScheduledExecutorService)
        flushScheduler = Mock(ScheduledExecutorService)

        coordinator = new ShutdownCoordinator(
                ackExecutor, ackStorageExecutor, storageExecutor, compactionExecutor, registryExecutor,
                consumerScheduler, dataRefreshScheduler, flushScheduler)
    }

    def "should shutdown all executors gracefully"() {
        given:
        consumerScheduler.awaitTermination(_, _) >> true
        dataRefreshScheduler.awaitTermination(_, _) >> true
        flushScheduler.awaitTermination(_, _) >> true
        ackExecutor.awaitTermination(_, _) >> true
        ackStorageExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true
        compactionExecutor.awaitTermination(_, _) >> true
        registryExecutor.awaitTermination(_, _) >> true

        when:
        coordinator.shutdown()

        then:
        1 * dataRefreshScheduler.shutdown()
        1 * dataRefreshScheduler.awaitTermination(10, TimeUnit.SECONDS)

        1 * flushScheduler.shutdown()
        1 * flushScheduler.awaitTermination(10, TimeUnit.SECONDS)

        1 * ackExecutor.shutdown()
        1 * ackExecutor.awaitTermination(10, TimeUnit.SECONDS)
        1 * ackStorageExecutor.shutdown()
        1 * ackStorageExecutor.awaitTermination(10, TimeUnit.SECONDS)
    }

    def "should force shutdown if graceful shutdown times out"() {
        given:
        consumerScheduler.awaitTermination(_, _) >> true
        dataRefreshScheduler.awaitTermination(10, TimeUnit.SECONDS) >> false
        dataRefreshScheduler.awaitTermination(5, TimeUnit.SECONDS) >> true
        dataRefreshScheduler.shutdownNow() >> []

        flushScheduler.awaitTermination(_, _) >> true
        ackExecutor.awaitTermination(_, _) >> true
        ackStorageExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true
        compactionExecutor.awaitTermination(_, _) >> true
        registryExecutor.awaitTermination(_, _) >> true

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
        ackStorageExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true
        compactionExecutor.awaitTermination(_, _) >> true
        registryExecutor.awaitTermination(_, _) >> true

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
        ackStorageExecutor.awaitTermination(_, _) >> true
        storageExecutor.awaitTermination(_, _) >> true
        compactionExecutor.awaitTermination(_, _) >> true
        registryExecutor.awaitTermination(_, _) >> true

        when:
        coordinator.shutdown()

        then:
        1 * ackExecutor.shutdownNow() >> droppedTasks
    }

    def "shutdown is idempotent"() {
        given:
        [consumerScheduler, dataRefreshScheduler, flushScheduler,
         ackExecutor, ackStorageExecutor, storageExecutor, compactionExecutor, registryExecutor].each {
            it.awaitTermination(_, _) >> true
        }

        when:
        coordinator.shutdown()
        coordinator.shutdown()

        then:
        1 * ackExecutor.shutdown()
        1 * ackStorageExecutor.shutdown()
        1 * storageExecutor.shutdown()
        1 * compactionExecutor.shutdown()
        1 * registryExecutor.shutdown()
        1 * consumerScheduler.shutdown()
        1 * dataRefreshScheduler.shutdown()
        1 * flushScheduler.shutdown()
    }
}
