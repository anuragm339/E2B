package com.messaging.broker.consumer

import spock.lang.Specification

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class FlushingPropertiesStoreSpec extends Specification {

    def "start and stop are idempotent and stop performs one final flush"() {
        given:
        def delegate = Mock(PropertiesFileStore)
        def scheduler = Mock(ScheduledExecutorService)
        def scheduledTask = Mock(ScheduledFuture)
        def store = new FlushingPropertiesStore(delegate, scheduler, "test-state", 100L)

        when:
        store.start()
        store.start()
        store.stop()
        store.stop()

        then:
        1 * scheduler.scheduleWithFixedDelay(
                _ as Runnable, 100L, 100L, TimeUnit.MILLISECONDS) >> scheduledTask
        1 * scheduledTask.cancel(false)
        1 * scheduler.shutdown()
        1 * scheduler.awaitTermination(5L, TimeUnit.SECONDS) >> true
        1 * delegate.persistToDisk()
    }

    def "periodic flush failure is contained so the scheduler can invoke it again"() {
        given:
        Runnable periodicFlush
        def delegate = Mock(PropertiesFileStore)
        def scheduler = Mock(ScheduledExecutorService)
        def store = new FlushingPropertiesStore(delegate, scheduler, "test-state", 100L)
        scheduler.scheduleWithFixedDelay(
                _ as Runnable, 100L, 100L, TimeUnit.MILLISECONDS) >> {
            arguments ->
                periodicFlush = arguments[0] as Runnable
                Mock(ScheduledFuture)
        }
        delegate.persistToDisk() >>> [
                { throw new PropertiesStoreException("disk unavailable", new IOException("boom")) },
                { }
        ]

        when:
        store.start()
        periodicFlush.run()
        periodicFlush.run()

        then:
        noExceptionThrown()
        2 * delegate.persistToDisk()
    }
}
