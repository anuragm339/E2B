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

    def "pauseForWipe cancels the flush, drops in-memory state, and rejects writes/flush"() {
        given:
        def delegate = Mock(PropertiesFileStore)
        def scheduler = Mock(ScheduledExecutorService)
        def scheduledTask = Mock(ScheduledFuture)
        def store = new FlushingPropertiesStore(delegate, scheduler, "test-state", 100L)
        scheduler.scheduleWithFixedDelay(_ as Runnable, 100L, 100L, TimeUnit.MILLISECONDS) >> scheduledTask
        store.start()

        when:
        store.pauseForWipe()

        then: "periodic flush cancelled and in-memory state cleared"
        1 * scheduledTask.cancel(false)
        1 * delegate.clear()
        store.isPaused()

        when: "writes and flushes during the wipe window"
        store.put("k", "v")
        store.putAll([a: "1"])
        store.remove("k")
        store.flush()

        then: "all are dropped so the deleted file cannot be re-created with stale state"
        0 * delegate.put(_, _)
        0 * delegate.putAll(_)
        0 * delegate.remove(_)
        0 * delegate.flush()
        0 * delegate.persistToDisk()
    }

    def "resumeAfterWipe reloads from disk and re-arms the periodic flush"() {
        given:
        def delegate = Mock(PropertiesFileStore)
        def scheduler = Mock(ScheduledExecutorService)
        def store = new FlushingPropertiesStore(delegate, scheduler, "test-state", 100L)
        scheduler.scheduleWithFixedDelay(_ as Runnable, 100L, 100L, TimeUnit.MILLISECONDS) >> Mock(ScheduledFuture)
        store.start()
        store.pauseForWipe()

        when:
        store.resumeAfterWipe()

        then: "in-memory state re-synced to the wiped/restored file and writes flow again"
        1 * delegate.reload()
        !store.isPaused()

        when:
        store.put("k", "v")

        then:
        1 * delegate.put("k", "v")
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
