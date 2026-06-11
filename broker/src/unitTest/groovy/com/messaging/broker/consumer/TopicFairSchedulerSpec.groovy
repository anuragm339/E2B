package com.messaging.broker.consumer

import spock.lang.Specification
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutionException

class TopicFairSchedulerSpec extends Specification {

    def "scheduleWithKey keeps only one pending retry per delivery key"() {
        given: "a scheduler with 2 threads and 1 in-flight permit per topic"
        def scheduler = new TopicFairScheduler(2, 1)
        def pendingRetries = (ConcurrentMap) getPrivateField(scheduler, "pendingRetries")

        and: "a blocker that holds the sole semaphore permit until released"
        def blockerStarted = new CountDownLatch(1)
        def blockerRelease = new CountDownLatch(1)
        scheduler.scheduleWithKey("topic", "blocker", {
            blockerStarted.countDown()
            blockerRelease.await(5, TimeUnit.SECONDS)
        }, 0, TimeUnit.MILLISECONDS)
        blockerStarted.await(5, TimeUnit.SECONDS)  // permit is now held

        when: "three retries are scheduled with the same delivery key while the permit is exhausted"
        scheduler.scheduleWithKey("topic", "key", { }, 0, TimeUnit.MILLISECONDS)
        scheduler.scheduleWithKey("topic", "key", { }, 0, TimeUnit.MILLISECONDS)
        scheduler.scheduleWithKey("topic", "key", { }, 0, TimeUnit.MILLISECONDS)
        Thread.sleep(200)

        then: "only one pending retry is tracked"
        pendingRetries.size() == 1

        cleanup:
        blockerRelease.countDown()
        scheduler.shutdown()
    }

    def "task failure remains observable through the returned future"() {
        given:
        def scheduler = new TopicFairScheduler(1, 1)

        when:
        def future = scheduler.scheduleWithKey(
                "topic", "key", { throw new IllegalStateException("boom") },
                0, TimeUnit.MILLISECONDS)
        future.get(2, TimeUnit.SECONDS)

        then:
        def failure = thrown(ExecutionException)
        failure.cause instanceof IllegalStateException

        cleanup:
        scheduler.shutdown()
    }

    private static Object getPrivateField(Object target, String fieldName) {
        def field = target.class.getDeclaredField(fieldName)
        field.setAccessible(true)
        return field.get(target)
    }
}
