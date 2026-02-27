package com.messaging.broker.consumer

import spock.lang.Specification
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentMap

class TopicFairSchedulerSpec extends Specification {

    def "scheduleWithKey keeps only one pending retry per delivery key"() {
        given:
        def scheduler = new TopicFairScheduler(1, 0)
        def pendingRetries = (ConcurrentMap) getPrivateField(scheduler, "pendingRetries")

        when: "scheduling multiple retries with the same key and no permits available"
        scheduler.scheduleWithKey("topic", "key", { }, 0, TimeUnit.MILLISECONDS)
        scheduler.scheduleWithKey("topic", "key", { }, 0, TimeUnit.MILLISECONDS)
        scheduler.scheduleWithKey("topic", "key", { }, 0, TimeUnit.MILLISECONDS)
        Thread.sleep(100)

        then: "only one pending retry is tracked"
        pendingRetries.size() == 1

        cleanup:
        scheduler.shutdown()
    }

    private static Object getPrivateField(Object target, String fieldName) {
        def field = target.class.getDeclaredField(fieldName)
        field.setAccessible(true)
        return field.get(target)
    }
}
