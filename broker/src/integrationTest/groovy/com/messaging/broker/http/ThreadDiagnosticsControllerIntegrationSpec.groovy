package com.messaging.broker.http

import com.messaging.broker.support.BrokerHttpSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest

import java.lang.management.ManagementFactory

@MicronautTest
class ThreadDiagnosticsControllerIntegrationSpec extends BrokerHttpSpecSupport {

    def "thread diagnostics endpoints return valid summary lists dump and resource views"() {
        when:
        def summary = get('/diagnostics/threads/summary')
        def topCpu = get('/diagnostics/threads/top-cpu')
        def topCpuLimited = get('/diagnostics/threads/top-cpu?limit=5')
        def topMemory = get('/diagnostics/threads/top-memory')
        def topMemoryLimited = get('/diagnostics/threads/top-memory?limit=3')
        def problematic = get('/diagnostics/threads/problematic')
        def dump = get('/diagnostics/threads/dump')
        def deadlocks = get('/diagnostics/threads/deadlocks')
        def byCategory = get('/diagnostics/threads/by-category')
        def resources = get('/diagnostics/threads/resources')
        def filteredResources = get('/diagnostics/threads/resources?category=Other')

        then:
        (json(summary).totalThreads as int) > 0
        json(topCpu) instanceof List
        (json(topCpuLimited) as List).size() <= 5
        json(topMemory) instanceof List
        (json(topMemoryLimited) as List).size() <= 3
        json(problematic) instanceof List
        dump.body().contains('Thread')
        json(deadlocks).deadlocked == false
        json(byCategory).categories instanceof List
        (json(resources).threadCount as int) > 0
        json(resources).threads instanceof List
        json(filteredResources).threads instanceof List
    }

    def "thread diagnostics thread-id lookup works for existing and missing threads"() {
        given:
        long liveId = ManagementFactory.getThreadMXBean().allThreadIds[0]

        when:
        def existing = get("/diagnostics/threads/${liveId}")
        def missing = get('/diagnostics/threads/999999999')

        then:
        existing.statusCode() == 200
        json(existing) != null
        missing.statusCode() == 200
        json(missing) != null
    }
}
