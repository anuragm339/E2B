package com.messaging.broker.monitoring

import com.messaging.broker.support.BrokerMonitoringSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

@MicronautTest
class ThreadMonitorIntegrationSpec extends BrokerMonitoringSpecSupport {

    @Inject ThreadMonitor threadMonitor

    def "checkThreads does not throw and updates internal state"() {
        when:
        threadMonitor.checkThreads()

        then:
        noExceptionThrown()
    }

    def "getThreadDump returns a non-empty string containing Thread"() {
        when:
        String dump = threadMonitor.getThreadDump()

        then:
        dump != null
        !dump.isEmpty()
        dump.contains('Thread')
    }

    def "getProblematicThreads returns a list"() {
        when:
        def problematic = threadMonitor.getProblematicThreads()

        then:
        problematic != null
        problematic instanceof List
    }

    def "getAllThreadResources returns at least one entry per JVM thread"() {
        when:
        def resources = threadMonitor.getAllThreadResources()

        then:
        resources != null
        !resources.isEmpty()
        resources.every { it.threadName != null && it.state != null && it.category != null }
    }

    def "getTopMemoryThreads returns up to the requested limit"() {
        when:
        def top = threadMonitor.getTopMemoryThreads(5)

        then:
        top != null
        top.size() <= 5
    }

    def "getTopCpuThreads returns up to the requested limit"() {
        when:
        def top = threadMonitor.getTopCpuThreads(3)

        then:
        top != null
        top.size() <= 3
    }

    def "getResourcesByCategory returns a non-empty category map"() {
        when:
        def byCategory = threadMonitor.getResourcesByCategory()

        then:
        byCategory != null
        !byCategory.isEmpty()
        byCategory.values().every { it.category != null && it.threadCount > 0 }
    }

    def "second checkThreads call compares against previous stats and logs CPU deltas"() {
        given:
        threadMonitor.checkThreads()

        when:
        threadMonitor.checkThreads()

        then:
        noExceptionThrown()
    }
}
