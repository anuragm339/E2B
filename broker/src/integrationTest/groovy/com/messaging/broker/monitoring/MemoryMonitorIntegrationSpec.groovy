package com.messaging.broker.monitoring

import com.messaging.broker.support.BrokerMonitoringSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

@MicronautTest
class MemoryMonitorIntegrationSpec extends BrokerMonitoringSpecSupport {

    @Inject MemoryMonitor memoryMonitor

    def "checkMemory does not throw"() {
        when:
        memoryMonitor.checkMemory()

        then:
        noExceptionThrown()
    }

    def "two consecutive checkMemory calls exercise state-change logging"() {
        given:
        memoryMonitor.checkMemory()

        when:
        memoryMonitor.checkMemory()

        then:
        noExceptionThrown()
    }

    def "getHeapUsagePercent returns a value in (0, 1]"() {
        when:
        double usage = memoryMonitor.getHeapUsagePercent()

        then:
        usage > 0.0
        usage <= 1.0
    }

    def "getNonHeapUsagePercent returns a non-negative value"() {
        when:
        double usage = memoryMonitor.getNonHeapUsagePercent()

        then:
        usage >= 0.0
    }

    def "isMemoryPressureHigh returns false under normal test conditions"() {
        expect:
        !memoryMonitor.isMemoryPressureHigh()
    }

    def "isMemoryCritical returns false under normal test conditions"() {
        expect:
        !memoryMonitor.isMemoryCritical()
    }

    def "suggestGC does not throw"() {
        when:
        memoryMonitor.suggestGC()

        then:
        noExceptionThrown()
    }

    def "getMemoryStatus returns a populated MemoryStatus object"() {
        when:
        def status = memoryMonitor.getMemoryStatus()

        then:
        status != null
        status.heapUsed > 0
        status.heapMax > 0
        status.heapPercent > 0.0
    }
}
