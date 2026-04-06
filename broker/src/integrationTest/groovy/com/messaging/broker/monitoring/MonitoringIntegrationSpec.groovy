package com.messaging.broker.monitoring

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.nio.file.Files

/**
 * Integration tests for ThreadMonitor and MemoryMonitor.
 *
 * Both classes have @Scheduled methods that only fire every 10-30 seconds in
 * production.  These tests inject the beans and invoke the public (and scheduled)
 * methods directly to exercise the body without waiting for the scheduler.
 */
@MicronautTest
class MonitoringIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-monitor-').toAbsolutePath().toString()
        return [
            'broker.network.port'    : '19094',
            'micronaut.server.port'  : '18084',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject ThreadMonitor threadMonitor
    @Inject MemoryMonitor memoryMonitor

    // =========================================================================
    // ThreadMonitor — scheduled method
    // =========================================================================

    def "checkThreads does not throw and updates internal state"() {
        when:
        threadMonitor.checkThreads()

        then:
        noExceptionThrown()
    }

    // =========================================================================
    // ThreadMonitor — public API
    // =========================================================================

    def "getThreadDump returns a non-empty string containing 'Thread'"() {
        when:
        String dump = threadMonitor.getThreadDump()

        then:
        dump != null
        !dump.isEmpty()
        dump.contains('Thread')
    }

    def "getProblematicThreads returns a list (may be empty in healthy JVM)"() {
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
        given: "prime the previousStats cache with one call"
        threadMonitor.checkThreads()

        when: "a second call computes CPU deltas"
        threadMonitor.checkThreads()

        then:
        noExceptionThrown()
    }

    // =========================================================================
    // MemoryMonitor — scheduled method
    // =========================================================================

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

    // =========================================================================
    // MemoryMonitor — public API
    // =========================================================================

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

    def "suggestGC does not throw (no-op when below threshold)"() {
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
