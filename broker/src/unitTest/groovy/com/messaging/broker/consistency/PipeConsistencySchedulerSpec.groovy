package com.messaging.broker.consistency

import com.messaging.broker.core.TopologyManager
import com.messaging.broker.monitoring.MemoryMonitor
import spock.lang.Specification

import java.util.concurrent.Executors

class PipeConsistencySchedulerSpec extends Specification {

    PipeConsistencyService service = Mock()
    TopologyManager topology = Mock()
    MemoryMonitor memory = Mock()
    def directExecutor = Executors.newSingleThreadExecutor()

    def cleanup() {
        directExecutor.shutdownNow()
    }

    private PipeConsistencyScheduler scheduler(boolean enabled = true, String target = 'parent') {
        new PipeConsistencyScheduler(service, topology, memory, directExecutor, enabled, target)
    }

    def "scheduled tick runs an all-topics check against the parent"() {
        given:
        topology.getCurrentParentUrl() >> 'http://parent:8081'
        memory.isMemoryPressureHigh() >> false
        service.isRunning() >> false

        when:
        scheduler().runScheduledCheck()
        directExecutor.shutdown()
        directExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)

        then:
        1 * service.runCheck('all', 'parent') >> []
    }

    def "skips with a reason instead of queueing"() {
        given:
        service.isRunning() >> running
        topology.getCurrentParentUrl() >> parentUrl
        memory.isMemoryPressureHigh() >> pressure

        expect:
        scheduler(scheduleEnabled).skipReason() == reason

        where:
        scheduleEnabled | running | parentUrl       | pressure | reason
        false           | false   | 'http://p:8081' | false    | 'schedule_disabled'
        true            | true    | 'http://p:8081' | false    | 'already_running'
        true            | false   | null            | false    | 'no_parent_offline'
        true            | false   | 'http://p:8081' | true     | 'memory_pressure'
        true            | false   | 'http://p:8081' | false    | null
    }

    def "cloud target does not require a parent to be assigned"() {
        given:
        topology.getCurrentParentUrl() >> null
        memory.isMemoryPressureHigh() >> false
        service.isRunning() >> false

        expect:
        scheduler(true, 'cloud').skipReason() == null
    }

    def "a throwing check is contained and logged, not propagated to the pool"() {
        given:
        topology.getCurrentParentUrl() >> 'http://parent:8081'
        memory.isMemoryPressureHigh() >> false
        service.isRunning() >> false
        service.runCheck('all', 'parent') >> { throw new RuntimeException('index exploded') }

        when:
        scheduler().runScheduledCheck()
        directExecutor.shutdown()

        then:
        directExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
        noExceptionThrown()
    }
}
