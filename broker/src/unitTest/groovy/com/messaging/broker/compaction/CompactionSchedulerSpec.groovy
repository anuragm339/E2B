package com.messaging.broker.compaction

import com.messaging.common.api.StorageEngine
import com.messaging.broker.monitoring.MemoryMonitor
import com.messaging.storage.segment.Segment
import com.messaging.storage.segment.SegmentAccess
import com.messaging.storage.segment.SegmentManager
import spock.lang.Specification

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class CompactionSchedulerSpec extends Specification {

    def "process CPU load normalization accepts fractional and percentage JVM formats"() {
        expect:
        CompactionScheduler.normalizeProcessCpuLoad(raw) == normalized

        where:
        raw     || normalized
        -1.0d   || -1.0d
        0.42d   || 0.42d
        1.75d   || 0.0175d
        100.0d  || 1.0d
    }

    def "compact() does nothing when disabled"() {
        given:
        def storage = Mock(StorageEngine)
        def segmentAccess = Mock(SegmentAccess)
        def checkpointStore = Mock(CompactionCheckpointStore)
        def planner = Mock(CompactionPlanner)
        def rewriter = Mock(CompactionRewriter)
        def compactionIndex = Mock(RocksDbCompactionIndex)
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)
        def memoryMonitor = Stub(MemoryMonitor) {
            getHeapUsagePercent() >> 0.0d
            isMemoryPressureHigh() >> false
        }

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                memoryMonitor, { Runnable task -> task.run() } as java.util.concurrent.Executor,
                false, 7, 10, Integer.MAX_VALUE, 1, 1.0d, 1.0d)

        when:
        scheduler.compact()

        then:
        1 * metrics.recordCompactionSkipped("disabled")
        0 * storage.getTopicNames()
        0 * planner.selectDirtyWindow(_, _, _)
        0 * rewriter.rewrite(_, _, _, _, _, _)
    }

    def "compact() skips topic when dirty window is empty"() {
        given:
        def segmentManager = Mock(SegmentManager) {
            getInactiveSegments() >> []
            getDataDir() >> Path.of("/tmp/test")
        }
        def storage = Mock(StorageEngine) { getTopicNames() >> ["prices-v1"] }
        def segmentAccess = Mock(SegmentAccess) {
            getSegmentManager("prices-v1", 0) >> segmentManager
        }
        def checkpointStore = Mock(CompactionCheckpointStore) {
            loadCheckpoint("prices-v1", 0) >> -1L
        }
        def planner = Mock(CompactionPlanner) { selectDirtyWindow([], -1L, 10) >> [] }
        def rewriter = Mock(CompactionRewriter)
        def compactionIndex = Mock(RocksDbCompactionIndex)
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)
        def memoryMonitor = Stub(MemoryMonitor) {
            getHeapUsagePercent() >> 0.0d
            isMemoryPressureHigh() >> false
        }

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                memoryMonitor, { Runnable task -> task.run() } as java.util.concurrent.Executor,
                true, 7, 10, Integer.MAX_VALUE, 1, 1.0d, 1.0d)

        when:
        scheduler.compact()

        then:
        1 * metrics.recordCompactionSkipped("not_enough_sealed_segments")
        0 * rewriter.rewrite(_, _, _, _, _, _)
        0 * checkpointStore.saveCheckpoint(_, _, _)
    }

    def "compact() calls planner, rewriter, saves checkpoint, and records metrics"() {
        given:
        def segment = Mock(Segment) { getBaseOffset() >> 0L }
        def segmentManager = Mock(SegmentManager) {
            getInactiveSegments() >> [segment]
            getDataDir() >> Path.of("/tmp/test")
            getMaxSegmentSize() >> 1073741824L
        }
        def compactionIndex = Mock(RocksDbCompactionIndex)
        def storage = Mock(StorageEngine) { getTopicNames() >> ["prices-v1"] }
        def segmentAccess = Mock(SegmentAccess) {
            getSegmentManager("prices-v1", 0) >> segmentManager
        }
        def checkpointStore = Mock(CompactionCheckpointStore) {
            loadCheckpoint("prices-v1", 0) >> -1L
        }
        def planner = Mock(CompactionPlanner) {
            selectDirtyWindow([segment], -1L, 10) >> [segment]
        }
        def rewriter = Mock(CompactionRewriter) {
            rewrite([segment], "prices-v1", 0, segmentManager, compactionIndex, 7) >>
                    new CompactionRewriter.CompactionResult(5, 2, 1024L, 800L, 224L, 1, false)
        }
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)
        def memoryMonitor = Stub(MemoryMonitor) {
            getHeapUsagePercent() >> 0.0d
            isMemoryPressureHigh() >> false
        }

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                memoryMonitor, { Runnable task -> task.run() } as java.util.concurrent.Executor,
                true, 7, 10, Integer.MAX_VALUE, 1, 1.0d, 1.0d)

        when:
        scheduler.compact()

        then:
        1 * planner.selectDirtyWindow([segment], -1L, 10) >> [segment]
        1 * rewriter.rewrite([segment], "prices-v1", 0, segmentManager, compactionIndex, 7) >>
                new CompactionRewriter.CompactionResult(5, 2, 1024L, 800L, 224L, 1, false)
        1 * checkpointStore.saveCheckpoint("prices-v1", 0, 0L)
        1 * metrics.recordCompactionRun()
        1 * metrics.recordCompactionTopicRun("prices-v1", 5, 2, 1024L, 800L, 224L, 1)
        1 * metrics.markCompactionActive("prices-v1")
        1 * metrics.markCompactionComplete("prices-v1")
    }

    def "compact() continues to next topic when one topic throws"() {
        given:
        def storage = Mock(StorageEngine) { getTopicNames() >> ["topic-a", "topic-b"] }
        def segmentAccess = Mock(SegmentAccess) {
            getSegmentManager("topic-a", 0) >> { throw new RuntimeException("boom") }
            getSegmentManager("topic-b", 0) >> Mock(SegmentManager) {
                getInactiveSegments() >> []
                getDataDir() >> Path.of("/tmp/test")
            }
        }
        def checkpointStore = Mock(CompactionCheckpointStore) {
            loadCheckpoint(_, _) >> -1L
        }
        def planner = Mock(CompactionPlanner) { selectDirtyWindow(_, _, _) >> [] }
        def rewriter = Mock(CompactionRewriter)
        def compactionIndex = Mock(RocksDbCompactionIndex)
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)
        def memoryMonitor = Stub(MemoryMonitor) {
            getHeapUsagePercent() >> 0.0d
            isMemoryPressureHigh() >> false
        }

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                memoryMonitor, { Runnable task -> task.run() } as java.util.concurrent.Executor,
                true, 7, 10, Integer.MAX_VALUE, 1, 1.0d, 1.0d)

        when:
        scheduler.compact()

        then:
        noExceptionThrown()
        1 * metrics.recordCompactionSkipped("not_enough_sealed_segments")
    }

    def "manual trigger rejects overlap until the claimed run completes"() {
        given:
        def queuedTask = new AtomicReference<Runnable>()
        def executor = { Runnable task -> queuedTask.set(task) } as java.util.concurrent.Executor
        def storage = Mock(StorageEngine) { getTopicNames() >> [] }
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)
        def memoryMonitor = Stub(MemoryMonitor) {
            getHeapUsagePercent() >> 0.0d
            isMemoryPressureHigh() >> false
        }
        def scheduler = new CompactionScheduler(
                storage,
                Mock(SegmentAccess),
                Mock(CompactionCheckpointStore),
                Mock(CompactionPlanner),
                Mock(CompactionRewriter),
                Mock(RocksDbCompactionIndex),
                metrics,
                memoryMonitor,
                executor,
                true, 7, 10, Integer.MAX_VALUE, 1, 1.0d, 1.0d)

        when:
        def firstAccepted = scheduler.triggerAsync()

        then:
        firstAccepted
        scheduler.isCompactionRunning()

        when:
        def secondAccepted = scheduler.triggerAsync()

        then:
        !secondAccepted
        1 * metrics.recordCompactionSkipped("already_running")

        when:
        queuedTask.get().run()

        then:
        !scheduler.isCompactionRunning()
        1 * storage.getTopicNames() >> []
    }

    def "manual preparation is protected by the compaction single-flight guard"() {
        given:
        def queuedTask = new AtomicReference<Runnable>()
        def executor = { Runnable task -> queuedTask.set(task) } as java.util.concurrent.Executor
        def preparationCount = new AtomicInteger()
        def storage = Mock(StorageEngine) { getTopicNames() >> [] }
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)
        def scheduler = new CompactionScheduler(
                storage,
                Mock(SegmentAccess),
                Mock(CompactionCheckpointStore),
                Mock(CompactionPlanner),
                Mock(CompactionRewriter),
                Mock(RocksDbCompactionIndex),
                metrics,
                Stub(MemoryMonitor) {
                    getHeapUsagePercent() >> 0.0d
                    isMemoryPressureHigh() >> false
                },
                executor,
                true, 7, 10, Integer.MAX_VALUE, 1, 1.0d, 1.0d)

        when:
        def firstAccepted = scheduler.triggerAsync { preparationCount.incrementAndGet() }
        def secondAccepted = scheduler.triggerAsync { preparationCount.incrementAndGet() }

        then:
        firstAccepted
        !secondAccepted
        preparationCount.get() == 1
        1 * metrics.recordCompactionSkipped("already_running")

        when:
        queuedTask.get().run()

        then:
        !scheduler.isCompactionRunning()
        1 * storage.getTopicNames() >> []
    }
}
