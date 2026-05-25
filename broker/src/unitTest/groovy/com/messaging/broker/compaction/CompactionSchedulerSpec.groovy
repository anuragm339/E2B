package com.messaging.broker.compaction

import com.messaging.common.api.StorageEngine
import com.messaging.storage.segment.Segment
import com.messaging.storage.segment.SegmentAccess
import com.messaging.storage.segment.SegmentManager
import spock.lang.Specification

import java.nio.file.Path

class CompactionSchedulerSpec extends Specification {

    def "compact() does nothing when disabled"() {
        given:
        def storage = Mock(StorageEngine)
        def segmentAccess = Mock(SegmentAccess)
        def checkpointStore = Mock(CompactionCheckpointStore)
        def planner = Mock(CompactionPlanner)
        def rewriter = Mock(CompactionRewriter)
        def compactionIndex = Mock(RocksDbCompactionIndex)
        def metrics = Mock(com.messaging.broker.monitoring.BrokerMetrics)

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                false, 7, 10)

        when:
        scheduler.compact()

        then:
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

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                true, 7, 10)

        when:
        scheduler.compact()

        then:
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

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                true, 7, 10)

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

        def scheduler = new CompactionScheduler(
                storage, segmentAccess, checkpointStore, planner, rewriter, compactionIndex, metrics,
                true, 7, 10)

        when:
        scheduler.compact()

        then:
        noExceptionThrown()
        1 * planner.selectDirtyWindow([], -1L, 10) >> []
    }
}
