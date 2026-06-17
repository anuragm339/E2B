package com.messaging.broker.http

import com.messaging.broker.consistency.PipeConsistencyService
import com.messaging.broker.consumer.ConsumerRegistry
import com.messaging.broker.consumer.ConsumerStateService
import com.messaging.broker.consumer.PendingAckStore
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.core.TopologyManager
import com.messaging.broker.monitoring.ErrorRecorder
import com.messaging.broker.monitoring.RefreshHistoryRecorder
import com.messaging.common.api.StorageEngine
import com.messaging.pipe.HttpPipeConnector
import com.messaging.storage.segment.Segment
import com.messaging.storage.segment.SegmentAccess
import com.messaging.storage.segment.SegmentManager
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

import java.nio.file.Path

/**
 * Covers the /admin/status/storage and /admin/status/storage/{topic} endpoints. Drives the
 * controller directly with mocked collaborators — the storage view is assembled purely from
 * in-memory SegmentManager/Segment accessors, so no broker/Micronaut context is needed.
 */
class StatusControllerStorageSpec extends Specification {

    StorageEngine storage = Mock()
    SegmentAccess segmentAccess = Mock()
    TopologyManager topology = Mock() { getNodeId() >> "broker-test" }

    StatusController controller = new StatusController(
            Mock(PipeConsistencyService), topology, Mock(ConsumerRegistry), storage, segmentAccess,
            Mock(PendingAckStore), Mock(HttpPipeConnector), Mock(MeterRegistry),
            Mock(ConsumerStateService), Mock(ErrorRecorder), Mock(RefreshCoordinator),
            Mock(RefreshHistoryRecorder), "ERROR")

    private SegmentManager managerWith(boolean hasActive, int sealedCount, long sealedBytes, long activeBytes) {
        def sm = Mock(SegmentManager)
        sm.getActiveSegment() >> (hasActive ? Mock(Segment) : null)
        sm.getSealedSegmentCount() >> sealedCount
        sm.getSealedSegmentBytes() >> sealedBytes
        sm.getActiveSegmentSizeBytes() >> activeBytes
        sm.getLargestSegmentBytes() >> Math.max(sealedBytes, activeBytes)
        sm.getMaxSegmentSize() >> 1073741824L
        sm.getPartition() >> 0
        sm.getDataDir() >> Path.of("/data/test")
        return sm
    }

    def "GET /storage rolls up per-topic offsets, segment counts and bytes (no disk IO)"() {
        given:
        storage.getTopicNames() >> ["prices-v1"]
        storage.getCurrentOffset("prices-v1", 0) >> 1000L
        storage.getMaxOffsetFromMetadata("prices-v1", 0) >> 990L
        storage.getEarliestOffset("prices-v1", 0) >> 0L
        segmentAccess.getSegmentManager("prices-v1", 0) >> managerWith(true, 2, 2048L, 512L)

        when:
        def resp = controller.storage(false)
        def json = new ObjectMapper().readValue(resp.body() as String, Map)

        then:
        resp.status.code == 200
        json.nodeId == "broker-test"
        json.topicCount == 1
        json.totalSegments == 3          // 2 sealed + 1 active
        json.totalBytes == 2560          // 2048 + 512
        json.diskSampled == false
        json.topics[0].topic == "prices-v1"
        json.topics[0].headOffset == 1000
        json.topics[0].durableMaxOffset == 990
        json.topics[0].durabilityLag == 10   // head - durableMax
        json.topics[0].sealedSegmentCount == 2
        json.topics[0].hasActiveSegment == true
        json.topics[0].totalBytes == 2560
        // disk field omitted unless disk=true
        !json.topics[0].containsKey("diskBytesOnDisk")
    }

    def "GET /storage/{topic} returns the segment inventory sorted by base offset"() {
        given:
        storage.getCurrentOffset("prices-v1", 0) >> 1500L
        storage.getMaxOffsetFromMetadata("prices-v1", 0) >> 1500L
        storage.getEarliestOffset("prices-v1", 0) >> 0L

        and: "two segments returned out of order — endpoint must sort by baseOffset"
        def s0 = Mock(Segment) {
            getBaseOffset() >> 1000L; getNextOffset() >> 1500L; getRecordCount() >> 500L
            getSize() >> 4096; isActive() >> true; isFull(1073741824L) >> false; getLogPath() >> Path.of("/d/1000.log")
        }
        def s1 = Mock(Segment) {
            getBaseOffset() >> 0L; getNextOffset() >> 1000L; getRecordCount() >> 1000L
            getSize() >> 8192; isActive() >> false; isFull(1073741824L) >> true; getLogPath() >> Path.of("/d/0.log")
        }
        def sm = managerWith(true, 1, 8192L, 4096L)
        sm.getAllSegments() >> [s0, s1]
        segmentAccess.getSegmentManager("prices-v1", 0) >> sm

        when:
        def resp = controller.storageTopic("prices-v1", false)
        def json = new ObjectMapper().readValue(resp.body() as String, Map)

        then:
        resp.status.code == 200
        json.topic == "prices-v1"
        json.durabilityLag == 0
        json.segments.size() == 2
        json.segments[0].baseOffset == 0      // sorted ascending
        json.segments[0].full == true
        json.segments[0].active == false
        json.segments[1].baseOffset == 1000
        json.segments[1].active == true
        json.segments[1].recordCount == 500
        json.segments[0].logFile == "0.log"   // file name only, not full path
    }

    def "GET /storage/{topic} is graceful when the topic has no segment manager"() {
        given:
        storage.getCurrentOffset("ghost", 0) >> -1L
        storage.getMaxOffsetFromMetadata("ghost", 0) >> -1L
        storage.getEarliestOffset("ghost", 0) >> -1L
        segmentAccess.getSegmentManager("ghost", 0) >> null

        when:
        def resp = controller.storageTopic("ghost", false)
        def json = new ObjectMapper().readValue(resp.body() as String, Map)

        then:
        resp.status.code == 200
        json.note == "no segment manager (topic not initialized)"
        json.segments == []
    }

    def "timerStats reports cumulative count/avg/total + a recent-window max (avg may exceed recentMax)"() {
        given: "a timer with one slow and one fast sample"
        def reg = new io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        def timer = io.micrometer.core.instrument.Timer.builder("t.test").register(reg)
        timer.record(java.time.Duration.ofMillis(500))
        timer.record(java.time.Duration.ofMillis(10))
        def ctl = new StatusController(
                Mock(PipeConsistencyService), topology, Mock(ConsumerRegistry), storage, segmentAccess,
                Mock(PendingAckStore), Mock(HttpPipeConnector), reg,
                Mock(ConsumerStateService), Mock(ErrorRecorder), Mock(RefreshCoordinator),
                Mock(RefreshHistoryRecorder), "ERROR")
        def method = StatusController.getDeclaredMethod("timerStats", String)
        method.accessible = true

        when:
        Map m = (Map) method.invoke(ctl, "t.test")

        then: "count is cumulative, avg == total/count, and the max is the renamed recent-window field"
        m.count == 2L
        m.containsKey("avgMs")
        m.containsKey("totalMs")
        m.containsKey("recentMaxMs")
        !m.containsKey("maxMs")                                     // renamed to recentMaxMs
        Math.abs((m.avgMs as double) - ((m.totalMs as double) / 2.0d)) < 0.5d   // avgMs == totalMs/count
    }
}
