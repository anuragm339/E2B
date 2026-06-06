package com.messaging.broker.consistency

import com.messaging.broker.core.TopologyManager
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

import java.time.Instant

class PipeConsistencyCheckerHopSpec extends Specification {

    SimpleMeterRegistry registry = new SimpleMeterRegistry()
    StorageEngine storage = Stub()
    BrokerSegmentHashView hashView = Mock()
    HashCache hashCache = Mock()
    PipeLineageStore lineageStore = Mock()
    TopologyManager topology = Stub()
    UpstreamConsistencyClient upstream = Mock()
    PipeConsistencyReportStore reportStore = Mock()
    PipeConsistencyMetrics metrics = new PipeConsistencyMetrics(registry)
    PipeConsistencyChecker checker

    def setup() {
        checker = new PipeConsistencyChecker(
                storage, hashView, hashCache, lineageStore, topology, upstream, reportStore, metrics,
                4, 100, 100, 1_000, 2)
    }

    private double gaugeValue(String name, Map<String, String> tags) {
        Gauge gauge = registry.find(name)
                .tags(tags.collect { k, v -> Tag.of(k, v) })
                .gauge()
        return gauge == null ? Double.NaN : gauge.value()
    }

    def "runHop returns consistent when topic has no local data"() {
        given:
        storage.getCurrentOffset('prices-v1', 0) >> -1L

        when:
        def report = checker.runHop('prices-v1')

        then:
        report.status == PipeConsistencyReport.Status.CONSISTENT
        1 * reportStore.save({ PipeConsistencyReport r -> r.status == PipeConsistencyReport.Status.CONSISTENT })
        0 * _
    }

    def "runHop falls back to current parent and errors when no lineage and no parent assigned"() {
        given:
        storage.getCurrentOffset('prices-v1', 0) >> 10L
        storage.getEarliestOffset('prices-v1', 0) >> 0L
        lineageStore.resolve(0L, 10L) >> []
        topology.getCurrentParentUrl() >> null

        when:
        def report = checker.runHop('prices-v1')

        then:
        report.status == PipeConsistencyReport.Status.ERROR
        report.errorMessage == 'no parent assigned'
        1 * reportStore.save({ PipeConsistencyReport r -> r.status == PipeConsistencyReport.Status.ERROR })
        0 * upstream._
    }

    def "runHop marks lineage stale when parent for subrange is unreachable"() {
        given:
        storage.getCurrentOffset('prices-v1', 0) >> 50L
        storage.getEarliestOffset('prices-v1', 0) >> 0L
        lineageStore.resolve(0L, 50L) >> [new PipeLineageStore.Subrange(0L, 50L, 'http://parent')]
        upstream.fetchMaxOffset('http://parent', 'prices-v1') >> -1L

        when:
        def report = checker.runHop('prices-v1')

        then:
        report.status == PipeConsistencyReport.Status.LINEAGE_STALE
        1 * reportStore.save({ PipeConsistencyReport r -> r.status == PipeConsistencyReport.Status.LINEAGE_STALE })
        0 * hashView._
    }

    def "runHop records mismatch and drill-down classifications for topic range"() {
        given:
        storage.getCurrentOffset('prices-v1', 0) >> 2L
        storage.getEarliestOffset('prices-v1', 0) >> 0L
        lineageStore.resolve(0L, 2L) >> [new PipeLineageStore.Subrange(0L, 2L, 'http://parent')]
        upstream.fetchMaxOffset('http://parent', 'prices-v1') >> 2L
        hashView.computeHash('prices-v1', 0L, 2L) >> new BrokerSegmentHashView.Computed('LOCAL'.bytes, 2L, 0)
        upstream.fetchHash('http://parent', 'prices-v1', 0L, 2L, 'raw') >>
                new UpstreamConsistencyClient.HashResponse(true, 'UP'.bytes, 3L, 'raw', 'parent-1', null, 200)

        upstream.fetchRange('http://parent', 'prices-v1', 0L, 2L, null, 100) >> [
                new UpstreamConsistencyClient.RangeRecord(0L, 'k0', 'M' as char, '{"v":0}'),
                new UpstreamConsistencyClient.RangeRecord(1L, 'k1', 'M' as char, '{"v":1-upstream}'),
                new UpstreamConsistencyClient.RangeRecord(2L, 'k2', 'M' as char, '{"v":2}')
        ]
        upstream.fetchRange('http://parent', 'prices-v1', 0L, 2L, null, 100) >> []

        storage.read('prices-v1', 0, 0L, 3) >> [
                record(0L, 'prices-v1', 'k0', '{"v":0}'),
                record(1L, 'prices-v1', 'k1', '{"v":1-local}')
        ]

        when:
        def report = checker.runHop('prices-v1')

        then:
        report.status == PipeConsistencyReport.Status.MISMATCH
        report.missingOnBroker*.offset == [2L]
        report.dataMismatch*.offset == [1L]
        report.extraOnBroker.isEmpty()
        report.mismatchedSegments.size() == 1
        report.comparedUpstreamNodeId == 'parent-1'
        1 * reportStore.save({ PipeConsistencyReport r ->
            r.status == PipeConsistencyReport.Status.MISMATCH &&
                    r.missingOnBroker*.offset == [2L] &&
                    r.dataMismatch*.offset == [1L]
        })
    }

    def "runHopGlobal buckets mismatch findings by topic and marks extra records after upstream is exhausted"() {
        given:
        hashView.globalMaxOffset() >> 2L
        hashView.globalEarliestOffset() >> 0L
        storage.getTopicNames() >> (['topic-a', 'topic-b'] as Set)
        lineageStore.resolve(0L, 2L) >> [new PipeLineageStore.Subrange(0L, 2L, 'http://parent')]
        upstream.fetchMaxOffset('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE) >> 2L
        hashView.computeGlobalHash(0L, 2L) >> new BrokerSegmentHashView.Computed('LOCAL'.bytes, 2L, 0)
        upstream.fetchHash('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 2L, 'raw') >>
                new UpstreamConsistencyClient.HashResponse(true, 'UP'.bytes, 2L, 'raw', 'cloud-root', null, 200)

        upstream.fetchRange('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 2L, null, 100) >> [
                new UpstreamConsistencyClient.RangeRecord(0L, 'ka', 'M' as char, '{"topic":"topic-a","v":0}'),
                new UpstreamConsistencyClient.RangeRecord(1L, 'kb', 'M' as char, '{"topic":"topic-b","v":1-upstream}')
        ]
        upstream.fetchRange('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 2L, 1L, 100) >> []

        storage.read('topic-a', 0, 0L, 2) >> [record(0L, 'topic-a', 'ka', '{"topic":"topic-a","v":0}')]
        storage.read('topic-a', 0, 2L, 1) >> []
        storage.read('topic-b', 0, 0L, 2) >> [
                record(1L, 'topic-b', 'kb', '{"topic":"topic-b","v":1-local}'),
                record(2L, 'topic-b', 'kc', '{"topic":"topic-b","v":2-local-only}')
        ]
        storage.read('topic-b', 0, 2L, 1) >> [record(2L, 'topic-b', 'kc', '{"topic":"topic-b","v":2-local-only}')]

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.MISMATCH
        report.dataMismatch*.offset == [1L]
        report.extraOnBroker*.offset == [2L]
        report.missingOnBroker.isEmpty()
        report.comparedUpstreamNodeId == 'cloud-root'
        1 * reportStore.save({ PipeConsistencyReport r ->
            r.topic == PipeConsistencyChecker.GLOBAL_SCOPE &&
                    r.status == PipeConsistencyReport.Status.MISMATCH &&
                    r.dataMismatch*.offset == [1L] &&
                    r.extraOnBroker*.offset == [2L]
        })
    }

    def "runHopGlobal returns consistent when global storage is empty and clears stale per-topic gauges"() {
        given:
        metrics.setLatestBreakdown('topic-a', 'hop', 5L, 4L, 3L, 2L)
        hashView.globalMaxOffset() >> -1L
        storage.getTopicNames() >> (['topic-a'] as Set)

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.CONSISTENT
        gaugeValue('pipe_consistency_missing_records_latest', [topic: 'topic-a', mode: 'hop']) == 0.0d
        gaugeValue('pipe_consistency_extra_records_latest', [topic: 'topic-a', mode: 'hop']) == 0.0d
        gaugeValue('pipe_consistency_data_mismatch_records_latest', [topic: 'topic-a', mode: 'hop']) == 0.0d
        gaugeValue('pipe_consistency_mismatch_segments_latest', [topic: 'topic-a', mode: 'hop']) == 0.0d
        1 * reportStore.save({ PipeConsistencyReport r -> r.status == PipeConsistencyReport.Status.CONSISTENT })
        0 * upstream._
    }

    def "runHopGlobal falls back to current parent and errors when no lineage and no parent assigned"() {
        given:
        hashView.globalMaxOffset() >> 10L
        hashView.globalEarliestOffset() >> 0L
        storage.getTopicNames() >> ([] as Set)
        lineageStore.resolve(0L, 10L) >> []
        topology.getCurrentParentUrl() >> null

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.ERROR
        report.errorMessage == 'no parent assigned'
        1 * reportStore.save({ PipeConsistencyReport r -> r.status == PipeConsistencyReport.Status.ERROR })
        0 * upstream._
    }

    def "runHopGlobal marks lineage stale when upstream parent is unreachable"() {
        given:
        hashView.globalMaxOffset() >> 25L
        hashView.globalEarliestOffset() >> 0L
        storage.getTopicNames() >> ([] as Set)
        lineageStore.resolve(0L, 25L) >> [new PipeLineageStore.Subrange(0L, 25L, 'http://parent')]
        upstream.fetchMaxOffset('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE) >> -1L

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.LINEAGE_STALE
        gaugeValue('pipe_consistency_status', [topic: PipeConsistencyChecker.GLOBAL_SCOPE, mode: 'hop']) == 3.0d
        1 * reportStore.save({ PipeConsistencyReport r -> r.status == PipeConsistencyReport.Status.LINEAGE_STALE })
        0 * hashView.computeGlobalHash(_, _)
    }

    def "runHopGlobal prefers error over stale when one subrange fails and another is unreachable"() {
        given:
        hashView.globalMaxOffset() >> 9L
        hashView.globalEarliestOffset() >> 0L
        storage.getTopicNames() >> ([] as Set)
        lineageStore.resolve(0L, 9L) >> [
                new PipeLineageStore.Subrange(0L, 4L, 'http://stale-parent'),
                new PipeLineageStore.Subrange(5L, 9L, 'http://bad-parent')
        ]
        upstream.fetchMaxOffset('http://stale-parent', PipeConsistencyChecker.GLOBAL_SCOPE) >> -1L
        upstream.fetchMaxOffset('http://bad-parent', PipeConsistencyChecker.GLOBAL_SCOPE) >> 9L
        hashView.computeGlobalHash(5L, 9L) >> new BrokerSegmentHashView.Computed('LOCAL'.bytes, 5L, 0)
        upstream.fetchHash('http://bad-parent', PipeConsistencyChecker.GLOBAL_SCOPE, 5L, 9L, 'raw') >>
                new UpstreamConsistencyClient.HashResponse(false, null, 0L, 'raw', null, null, 503)

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.ERROR
        report.errorMessage == 'one or more subranges failed'
        1 * reportStore.save({ PipeConsistencyReport r ->
            r.status == PipeConsistencyReport.Status.ERROR &&
                    r.errorMessage == 'one or more subranges failed'
        })
    }

    def "runHopGlobal truncates after mismatch chunks exceed drill-down cap"() {
        given:
        checker = new PipeConsistencyChecker(
                storage, hashView, hashCache, lineageStore, topology, upstream, reportStore, metrics,
                4, 100, 100, 1_000, 1)
        hashView.globalMaxOffset() >> 1001L
        hashView.globalEarliestOffset() >> 0L
        storage.getTopicNames() >> ([] as Set)
        lineageStore.resolve(0L, 1001L) >> [new PipeLineageStore.Subrange(0L, 1001L, 'http://parent')]
        upstream.fetchMaxOffset('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE) >> 1001L

        hashView.computeGlobalHash(0L, 999L) >> new BrokerSegmentHashView.Computed('LOCAL-1'.bytes, 1L, 0)
        upstream.fetchHash('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 999L, 'raw') >>
                new UpstreamConsistencyClient.HashResponse(true, 'UP-1'.bytes, 2L, 'raw', 'cloud-root', null, 200)
        upstream.fetchRange('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 999L, null, 100) >> []

        hashView.computeGlobalHash(1000L, 1001L) >> new BrokerSegmentHashView.Computed('LOCAL-2'.bytes, 1L, 0)
        upstream.fetchHash('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 1000L, 1001L, 'raw') >>
                new UpstreamConsistencyClient.HashResponse(true, 'UP-2'.bytes, 2L, 'raw', 'cloud-root', null, 200)

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.MISMATCH
        report.truncated
        report.mismatchedSegments*.baseOffset == [0L, 1000L]
        report.comparedUpstreamNodeId == 'cloud-root'
        1 * reportStore.save({ PipeConsistencyReport r ->
            r.status == PipeConsistencyReport.Status.MISMATCH &&
                    r.truncated &&
                    r.mismatchedSegments*.baseOffset == [0L, 1000L]
        })
    }

    def "runHopGlobal publishes unknown-topic missing records and resets untouched topics to zero"() {
        given:
        metrics.setLatestBreakdown('topic-a', 'hop', 9L, 8L, 7L, 6L)
        hashView.globalMaxOffset() >> 0L
        hashView.globalEarliestOffset() >> 0L
        storage.getTopicNames() >> (['topic-a'] as Set)
        lineageStore.resolve(0L, 0L) >> [new PipeLineageStore.Subrange(0L, 0L, 'http://parent')]
        upstream.fetchMaxOffset('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE) >> 0L
        hashView.computeGlobalHash(0L, 0L) >> new BrokerSegmentHashView.Computed('LOCAL'.bytes, 1L, 0)
        upstream.fetchHash('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 0L, 'raw') >>
                new UpstreamConsistencyClient.HashResponse(true, 'UP'.bytes, 1L, 'raw', 'cloud-root', null, 200)
        upstream.fetchRange('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 0L, null, 100) >> [
                new UpstreamConsistencyClient.RangeRecord(0L, 'ka', 'M' as char, '{"v":0}')
        ]
        upstream.fetchRange('http://parent', PipeConsistencyChecker.GLOBAL_SCOPE, 0L, 0L, 0L, 100) >> []
        storage.read('topic-a', 0, 0L, 1) >> []

        when:
        def report = checker.runHopGlobal()

        then:
        report.status == PipeConsistencyReport.Status.MISMATCH
        report.missingOnBroker*.detail == ['missing on broker (topic=unknown)']
        gaugeValue('pipe_consistency_missing_records_latest', [topic: 'topic-a', mode: 'hop']) == 0.0d
        gaugeValue('pipe_consistency_missing_records_latest', [topic: 'unknown', mode: 'hop']) == 1.0d
    }

    private static MessageRecord record(long offset, String topic, String key, String data) {
        new MessageRecord(offset, topic, 0, key, EventType.MESSAGE, data, Instant.now())
    }
}
