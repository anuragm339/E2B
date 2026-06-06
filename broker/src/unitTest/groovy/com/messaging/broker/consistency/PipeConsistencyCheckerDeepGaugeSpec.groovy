package com.messaging.broker.consistency

import com.messaging.broker.core.TopologyManager
import com.messaging.common.api.StorageEngine
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

/**
 * Unit coverage for Fix 3 — DEEP audit must always update the
 * {@code pipe_consistency_chain_first_divergent_hop} gauge so that a stale
 * divergent-hop hash from a previous MISMATCH cycle does NOT linger on the
 * dashboard after recovery.
 *
 * Bug pre-fix: gauge was set inside an
 * {@code if (firstDivergentHopNodeId != null)} branch, so CONSISTENT /
 * ERROR / DEEP_WALK_ABORTED outcomes left yesterday's bad hash code in place.
 */
class PipeConsistencyCheckerDeepGaugeSpec extends Specification {

    static final String GAUGE = "pipe_consistency_chain_first_divergent_hop"
    static final String SCOPE = PipeConsistencyChecker.GLOBAL_SCOPE

    SimpleMeterRegistry registry
    PipeConsistencyMetrics metrics
    StorageEngine storage = Stub()
    BrokerSegmentHashView hashView = Mock()
    HashCache hashCache = Mock()
    PipeLineageStore lineageStore = Mock()
    TopologyManager topology = Stub()
    UpstreamConsistencyClient upstream = Mock()
    PipeConsistencyReportStore reportStore = Stub()
    PipeConsistencyChecker checker

    /** Used as the "bad upstream" nodeId in the MISMATCH cycle. Its hashCode is the value
     * that ends up in the gauge under the bug. */
    static final String BAD_UPSTREAM_NODE_ID = "broker-l2-001"

    def setup() {
        registry = new SimpleMeterRegistry()
        metrics = new PipeConsistencyMetrics(registry)
        checker = new PipeConsistencyChecker(
                storage, hashView, hashCache, lineageStore, topology, upstream, reportStore, metrics,
                8, 1000, 100, 10_000, 5)

        topology.getCurrentParentUrl() >> "http://parent"
        topology.getNodeId() >> "broker-local"
        hashView.globalMaxOffset() >> 1_000L
        hashView.globalEarliestOffset() >> 0L
        // Local hash for our scope — same `localHash` returned to keep test deterministic.
        hashView.computeGlobalHash(_, _) >> new BrokerSegmentHashView.Computed(
                "LOCAL_HASH".getBytes(), 1000L, 0)
        storage.getTopicNames() >> ([] as Set)   // resetAllPerTopicGauges iterates this
    }

    private double gaugeValue() {
        Gauge g = registry.find(GAUGE).tags(Tags.of("topic", SCOPE)).gauge()
        return g == null ? 0.0d : g.value()
    }

    def "gauge holds the bad nodeId hash after a MISMATCH cycle"() {
        given: 'upstream returns a different hash → MISMATCH, divergent hop nodeId set'
        upstream.fetchMaxOffset(_, _) >> 1_000L
        upstream.fetchHash(_, _, _, _, _) >> new UpstreamConsistencyClient.HashResponse(
                true,
                "REMOTE_DIFFERENT_HASH".getBytes(),
                1000L,
                "raw",
                BAD_UPSTREAM_NODE_ID,
                null,
                200)

        when:
        def report = checker.runDeepGlobal()

        then: 'audit detected a mismatch and the gauge holds the bad nodeId hash'
        report.status == PipeConsistencyReport.Status.MISMATCH
        // Local nodeId is first in chain (no parentUrl); next hop is the divergent one.
        report.firstDivergentHopNodeId == "broker-local"
        gaugeValue() == (double) "broker-local".hashCode()
    }

    def "next CONSISTENT cycle CLEARS the gauge back to 0 (Fix 3)"() {
        given: 'previous cycle left a non-zero value — simulate by calling the metric directly'
        metrics.setFirstDivergentHop(SCOPE, BAD_UPSTREAM_NODE_ID)
        assert gaugeValue() == (double) BAD_UPSTREAM_NODE_ID.hashCode()

        and: 'now upstream returns the SAME hash → CONSISTENT'
        upstream.fetchMaxOffset(_, _) >> 1_000L
        upstream.fetchHash(_, _, _, _, _) >> new UpstreamConsistencyClient.HashResponse(
                true,
                "LOCAL_HASH".getBytes(),  // matches local
                1000L,
                "raw",
                "broker-l2-001",
                null,    // chain ends here — this is the chain root
                200)

        when:
        def report = checker.runDeepGlobal()

        then:
        report.status == PipeConsistencyReport.Status.CONSISTENT
        report.firstDivergentHopNodeId == null
        gaugeValue() == 0.0d
    }

    def "next ERROR cycle CLEARS the gauge back to 0 (Fix 3)"() {
        given:
        metrics.setFirstDivergentHop(SCOPE, BAD_UPSTREAM_NODE_ID)
        assert gaugeValue() != 0.0d

        and: 'upstream unreachable for the immediate parent'
        upstream.fetchMaxOffset(_, _) >> -1L

        when:
        def report = checker.runDeepGlobal()

        then:
        report.status == PipeConsistencyReport.Status.ERROR
        gaugeValue() == 0.0d
    }

    def "DEEP_WALK_ABORTED on hop limit still clears the gauge"() {
        given:
        metrics.setFirstDivergentHop(SCOPE, BAD_UPSTREAM_NODE_ID)

        and: 'each hop responds with same hash & a non-null parentUrl → walk never terminates'
        upstream.fetchMaxOffset(_, _) >> 1_000L
        int [] callCount = [0]
        upstream.fetchHash(_, _, _, _, _) >> { args ->
            // produce a distinct nodeId per hop and always point at a non-null next parent
            callCount[0]++
            return new UpstreamConsistencyClient.HashResponse(
                    true, "LOCAL_HASH".getBytes(), 1000L, "raw",
                    "hop-" + callCount[0], "http://hop-" + (callCount[0] + 1), 200)
        }

        when:
        def report = checker.runDeepGlobal()

        then:
        report.status == PipeConsistencyReport.Status.DEEP_WALK_ABORTED
        gaugeValue() == 0.0d
    }
}
