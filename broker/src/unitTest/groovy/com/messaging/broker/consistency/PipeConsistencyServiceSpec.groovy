package com.messaging.broker.consistency

import com.messaging.broker.compaction.InMemoryCompactionIndex
import com.messaging.broker.consistency.ParentConsistencyClient.BucketEntry
import com.messaging.broker.consistency.ParentConsistencyClient.ClassifyResponse
import com.messaging.broker.consistency.ParentConsistencyClient.DigestResponse
import com.messaging.broker.consistency.ParentConsistencyClient.KeyState
import com.messaging.broker.consistency.PipeConsistencyReport.State
import com.messaging.broker.core.TopologyManager
import com.messaging.common.api.StorageEngine
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

class PipeConsistencyServiceSpec extends Specification {

    static final int BUCKETS = 16
    static final String TOPIC = 'prices-v1'
    static final String PARENT = 'http://parent:8081'

    StorageEngine storage = Mock()
    InMemoryCompactionIndex childIndex = new InMemoryCompactionIndex()
    InMemoryCompactionIndex parentIndex = new InMemoryCompactionIndex()
    ParentConsistencyClient client = Mock()
    TopologyManager topology = Mock()

    PipeConsistencyService service

    def setup() {
        topology.getCurrentParentUrl() >> PARENT
        service = serviceWith(2)   // escalate after 2 consecutive clamped checks
    }

    private PipeConsistencyService serviceWith(int escalateAfter) {
        new PipeConsistencyService(
                storage, childIndex, client, topology, new SimpleMeterRegistry(),
                'http://cloud:8080', true, BUCKETS, 0, 8, 1000,
                true, escalateAfter, 5)
    }

    // ── Helpers simulating the parent side from parentIndex ───────────────────

    private DigestResponse parentDigest(long requestedWatermark, long parentHead) {
        long effective = Math.min(requestedWatermark, parentHead)
        def r = KeyspaceDigest.compute(parentIndex, TOPIC, effective, BUCKETS, 0)
        new DigestResponse(parentHead, effective, r.digests, r.counts)
    }

    private Map<Integer, List<BucketEntry>> parentBuckets(long watermark, List<Integer> ids) {
        Map<Integer, List<BucketEntry>> result = ids.collectEntries { [(it): []] }
        parentIndex.forEachEntry(TOPIC) { msgKey, offset, ts ->
            if (offset <= watermark) {
                long h = KeyspaceDigest.hash64(msgKey)
                int b = KeyspaceDigest.bucketOf(h, BUCKETS)
                result[b]?.add(new BucketEntry(h, offset))
            }
        }
        result
    }

    def "identical keyspaces are CONSISTENT with a single digest exchange"() {
        given:
        ['a': 1L, 'b': 2L, 'c': 3L].each { k, o ->
            childIndex.updateKey(TOPIC, k, o, 1L)
            parentIndex.updateKey(TOPIC, k, o, 1L)
        }
        storage.getCurrentOffset(TOPIC, 0) >> 3L

        when:
        def reports = service.runCheck(TOPIC, 'parent')

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 3L, BUCKETS) >> { parentDigest(3L, 10L) }
        0 * client.fetchBuckets(*_)
        0 * client.classify(*_)
        reports.size() == 1
        reports[0].state == State.CONSISTENT
        reports[0].keysScanned == 3
    }

    def "missing key with physically present parent record is INCONSISTENT"() {
        given: 'parent has key-X@2 within the child watermark; child never stored it'
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'key-X', 2L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 5L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 5L, BUCKETS) >> { parentDigest(5L, 5L) }
        1 * client.fetchBuckets(PARENT, TOPIC, 5L, BUCKETS, _) >> { u, t, w, b, ids -> parentBuckets(5L, ids) }
        1 * client.classify(PARENT, TOPIC, 5L, [2L], []) >>
                new ClassifyResponse([(2L): true], [:])
        report.state == State.INCONSISTENT
        report.missingKeys == 1
        report.refreshRecommended
    }

    def "missing key whose parent record was compacted away is benign"() {
        given: 'parent index remembers a key whose record (expired tombstone) is physically gone'
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'deleted-key', 2L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 5L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 5L, BUCKETS) >> { parentDigest(5L, 5L) }
        1 * client.fetchBuckets(*_) >> { u, t, w, b, ids -> parentBuckets(5L, ids) }
        1 * client.classify(PARENT, TOPIC, 5L, [2L], []) >>
                new ClassifyResponse([(2L): false], [:])
        report.state == State.CONSISTENT
        report.missingKeys == 0
    }

    def "stale key whose parent record was compacted away is a ZOMBIE"() {
        given: 'child holds key-Z@3; parent latest for key-Z was a tombstone@7 that has expired'
        childIndex.updateKey(TOPIC, 'key-Z', 3L, 1L)
        parentIndex.updateKey(TOPIC, 'key-Z', 7L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 10L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 10L, BUCKETS) >> { parentDigest(10L, 10L) }
        1 * client.fetchBuckets(*_) >> { u, t, w, b, ids -> parentBuckets(10L, ids) }
        1 * client.classify(PARENT, TOPIC, 10L, [7L], []) >>
                new ClassifyResponse([(7L): false], [:])
        report.state == State.INCONSISTENT
        report.zombieKeys == 1
        report.staleKeys == 0
        report.refreshRecommended
    }

    def "stale key with physically present parent record is INCONSISTENT stale"() {
        given:
        childIndex.updateKey(TOPIC, 'key-S', 3L, 1L)
        parentIndex.updateKey(TOPIC, 'key-S', 7L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 10L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 10L, BUCKETS) >> { parentDigest(10L, 10L) }
        1 * client.fetchBuckets(*_) >> { u, t, w, b, ids -> parentBuckets(10L, ids) }
        1 * client.classify(PARENT, TOPIC, 10L, [7L], []) >>
                new ClassifyResponse([(7L): true], [:])
        report.staleKeys == 1
        report.state == State.INCONSISTENT
    }

    def "fabricated keys: authoritative ABSENT is INCONSISTENT, plain parent ABSENT stays a warning"() {
        given: 'child holds a key within its watermark that the verifier never had'
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'a', 1L, 1L)
        childIndex.updateKey(TOPIC, 'injected-key', 3L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 5L

        when: 'the verifier is AUTHORITATIVE (cloud — complete, never-expiring keyspace)'
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 5L, BUCKETS) >> { parentDigest(5L, 5L) }
        1 * client.fetchBuckets(*_) >> { u, t, w, b, ids -> parentBuckets(5L, ids) }
        1 * client.classify(PARENT, TOPIC, 5L, [], ['injected-key']) >>
                new ClassifyResponse([:], ['injected-key': KeyState.ABSENT], true)
        report.state == State.INCONSISTENT
        report.fabricatedKeys == 1
        report.refreshRecommended

        when: 'the same answer from a NON-authoritative POS parent (may be freshly provisioned)'
        def report2 = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 5L, BUCKETS) >> { parentDigest(5L, 5L) }
        1 * client.fetchBuckets(*_) >> { u, t, w, b, ids -> parentBuckets(5L, ids) }
        1 * client.classify(PARENT, TOPIC, 5L, [], ['injected-key']) >>
                new ClassifyResponse([:], ['injected-key': KeyState.ABSENT], false)
        report2.state == State.CONSISTENT
        report2.extraKeys == 1
        report2.fabricatedKeys == 0
    }

    def "child-extra key superseded beyond the watermark is benign lag"() {
        given: 'child has key-L@4; parent already advanced key-L to 20 (beyond W=10)'
        childIndex.updateKey(TOPIC, 'key-L', 4L, 1L)
        parentIndex.updateKey(TOPIC, 'key-L', 20L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 10L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 10L, BUCKETS) >> { parentDigest(10L, 25L) }
        1 * client.fetchBuckets(*_) >> { u, t, w, b, ids -> parentBuckets(10L, ids) }
        1 * client.classify(PARENT, TOPIC, 10L, [], ['key-L']) >>
                new ClassifyResponse([:], ['key-L': KeyState.PRESENT_BEYOND_WATERMARK])
        report.state == State.CONSISTENT
        report.laggingKeys == 1
    }

    def "watermark clamps to a lagging parent head and reports CONSISTENT_UP_TO"() {
        given: 'post-reshuffle: parent head (5) is behind the child head (9); state matches up to 5'
        ['a': 1L, 'b': 4L].each { k, o ->
            childIndex.updateKey(TOPIC, k, o, 1L)
            parentIndex.updateKey(TOPIC, k, o, 1L)
        }
        childIndex.updateKey(TOPIC, 'newer', 9L, 1L)   // beyond parent head — excluded by clamp
        storage.getCurrentOffset(TOPIC, 0) >> 9L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 9L, BUCKETS) >> { parentDigest(9L, 5L) }
        report.state == State.CONSISTENT_UP_TO
        report.watermark == 9L
        report.effectiveWatermark == 5L
    }

    def "key advanced past a clamped watermark is benign child-ahead, not missing"() {
        given: 'cloud loopback clamp: parent verifies only up to 5; child already replayed key-R at 9'
        childIndex.updateKey(TOPIC, 'key-R', 9L, 1L)   // child latest is beyond the clamp
        parentIndex.updateKey(TOPIC, 'key-R', 4L, 1L)  // parent (clamped) still reports 4
        storage.getCurrentOffset(TOPIC, 0) >> 9L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then: 'parent clamps to head 5; child digest at 5 excludes key-R, parent includes it'
        1 * client.fetchDigest(PARENT, TOPIC, 9L, BUCKETS) >> { parentDigest(9L, 5L) }
        1 * client.fetchBuckets(PARENT, TOPIC, 5L, BUCKETS, _) >> { u, t, w, b, ids -> parentBuckets(5L, ids) }
        0 * client.classify(*_)   // resolved locally — no false missing, no network call

        report.state == State.CONSISTENT_UP_TO
        report.missingKeys == 0
        report.childNewerKeys == 1
    }

    def "capped drill-down at a clamped watermark is INCONCLUSIVE, at an equal watermark INCONSISTENT"() {
        given: 'every key diverges so all buckets mismatch (cap is 8 of 16)'
        (1..200).each { childIndex.updateKey(TOPIC, "key-$it".toString(), (long) it, 1L) }
        (1..200).each { parentIndex.updateKey(TOPIC, "key-$it".toString(), it + 1000L, 1L) }
        storage.getCurrentOffset(TOPIC, 0) >> 2000L

        when: 'parent head equals child watermark — real mass divergence'
        def equalReport = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 2000L, BUCKETS) >> { parentDigest(2000L, 2000L) }
        equalReport.state == State.INCONSISTENT
        equalReport.refreshRecommended

        when: 'parent clamps below the child head — divergence may be benign child-ahead'
        def clampedReport = service.runCheck(TOPIC, 'parent')[0]

        then:
        1 * client.fetchDigest(PARENT, TOPIC, 2000L, BUCKETS) >> { parentDigest(2000L, 150L) }
        clampedReport.state == State.INCONCLUSIVE
        !clampedReport.refreshRecommended
    }

    // ── Escalation: clamped parent → registry verifier candidates ─────────────

    def "first clamped check only marks verificationPending — no escalation probes"() {
        given:
        ['a': 1L].each { k, o -> childIndex.updateKey(TOPIC, k, o, 1L); parentIndex.updateKey(TOPIC, k, o, 1L) }
        childIndex.updateKey(TOPIC, 'newer', 9L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 9L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then: 'parent clamps to head 5'
        1 * client.fetchDigest(PARENT, TOPIC, 9L, BUCKETS) >> { parentDigest(9L, 5L) }
        0 * client.fetchHead(*_)
        report.state == State.CONSISTENT_UP_TO
        report.verificationPending
    }

    def "persistent clamp escalates: dead candidate skipped, behind candidate skipped, qualified one verifies"() {
        given: 'parent stuck at head 5; this node at 9'
        ['a': 1L].each { k, o -> childIndex.updateKey(TOPIC, k, o, 1L); parentIndex.updateKey(TOPIC, k, o, 1L) }
        childIndex.updateKey(TOPIC, 'newer', 9L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 9L
        topology.getVerifierCandidates() >> ['http://dead:8081', 'http://behind:8081', 'http://good:8081']

        and: 'the qualified verifier holds the same state through offset 9'
        def verifierIndex = new com.messaging.broker.compaction.InMemoryCompactionIndex()
        verifierIndex.updateKey(TOPIC, 'a', 1L, 1L)
        verifierIndex.updateKey(TOPIC, 'newer', 9L, 1L)

        when: 'check 1 (pending) then check 2 (escalates)'
        service.runCheck(TOPIC, 'parent')
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        2 * client.fetchDigest(PARENT, TOPIC, 9L, BUCKETS) >> { parentDigest(9L, 5L) }
        1 * client.fetchHead('http://dead:8081', TOPIC) >> { throw new RuntimeException('connection refused') }
        1 * client.fetchHead('http://behind:8081', TOPIC) >> 4L
        1 * client.fetchHead('http://good:8081', TOPIC) >> 12L
        1 * client.fetchDigest('http://good:8081', TOPIC, 9L, BUCKETS) >> {
            def r = KeyspaceDigest.compute(verifierIndex, TOPIC, 9L, BUCKETS, 0)
            new DigestResponse(12L, 9L, r.digests, r.counts)
        }

        and: 'full verdict from the in-store verifier, lineage recorded'
        report.state == State.CONSISTENT
        report.escalatedFrom == PARENT
        report.target == 'http://good:8081'
        !report.verificationPending
    }

    def "escalation finds real divergence on the verifier"() {
        given: 'parent stuck; verifier has a key this node is missing'
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'a', 1L, 1L)
        childIndex.updateKey(TOPIC, 'newer', 9L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 9L
        topology.getVerifierCandidates() >> ['http://good:8081']

        def verifierIndex = new com.messaging.broker.compaction.InMemoryCompactionIndex()
        verifierIndex.updateKey(TOPIC, 'a', 1L, 1L)
        verifierIndex.updateKey(TOPIC, 'newer', 9L, 1L)
        verifierIndex.updateKey(TOPIC, 'missed', 7L, 1L)   // child never stored this

        def verifierBuckets = { long w, List<Integer> ids ->
            Map<Integer, List<ParentConsistencyClient.BucketEntry>> result = ids.collectEntries { [(it): []] }
            verifierIndex.forEachEntry(TOPIC) { k, o, ts ->
                if (o <= w) {
                    long h = KeyspaceDigest.hash64(k)
                    result[KeyspaceDigest.bucketOf(h, BUCKETS)]?.add(new ParentConsistencyClient.BucketEntry(h, o))
                }
            }
            result
        }

        when:
        service.runCheck(TOPIC, 'parent')
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        2 * client.fetchDigest(PARENT, TOPIC, 9L, BUCKETS) >> { parentDigest(9L, 5L) }
        1 * client.fetchHead('http://good:8081', TOPIC) >> 12L
        1 * client.fetchDigest('http://good:8081', TOPIC, 9L, BUCKETS) >> {
            def r = KeyspaceDigest.compute(verifierIndex, TOPIC, 9L, BUCKETS, 0)
            new DigestResponse(12L, 9L, r.digests, r.counts)
        }
        1 * client.fetchBuckets('http://good:8081', TOPIC, 9L, BUCKETS, _) >> { u, t, w, b, ids -> verifierBuckets(9L, ids) }
        1 * client.classify('http://good:8081', TOPIC, 9L, [7L], []) >>
                new ClassifyResponse([(7L): true], [:])

        report.state == State.INCONSISTENT
        report.missingKeys == 1
        report.escalatedFrom == PARENT
    }

    def "all candidates behind or empty list keeps verificationPending"() {
        given:
        childIndex.updateKey(TOPIC, 'newer', 9L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 9L
        topology.getVerifierCandidates() >> ['http://behind:8081']
        client.fetchHead('http://behind:8081', TOPIC) >> 4L

        when: 'two consecutive clamped checks'
        service.runCheck(TOPIC, 'parent')
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        2 * client.fetchDigest(PARENT, TOPIC, 9L, BUCKETS) >> { parentDigest(9L, 5L) }
        report.verificationPending
        report.state == State.CONSISTENT_UP_TO || report.state == State.INCONCLUSIVE
    }

    def "an unclamped check resets the clamp streak"() {
        given:
        ['a': 1L].each { k, o -> childIndex.updateKey(TOPIC, k, o, 1L); parentIndex.updateKey(TOPIC, k, o, 1L) }
        storage.getCurrentOffset(TOPIC, 0) >> 1L
        topology.getVerifierCandidates() >> ['http://good:8081']

        when: 'clamp, then clean check, then clamp again — never two in a row'
        client.fetchDigest(PARENT, TOPIC, 1L, BUCKETS) >>> [
                parentDigest(1L, 0L),   // clamped (parent head 0)
                parentDigest(1L, 1L),   // clean — resets streak
                parentDigest(1L, 0L)    // clamped again — streak back to 1
        ]
        3.times { service.runCheck(TOPIC, 'parent') }

        then: 'escalation never fires'
        0 * client.fetchHead(*_)
    }

    def "unreachable parent reports UNREACHABLE"() {
        given:
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 1L
        client.fetchDigest(*_) >> { throw new RuntimeException('boom', new java.io.IOException('refused')) }

        expect:
        service.runCheck(TOPIC, 'parent')[0].state == State.UNREACHABLE
    }

    def "parent without consistency endpoints reports UNSUPPORTED_PARENT"() {
        given:
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 1L
        client.fetchDigest(*_) >> { throw new ParentConsistencyClient.UnsupportedParentException(PARENT) }

        expect:
        service.runCheck(TOPIC, 'parent')[0].state == State.UNSUPPORTED_PARENT
    }

    def "empty topic is vacuously CONSISTENT without any network call"() {
        given:
        storage.getCurrentOffset(TOPIC, 0) >> -1L

        when:
        def report = service.runCheck(TOPIC, 'parent')[0]

        then:
        0 * client._
        report.state == State.CONSISTENT
        report.watermark == -1L
    }

    def "concurrent run is rejected by the single-flight guard"() {
        given:
        childIndex.updateKey(TOPIC, 'a', 1L, 1L)
        parentIndex.updateKey(TOPIC, 'a', 1L, 1L)
        storage.getCurrentOffset(TOPIC, 0) >> 1L
        def overlappingResult = 'unset'
        client.fetchDigest(*_) >> { url, t, w, b ->
            overlappingResult = service.runCheck(TOPIC, 'parent')   // re-entrant attempt mid-run
            parentDigest(1L, 1L)
        }

        when:
        def reports = service.runCheck(TOPIC, 'parent')

        then:
        overlappingResult == null
        reports[0].state == State.CONSISTENT
        !service.isRunning()
    }

    def "checking all topics walks storage topic names against the cloud target"() {
        given:
        storage.getTopicNames() >> (['t2', 't1'] as Set)
        storage.getCurrentOffset(_, 0) >> -1L

        when:
        def reports = service.runCheck('all', 'cloud')

        then:
        reports*.topic == ['t1', 't2']
        reports*.target.every { it == 'http://cloud:8080' }
    }
}
