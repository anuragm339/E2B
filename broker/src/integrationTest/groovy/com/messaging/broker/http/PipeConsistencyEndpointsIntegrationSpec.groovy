package com.messaging.broker.http

import com.messaging.broker.compaction.CompactionIndex
import com.messaging.broker.consistency.KeyspaceDigest
import com.messaging.broker.support.BrokerHttpSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.util.concurrent.PollingConditions

@MicronautTest
class PipeConsistencyEndpointsIntegrationSpec extends BrokerHttpSpecSupport {

    @Inject
    CompactionIndex compactionIndex

    @Override
    Map<String, String> getProperties() {
        super.getProperties() + ['pipe.consistency.enabled': 'true']
    }

    private void seedTopic(String topic, int count) {
        def response = post('/test/inject-messages', [count: count, topic: topic, prefix: 'PC'])
        assert json(response).success == true
    }

    def "head endpoint answers the storage head without any scan"() {
        given:
        seedTopic('pc-head-topic', 4)   // offsets 0..3

        expect:
        json(get('/pipe/consistency/head?topic=pc-head-topic')).head == 3
        json(get('/pipe/consistency/head?topic=no-such-topic')).head == -1
    }

    def "digest endpoint returns bucket digests matching a local computation"() {
        given: 'records in storage and index entries at known offsets'
        seedTopic('pc-digest-topic', 5)   // offsets 0..4
        compactionIndex.updateKey('pc-digest-topic', 'k1', 2L, 100L)
        compactionIndex.updateKey('pc-digest-topic', 'k2', 4L, 100L)

        when:
        def response = get('/pipe/consistency/digest?topic=pc-digest-topic&watermark=4&buckets=8')
        def body = json(response)

        then:
        response.statusCode() == 200
        body.parentHead == 4
        body.effectiveWatermark == 4
        (body.digests as List).size() == 8
        (body.counts as List).sum() == 2

        and: 'digests equal an independent local computation over the same index'
        def local = KeyspaceDigest.compute(compactionIndex, 'pc-digest-topic', 4L, 8, 0)
        (body.digests as List).collect { it as long } == (local.digests as List)
    }

    def "digest clamps the effective watermark to this node's head"() {
        given:
        seedTopic('pc-clamp-topic', 3)   // head = 2

        expect:
        with(json(get('/pipe/consistency/digest?topic=pc-clamp-topic&watermark=999&buckets=8'))) {
            parentHead == 2
            effectiveWatermark == 2
        }
    }

    def "bucket endpoint serves requested buckets only, watermark-filtered"() {
        given:
        seedTopic('pc-bucket-topic', 6)
        compactionIndex.updateKey('pc-bucket-topic', 'bk1', 1L, 100L)
        compactionIndex.updateKey('pc-bucket-topic', 'bk2', 5L, 100L)  // beyond watermark 3
        int bucketOfBk1 = KeyspaceDigest.bucketOf(KeyspaceDigest.hash64('bk1'), 8)

        when:
        def allBuckets = (0..7).join(',')
        def body = json(get("/pipe/consistency/bucket?topic=pc-bucket-topic&watermark=3&buckets=8&bucket=${allBuckets}"))

        then: 'bk1 is served in its bucket; bk2 is filtered by the watermark'
        def entries = body.entries["${bucketOfBk1}"] as List
        entries.any { (it.h as long) == KeyspaceDigest.hash64('bk1') && (it.o as long) == 1L }
        body.entries.values().flatten().every { (it.o as long) <= 3L }
    }

    def "classify reports physical record presence and index state"() {
        given:
        seedTopic('pc-classify-topic', 4)   // records physically present at offsets 0..3
        compactionIndex.updateKey('pc-classify-topic', 'ck1', 2L, 100L)

        when:
        def response = post('/pipe/consistency/classify',
                [topic: 'pc-classify-topic', watermark: 3, offsets: [2, 99], keys: ['ck1', 'never-seen']])
        def body = json(response)

        then:
        body.offsets['2'] == true        // record physically present
        body.offsets['99'] == false      // no such record
        body.keys['ck1'] == 'PRESENT_AT_OR_BELOW_WATERMARK'
        body.keys['never-seen'] == 'ABSENT'
    }

    def "admin check without a parent records an UNREACHABLE report"() {
        when: 'no topology parent is connected in this test context'
        def started = post('/admin/pipe-consistency/check?topic=pc-digest-topic&target=parent', [:])

        then:
        started.statusCode() == 202

        and:
        new PollingConditions(timeout: 10).eventually {
            def report = json(get('/admin/pipe-consistency/report'))
            assert report.running == false
            assert report.latestByTopic['pc-digest-topic']?.state == 'UNREACHABLE'
        }
    }

    def "invalid requests are rejected"() {
        expect:
        get('/pipe/consistency/digest?topic=t&watermark=-1&buckets=8').statusCode() == 400
        get('/pipe/consistency/bucket?topic=t&watermark=1&buckets=8&bucket=99').statusCode() == 400
        post('/admin/pipe-consistency/check?target=nonsense', [:]).statusCode() == 400
    }
}
