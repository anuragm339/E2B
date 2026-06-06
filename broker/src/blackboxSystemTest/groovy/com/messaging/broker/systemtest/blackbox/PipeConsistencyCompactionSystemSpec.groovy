package com.messaging.broker.systemtest.blackbox

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.systemtest.support.ProcessBackedBrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

class PipeConsistencyCompactionSystemSpec extends ProcessBackedBrokerSystemTestSupport {

    private final ObjectMapper mapper = new ObjectMapper()
    private final HttpClient http = HttpClient.newHttpClient()

    @Override
    protected Map<String, String> brokerConfig() {
        def base = super.brokerConfig()
        base['compaction.enabled'] = 'true'
        base['compaction.schedule.initial-delay'] = '24h'
        base['compaction.schedule.interval'] = '24h'
        base['compaction.max-process-cpu-usage'] = '100.0'
        base['compaction.max-heap-usage'] = '100.0'
        base['pipe.consistency.endpoint.enabled'] = 'true'
        return base
    }

    def "pipe consistency endpoints show raw records before compaction and compacted survivors after compaction"() {
        given:
        def rawTopic = 'pc-bbx-raw'
        def compactTopic = 'pc-bbx-compact'

        and: 'the broker ingests a non-compacted topic with duplicate keys still physically present'
        cloudServer.enqueueMessages([
            [offset: 10L, topic: rawTopic, partition: 0, msgKey: 'dup',  eventType: 'MESSAGE', data: '{"topic":"pc-bbx-raw","v":1}'],
            [offset: 11L, topic: rawTopic, partition: 0, msgKey: 'keep', eventType: 'MESSAGE', data: '{"topic":"pc-bbx-raw","v":2}'],
            [offset: 12L, topic: rawTopic, partition: 0, msgKey: 'dup',  eventType: 'MESSAGE', data: '{"topic":"pc-bbx-raw","v":3}']
        ])

        expect: 'before compaction, range and hash still reflect all three stored records'
        waitForRecordCount(rawTopic, 10L, 12L, 3)
        def rawRange = getJson("/pipe/consistency/range?topic=${rawTopic}&from=10&to=12&pageSize=100")
        (rawRange.records as List)*.offset as List<Long> == [10L, 11L, 12L]
        (rawRange.records as List)*.msgKey == ['dup', 'keep', 'dup']

        def rawHash = getJson("/pipe/consistency/hash?topic=${rawTopic}&from=10&to=12&projection=raw")
        rawHash.recordCount == 3
        rawHash.projection == 'raw'

        when: 'a second topic is ingested and then compacted via the live admin endpoint'
        cloudServer.enqueueMessages([
            [offset: 100L, topic: compactTopic, partition: 0, msgKey: 'dup',  eventType: 'MESSAGE', data: '{"topic":"pc-bbx-compact","v":1}'],
            [offset: 101L, topic: compactTopic, partition: 0, msgKey: 'keep', eventType: 'MESSAGE', data: '{"topic":"pc-bbx-compact","v":2}'],
            [offset: 102L, topic: compactTopic, partition: 0, msgKey: 'dup',  eventType: 'MESSAGE', data: '{"topic":"pc-bbx-compact","v":3}']
        ])
        waitForRecordCount(compactTopic, 100L, 102L, 3)
        postEmpty("/admin/compaction/trigger")

        then: 'only the compacted survivors remain visible via the live consistency endpoints'
        waitForCompactedView(compactTopic, 100L, 102L, 2)

        def compactRange = getJson("/pipe/consistency/range?topic=${compactTopic}&from=100&to=102&pageSize=100")
        (compactRange.records as List)*.offset as List<Long> == [101L, 102L]
        (compactRange.records as List)*.msgKey == ['keep', 'dup']

        and: 'hash view over the same range now reports only the compacted record count'
        def compactHash = getJson("/pipe/consistency/hash?topic=${compactTopic}&from=100&to=102&projection=compacted")
        compactHash.recordCount == 2
        compactHash.projection == 'compacted'
    }

    private void waitForRecordCount(String topic, long from, long to, int expectedCount) {
        new PollingConditions(timeout: 20, delay: 0.25).eventually {
            def body = getJson("/pipe/consistency/range?topic=${topic}&from=${from}&to=${to}&pageSize=100")
            assert (body.records as List).size() == expectedCount
        }
    }

    private void waitForCompactedView(String topic, long from, long to, int expectedRecordCount) {
        new PollingConditions(timeout: 20, delay: 0.25).eventually {
            def range = getJson("/pipe/consistency/range?topic=${topic}&from=${from}&to=${to}&pageSize=100")
            assert (range.records as List).size() == expectedRecordCount

            def hash = getJson("/pipe/consistency/hash?topic=${topic}&from=${from}&to=${to}&projection=compacted")
            assert hash.recordCount == expectedRecordCount
        }
    }

    private Map getJson(String path) {
        def req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:${brokerHttpPort}${path}"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build()
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        assert resp.statusCode() == 200: "GET ${path} failed: ${resp.statusCode()} ${resp.body()}"
        mapper.readValue(resp.body(), Map)
    }

    private void postEmpty(String path) {
        def req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:${brokerHttpPort}${path}"))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        assert resp.statusCode() == 200: "POST ${path} failed: ${resp.statusCode()} ${resp.body()}"
    }
}
