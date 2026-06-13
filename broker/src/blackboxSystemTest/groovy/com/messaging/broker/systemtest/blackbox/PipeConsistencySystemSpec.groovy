package com.messaging.broker.systemtest.blackbox

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.systemtest.support.ProcessBackedBrokerSystemTestSupport
import spock.util.concurrent.PollingConditions

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * Full cross-process consistency check: the broker SUBPROCESS ingests records over the real
 * pipe, builds its real RocksDB compaction index, is triggered over real HTTP via the admin
 * endpoint, calls its parent (the mock cloud, serving production-hash digests) over real
 * HTTP, and reports CONSISTENT.
 */
class PipeConsistencySystemSpec extends ProcessBackedBrokerSystemTestSupport {

    static final String TOPIC = 'prices-v1'

    private final HttpClient http = HttpClient.newHttpClient()
    private final ObjectMapper mapper = new ObjectMapper()

    @Override
    protected Map<String, String> brokerConfig() {
        def base = super.brokerConfig()
        base['pipe.consistency.enabled'] = 'true'
        base['pipe.consistency.schedule.enabled'] = 'false'   // the spec triggers explicitly
        return base
    }

    private HttpResponse<String> brokerHttp(String method, String path) {
        def builder = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:${brokerHttpPort}${path}"))
                .timeout(java.time.Duration.ofSeconds(10))
        def request = (method == 'POST'
                ? builder.POST(HttpRequest.BodyPublishers.noBody())
                : builder.GET()).build()
        http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    def "consistency verdict crosses real process boundaries"() {
        given: 'the mock cloud serves three records over the pipe'
        cloudServer.enqueueMessages((1..3).collect { i ->
            [offset: (long) i, topic: TOPIC, partition: 0,
             msgKey: "pcs-key-$i", eventType: 'MESSAGE', data: "{\"v\":$i}"]
        })

        and: 'the broker subprocess has ingested all of them'
        waitForFileSize(brokerDataDir.resolve("${TOPIC}/partition-0/00000000000000000000.log")) { it > 0L }
        new PollingConditions(timeout: 30, delay: 0.5).eventually {
            def head = mapper.readTree(brokerHttp('GET', "/pipe/consistency/head?topic=${TOPIC}").body())
            assert head.path('head').asLong() == 3L
        }

        and: 'the mock cloud (as parent) can answer digests for what it served'
        cloudServer.consistencyHead = 3L
        (1..3).each { cloudServer.consistencyEntries["pcs-key-$it".toString()] = (long) it }

        when: 'the check is triggered over the admin HTTP API'
        def started = brokerHttp('POST', "/admin/pipe-consistency/check?topic=${TOPIC}&target=parent")

        then:
        started.statusCode() == 202

        and: 'the report converges on CONSISTENT across the process boundary'
        new PollingConditions(timeout: 30, delay: 1).eventually {
            def report = mapper.readTree(brokerHttp('GET', '/admin/pipe-consistency/report').body())
            assert !report.path('running').asBoolean()
            def topicReport = report.path('latestByTopic').path(TOPIC)
            assert topicReport.path('state').asText() == 'CONSISTENT'
            assert topicReport.path('watermark').asLong() == 3L
            assert topicReport.path('keysScanned').asLong() == 3L
        }

        and: 'the broker process survived the whole flow'
        brokerProcess.isAlive()
    }
}
