package com.messaging.pipe

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant

/**
 * Unit tests for the multi-topic bulk-poll and head endpoints added for download-refresh.
 */
class PipeServerSpec extends Specification {

    StorageEngine storage = Mock()
    PipeServer server = new PipeServer(storage)
    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()

    def rec(long offset, String topic, String key) {
        new MessageRecord(offset, topic, 0, key, null, null, Instant.now())
    }

    def "poll-multi merges records across topics from per-topic offsets"() {
        given:
        storage.read("prices-v1", 0, 0L, _) >> [rec(0L, "prices-v1", "p0"), rec(1L, "prices-v1", "p1")]
        storage.read("reference-data-v5", 0, 5L, _) >> [rec(5L, "reference-data-v5", "r5")]

        when:
        def resp = server.pollMulti("prices-v1,reference-data-v5", "0,5")

        then:
        resp.status.code == 200
        def records = mapper.readValue(resp.body.get(), MessageRecord[].class)
        records.length == 3
        records.collect { it.topic } as Set == ["prices-v1", "reference-data-v5"] as Set
    }

    def "poll-multi defaults a missing offset to 0"() {
        given:
        storage.read("prices-v1", 0, 0L, _) >> [rec(0L, "prices-v1", "p0")]

        when: "only one offset supplied for two topics -> second defaults to 0"
        def resp = server.pollMulti("prices-v1", "")

        then:
        resp.status.code == 200
        1 * storage.read("prices-v1", 0, 0L, _) >> [rec(0L, "prices-v1", "p0")]
    }

    def "poll-multi returns 204 when nothing to serve"() {
        given:
        storage.read(_, _, _, _) >> []

        when:
        def resp = server.pollMulti("prices-v1,reference-data-v5", "0,0")

        then:
        resp.status.code == 204
    }

    def "poll-multi rejects blank topics"() {
        when:
        def resp = server.pollMulti("", "")

        then:
        resp.status.code == 400
    }

    def "poll-multi rejects a non-numeric offset"() {
        when:
        def resp = server.pollMulti("prices-v1", "abc")

        then:
        resp.status.code == 400
    }

    def "head returns per-topic head offsets including -1 for empty"() {
        given:
        storage.getCurrentOffset("prices-v1", 0) >> 41L
        storage.getCurrentOffset("empty-topic", 0) >> -1L

        when:
        def resp = server.head("prices-v1,empty-topic")

        then:
        resp.status.code == 200
        def heads = mapper.readValue(resp.body.get(), Map.class)
        heads["prices-v1"] == 41
        heads["empty-topic"] == -1
    }
}
