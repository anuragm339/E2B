package com.messaging.broker.snapshot

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant

/**
 * Unit tests for the testable seams of HttpBootstrapSourceClient: offset-idempotent dedup on ingest
 * and response parsing. The HTTP orchestration loops are integration-level.
 */
class HttpBootstrapSourceClientSpec extends Specification {

    StorageEngine storage = Mock()
    HttpBootstrapSourceClient client = new HttpBootstrapSourceClient(storage, new BootstrapProgressTracker(), "http://cloud", 1000)
    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()

    def rec(long offset, String topic) {
        new MessageRecord(offset, topic, 0, "k" + offset, null, "{}", Instant.now())
    }

    def "ingestRecords appends new records and skips already-stored ones (dedup)"() {
        given: "topic head is at offset 10"
        storage.getCurrentOffset("t", 0) >> 10L

        when:
        client.ingestRecords([rec(5L, "t"), rec(15L, "t")])

        then: "offset 5 (<= head) skipped; offset 15 (> head) appended"
        0 * storage.append("t", 0, { it.offset == 5L })
        1 * storage.append("t", 0, { it.offset == 15L })
    }

    def "ingestRecords always appends offset-0 records (the dedup guard is offset > 0)"() {
        given:
        storage.getCurrentOffset("t", 0) >> 10L

        when:
        client.ingestRecords([rec(0L, "t")])

        then:
        1 * storage.append("t", 0, { it.offset == 0L })
    }

    def "parseRecords parses a JSON array of records"() {
        given:
        def json = mapper.writeValueAsString([rec(1L, "t"), rec(2L, "t")])

        expect:
        client.parseRecords(json).collect { it.offset } == [1L, 2L]
    }

    def "parseRecords returns empty for blank/null body"() {
        expect:
        client.parseRecords(null).isEmpty()
        client.parseRecords("").isEmpty()
    }
}
