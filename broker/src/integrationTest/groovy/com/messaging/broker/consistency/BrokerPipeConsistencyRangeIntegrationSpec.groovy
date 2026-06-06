package com.messaging.broker.consistency

import com.messaging.broker.support.BrokerHttpSpecSupport
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micronaut.test.extensions.spock.annotation.MicronautTest

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Integration coverage for Fix 2 — exercises the real {@code /pipe/consistency/range}
 * controller wired against a real {@link com.messaging.common.api.StorageEngine}.
 *
 * <p>Pre-fix, the controller paginated by offset arithmetic ({@code from + page*pageSize})
 * which silently skipped or duplicated records on sparse offset streams. This spec
 * appends records and walks the cursor pages, asserting the round-trip recovers
 * exactly the input set, ordered, with no duplicates and no skips.
 *
 * <p>Each feature uses a unique topic to avoid sharing storage state across methods
 * within the same Micronaut application context.
 */
@MicronautTest
class BrokerPipeConsistencyRangeIntegrationSpec extends BrokerHttpSpecSupport {

    private static final AtomicInteger TOPIC_COUNTER = new AtomicInteger()
    private String topic

    def setup() {
        topic = "consistency-range-${TOPIC_COUNTER.incrementAndGet()}"
    }

    private void seedDenseRecords(int count) {
        count.times { i ->
            storage.append(topic, 0, new MessageRecord(
                    "k${i}".toString(), EventType.MESSAGE, "{\"i\":${i}}".toString(), Instant.now()))
        }
    }

    private void seedSparseRecords(List<Long> offsets) {
        offsets.each { off ->
            storage.append(topic, 0, new MessageRecord(
                    off, topic, 0, "k${off}", EventType.MESSAGE, "{\"i\":${off}}".toString(), Instant.now()))
        }
    }

    /**
     * Returns the parsed body normalised so that missing keys (Micronaut/Jackson omits
     * nulls + empty collections from controller responses) become empty list / null.
     */
    private Map fetchPage(Long cursor, int pageSize) {
        def url = "/pipe/consistency/range?topic=${topic}&from=0&to=1000000&pageSize=${pageSize}"
        if (cursor != null) url += "&cursor=${cursor}"
        def resp = get(url)
        assert resp.statusCode() == 200
        def body = json(resp) as Map
        body.records = body.records ?: []
        if (!body.containsKey('nextCursor')) body.nextCursor = null
        if (!body.containsKey('cursor')) body.cursor = null
        return body
    }

    def "first page (no cursor) returns first records and a nextCursor"() {
        given:
        seedDenseRecords(10)

        when:
        def body = fetchPage(null, 3)

        then:
        body.records.size() == 3
        body.records*.offset as List<Long> == [0L, 1L, 2L]
        (body.nextCursor as Long) == 2L
        body.cursor == null
    }

    def "walking by cursor recovers the full ordered set with no duplicates and no skips"() {
        given:
        seedDenseRecords(23)  // 23 records → 5 pages of 5 + final page of 3

        when:
        def collected = [] as List<Long>
        Long cursor = null
        for (int i = 0; i < 30; i++) {
            def page = fetchPage(cursor, 5)
            def offs = (page.records*.offset as List<Long>)
            if (offs.isEmpty()) break
            collected.addAll(offs)
            def nc = page.nextCursor
            if (nc == null) break
            cursor = nc as Long
        }

        then: 'collected exactly 23 records, contiguous offsets 0..22, no dupes'
        collected == (0L..22L).toList()
        collected.toSet().size() == collected.size()
    }

    def "walking by cursor recovers the full sparse-offset set with no duplicates and no skips"() {
        given:
        def offsets = [0L, 42L, 100L, 873L, 1_200L, 2_005L, 5_123L, 9_000L, 9_999L]
        seedSparseRecords(offsets)

        when:
        def collected = [] as List<Long>
        Long cursor = null
        for (int i = 0; i < 20; i++) {
            def page = fetchPage(cursor, 3)
            def offs = page.records*.offset as List<Long>
            if (offs.isEmpty()) break
            collected.addAll(offs)
            def nc = page.nextCursor
            if (nc == null) break
            cursor = nc as Long
        }

        then:
        collected == offsets
        collected.toSet().size() == collected.size()
    }

    def "cursor at the last record returns an empty page (terminates walk)"() {
        given:
        seedDenseRecords(5)  // offsets 0..4

        when:
        def page = fetchPage(4L, 100)

        then:
        page.records.isEmpty()
        page.nextCursor == null
    }

    def "empty topic returns no records and null nextCursor without error"() {
        when: 'no seed — use this method\'s unique topic'
        def page = fetchPage(null, 100)

        then:
        page.records.isEmpty()
        page.nextCursor == null
    }
}
