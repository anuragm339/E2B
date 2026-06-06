package com.messaging.broker.consistency

import com.messaging.broker.core.TopologyManager
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micronaut.http.HttpResponse
import spock.lang.Specification

import java.time.Instant
import java.util.stream.Collectors

/**
 * Unit coverage for Fix 2 — /pipe/consistency/range cursor pagination.
 *
 * Verifies the controller:
 *   - Returns first records >= {@code from} on the first request (cursor absent).
 *   - Returns records with offset strictly > cursor on subsequent requests.
 *   - Does NOT duplicate the last record from the previous page (the off-by-one
 *     that page-arithmetic pagination had).
 *   - Does NOT skip records when storage caps a single read at fewer than
 *     pageSize records (the 1 MB-cap accumulator behaviour).
 *   - Stops cleanly at {@code to}; nextCursor is null when no more records.
 */
class BrokerPipeConsistencyControllerSpec extends Specification {

    BrokerSegmentHashView hashView = Mock()
    HashCache hashCache = Mock()
    TopologyManager topology = Stub() { getCurrentParentUrl() >> 'http://parent' }
    StorageEngine storage = Stub()

    BrokerPipeConsistencyController controller

    def setup() {
        controller = new BrokerPipeConsistencyController(
                hashView, hashCache, storage, topology, true, 'broker-test')
    }

    /** Build a MessageRecord with a stable JSON-ish data field. */
    private static MessageRecord rec(long off, String key) {
        new MessageRecord(off, 'prices-v1', 0, key, EventType.MESSAGE, "{\"v\":${off}}", Instant.now())
    }

    /** Stub storage.read so that given a query (from, max), returns records >= from from `data`. */
    private void stubSparseStorage(List<MessageRecord> data) {
        storage.read('prices-v1', 0, _ as Long, _ as Integer) >> { String t, int p, long from, int max ->
            data.findAll { it.offset >= from }.take(max)
        }
    }

    /** Stub storage.read with a per-call cap (e.g. 1MB ≈ ~50 records) to simulate the real
     * SegmentManager behaviour where a single call returns fewer than `max` records. */
    private void stubCappedStorage(List<MessageRecord> data, int perCallCap) {
        storage.read('prices-v1', 0, _ as Long, _ as Integer) >> { String t, int p, long from, int max ->
            data.findAll { it.offset >= from }.take(Math.min(max, perCallCap))
        }
    }

    def "first page (no cursor) returns records starting at `from`"() {
        given:
        stubSparseStorage([1000L, 1100L, 1200L, 1300L].collect { rec(it, "k${it}") })

        when:
        HttpResponse resp = controller.range('prices-v1', 0L, 2000L, Optional.empty(), 2)
        def body = resp.body.get()

        then:
        body.records.size() == 2
        body.records*.offset == [1000L, 1100L]
        body.nextCursor == 1100L
        body.cursor == null
    }

    def "second page with cursor=lastOffsetSeen returns strictly-greater offsets, no duplicates"() {
        given:
        stubSparseStorage([1000L, 1100L, 1200L, 1300L].collect { rec(it, "k${it}") })

        when: 'caller passes back the previous nextCursor'
        HttpResponse resp = controller.range('prices-v1', 0L, 2000L, Optional.of(1100L), 2)
        def body = resp.body.get()

        then: 'records all have offset > 1100 — no overlap with page 1'
        body.records*.offset == [1200L, 1300L]
        body.records.every { it.offset > 1100L }
        body.cursor == 1100L
        body.nextCursor == 1300L
    }

    def "paginating until empty returns the full ordered set with no duplicates and no skips"() {
        given:
        def offsets = [1000L, 1042L, 1100L, 1373L, 1500L, 1873L, 2000L, 2456L]
        stubSparseStorage(offsets.collect { rec(it, "k${it}") })

        when: 'walk pages until empty, pageSize=3'
        List<Long> collected = []
        Long cursor = null
        for (int i = 0; i < 10; i++) {
            HttpResponse r = controller.range('prices-v1', 0L, 5000L,
                    cursor == null ? Optional.empty() : Optional.of(cursor), 3)
            def page = r.body.get().records*.offset
            if (page.isEmpty()) break
            collected.addAll(page as List<Long>)
            cursor = r.body.get().nextCursor as Long
            if (cursor == null) break
        }

        then: 'we collected EXACTLY the input set, in offset order, with no duplicates'
        collected == offsets
        collected.toSet().size() == collected.size()  // no dupes
    }

    def "accumulates beyond storage's per-call cap until pageSize is filled"() {
        given: 'storage caps at 3 records per call (simulating the 1MB cap on real SegmentManager)'
        def records = (1000L..1010L).collect { rec(it, "k${it}") }
        stubCappedStorage(records, 3)

        when:
        HttpResponse resp = controller.range('prices-v1', 0L, 2000L, Optional.empty(), 10)
        def body = resp.body.get()

        then: 'page is filled to 10 records via multiple storage.read calls'
        body.records.size() == 10
        body.records*.offset == (1000L..1009L).toList()
    }

    def "respects upper bound `to` and returns nextCursor=null when exhausted"() {
        given:
        stubSparseStorage([1000L, 1100L, 1500L, 5000L].collect { rec(it, "k${it}") })

        when:
        HttpResponse resp = controller.range('prices-v1', 0L, 2000L, Optional.empty(), 100)
        def body = resp.body.get()

        then: 'only records with offset <= 2000 returned'
        body.records*.offset == [1000L, 1100L, 1500L]

        and: 'cursor walk past last record returns empty'
        HttpResponse r2 = controller.range('prices-v1', 0L, 2000L, Optional.of(1500L), 100)
        r2.body.get().records.isEmpty()
    }

    def "no records in range returns empty list and nextCursor=null without error"() {
        given:
        stubSparseStorage([])

        when:
        HttpResponse resp = controller.range('prices-v1', 1_000_000L, 1_010_000L, Optional.empty(), 100)
        def body = resp.body.get()

        then:
        body.records.isEmpty()
        body.nextCursor == null
    }

    def "disabled endpoint returns a server-error response"() {
        given:
        controller = new BrokerPipeConsistencyController(
                hashView, hashCache, storage, topology, false, 'broker-test')

        when:
        HttpResponse resp = controller.range('prices-v1', 0L, 100L, Optional.empty(), 10)

        then:
        resp.status.code >= 500
    }
}
