package com.messaging.pipe

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import spock.lang.Specification

import java.time.Instant

/**
 * Unit tests for /pipe/poll. Merge mode (X-Pipe-Cursors header present) does a k-way merge across
 * topics with per-topic exclusive cursors — the key property is NO DUPLICATES across polls. Legacy
 * mode (no header) keeps the original single-topic behavior for the existing steady-state client.
 */
class PipeServerSpec extends Specification {

    StorageEngine storage = Mock()
    PipeServer server = new PipeServer(storage)
    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()

    Map<String, List<MessageRecord>> backing = [:]

    def rec(long offset, String topic, String key) {
        new MessageRecord(offset, topic, 0, key, null, "{}", Instant.now())
    }

    def setup() {
        storage.read(_, _, _, _) >> { String t, int p, long from, int lim ->
            backing.getOrDefault(t, []).findAll { it.offset >= from }.take(lim)
        }
        storage.getCurrentOffset(_, _) >> { String t, int p ->
            def list = backing.getOrDefault(t, [])
            list.isEmpty() ? -1L : list[-1].offset
        }
        storage.getTopicNames() >> { backing.keySet() }
    }

    private List<MessageRecord> records(resp) {
        mapper.readValue(resp.body.get(), MessageRecord[].class) as List
    }

    private Map cursorsOf(resp) {
        mapper.readValue(resp.headers.get(PipeServer.CURSORS_HEADER), Map.class)
    }

    // ── Merge mode (header present) ──────────────────────────────────────────

    def "k-way merges across topics with disjoint offset ranges, ordered by offset"() {
        given:
        backing['topic-a'] = [rec(10000L, 'topic-a', 'a0'), rec(10002L, 'topic-a', 'a1'), rec(11000L, 'topic-a', 'a2')]
        backing['topic-b'] = [rec(20000L, 'topic-b', 'b0'), rec(21000L, 'topic-b', 'b1')]

        when:
        def resp = server.pollMessages("{}", 0L, 100, "")

        then:
        resp.status.code == 200
        records(resp).collect { it.offset } == [10000L, 10002L, 11000L, 20000L, 21000L]
        cursorsOf(resp)['topic-a'] == 11000
        cursorsOf(resp)['topic-b'] == 21000

        and: "heads header carries per-topic head offsets"
        def heads = mapper.readValue(resp.headers.get(PipeServer.HEADS_HEADER), Map.class)
        heads['topic-a'] == 11000
        heads['topic-b'] == 21000
    }

    def "NO DUPLICATES: a second poll with the returned cursors yields only new records"() {
        given:
        backing['topic-a'] = [rec(10000L, 'topic-a', 'a0'), rec(11000L, 'topic-a', 'a1')]
        backing['topic-b'] = [rec(20000L, 'topic-b', 'b0')]

        when:
        def first = server.pollMessages("{}", 0L, 100, "")

        then:
        records(first).collect { it.offset } == [10000L, 11000L, 20000L]

        when: "more data arrives on topic-a, then poll again with the returned cursors"
        backing['topic-a'] << rec(12000L, 'topic-a', 'a2')
        def cursorHeader = mapper.writeValueAsString(cursorsOf(first))
        def second = server.pollMessages(cursorHeader, 0L, 100, "")

        then: "only the NEW record is returned — nothing re-sent"
        records(second).collect { it.offset } == [12000L]
    }

    def "the floor offset applies only to topics absent from the cursor map"() {
        given:
        backing['topic-a'] = [rec(10000L, 'topic-a', 'a0'), rec(10500L, 'topic-a', 'a1')]

        when:
        def resp = server.pollMessages("{}", 10200L, 100, "")

        then:
        records(resp).collect { it.offset } == [10500L]
    }

    def "a topic cursor is exclusive — the boundary record is not re-sent"() {
        given:
        backing['topic-a'] = [rec(10000L, 'topic-a', 'a0'), rec(10001L, 'topic-a', 'a1')]
        def header = mapper.writeValueAsString(['topic-a': 10000L])

        when:
        def resp = server.pollMessages(header, 0L, 100, "")

        then:
        records(resp).collect { it.offset } == [10001L]
    }

    def "optional topic filter restricts the merge to one topic"() {
        given:
        backing['topic-a'] = [rec(1L, 'topic-a', 'a0')]
        backing['topic-b'] = [rec(2L, 'topic-b', 'b0')]

        when:
        def resp = server.pollMessages("{}", 0L, 100, "topic-a")

        then:
        records(resp).collect { it.topic } as Set == ['topic-a'] as Set
    }

    def "merge mode returns 204 with cursor+head headers when caught up"() {
        given:
        backing['topic-a'] = [rec(5L, 'topic-a', 'a0')]
        def header = mapper.writeValueAsString(['topic-a': 5L])

        when:
        def resp = server.pollMessages(header, 0L, 100, "")

        then:
        resp.status.code == 204
        resp.headers.get(PipeServer.HEADS_HEADER) != null
    }

    // ── Legacy mode (no header) ──────────────────────────────────────────────

    def "legacy mode (no cursor header) serves a single topic from the offset"() {
        given:
        backing['price-topic'] = [rec(0L, 'price-topic', 'p0'), rec(1L, 'price-topic', 'p1')]

        when: "no cursor header, explicit topic"
        def resp = server.pollMessages("", 0L, 100, "price-topic")

        then: "all records of that topic from the offset (inclusive, legacy semantics)"
        records(resp).collect { it.offset } == [0L, 1L]

        and: "no merge headers in legacy mode"
        resp.headers.get(PipeServer.CURSORS_HEADER) == null
    }
}
