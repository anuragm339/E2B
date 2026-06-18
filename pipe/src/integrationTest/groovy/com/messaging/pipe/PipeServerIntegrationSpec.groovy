package com.messaging.pipe

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.MessageRecord
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Instant

/**
 * Integration tests for the unified k-way-merge /pipe/poll over the real Micronaut HTTP stack:
 * global merge across topics, per-topic cursor map in the X-Pipe-Cursors request/response header,
 * and the no-duplicate guarantee across sequential polls.
 */
@MicronautTest
class PipeServerIntegrationSpec extends Specification {

    @Inject EmbeddedServer embeddedServer
    @Inject StorageEngine  storage

    @MockBean(StorageEngine)
    StorageEngine mockStorage() { Mock(StorageEngine) }

    private final HttpClient http = HttpClient.newHttpClient()
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()

    Map<String, List<MessageRecord>> backing = [:]

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

    private MessageRecord rec(long offset, String topic, String key) {
        new MessageRecord(offset, topic, 0, key, com.messaging.common.model.EventType.MESSAGE, '{}', Instant.now())
    }

    private HttpResponse<String> poll(String cursorsJson = '{}', long offset = 0) {
        // Always send the X-Pipe-Cursors header → merge mode (empty map "{}" = first poll).
        def builder = HttpRequest.newBuilder()
            .uri(new URI("http://localhost:${embeddedServer.port}/pipe/poll?offset=${offset}"))
            .timeout(java.time.Duration.ofSeconds(5))
            .header(PipeServer.CURSORS_HEADER, cursorsJson)
            .GET()
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    def "global poll k-way merges all topics ordered by offset, returns cursor + head headers"() {
        given:
        backing['topic-a'] = [rec(10000L, 'topic-a', 'a0'), rec(11000L, 'topic-a', 'a1')]
        backing['topic-b'] = [rec(20000L, 'topic-b', 'b0')]

        when:
        def resp = poll()

        then:
        resp.statusCode() == 200
        def body = resp.body()
        body.indexOf('a0') < body.indexOf('a1')
        body.indexOf('a1') < body.indexOf('b0')

        and:
        def cursors = mapper.readValue(resp.headers().firstValue(PipeServer.CURSORS_HEADER).get(), Map)
        cursors['topic-a'] == 11000
        cursors['topic-b'] == 20000
        resp.headers().firstValue(PipeServer.HEADS_HEADER).isPresent()
    }

    def "no duplicates: replaying the returned cursors yields only newly-arrived records"() {
        given:
        backing['topic-a'] = [rec(10000L, 'topic-a', 'a0')]

        when: "drain, then more arrives, then poll with the returned cursors"
        def first = poll()
        backing['topic-a'] << rec(12000L, 'topic-a', 'a1')
        def cursorHeader = first.headers().firstValue(PipeServer.CURSORS_HEADER).get()
        def second = poll(cursorHeader)

        then:
        second.statusCode() == 200
        def records = mapper.readValue(second.body(), MessageRecord[])
        records.collect { it.offset } == [12000L]
    }

    def "returns 204 when everything is caught up"() {
        given:
        backing['topic-a'] = [rec(5L, 'topic-a', 'a0')]

        when:
        def resp = poll(mapper.writeValueAsString(['topic-a': 5L]))

        then:
        resp.statusCode() == 204
    }
}
