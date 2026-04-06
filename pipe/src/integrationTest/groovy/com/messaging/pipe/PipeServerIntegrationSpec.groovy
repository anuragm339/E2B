package com.messaging.pipe

import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
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
 * Integration tests for PipeServer — the /pipe/poll HTTP endpoint.
 *
 * Uses @MicronautTest so the real Micronaut HTTP stack (routing, serialization,
 * query-param binding) is exercised.  StorageEngine is replaced with a Spock mock
 * via @MockBean so tests run without a real storage directory.
 */
@MicronautTest
class PipeServerIntegrationSpec extends Specification {

    @Inject EmbeddedServer embeddedServer
    @Inject StorageEngine  storage

    @MockBean(StorageEngine)
    StorageEngine mockStorage() { Mock(StorageEngine) }

    // Java HTTP client (no Micronaut context required — just a JDK helper)
    private final HttpClient http = HttpClient.newHttpClient()

    // =========================================================================
    // Helpers
    // =========================================================================

    private HttpResponse<String> get(String path) {
        def req = HttpRequest.newBuilder()
            .uri(new URI("http://localhost:${embeddedServer.port}${path}"))
            .timeout(java.time.Duration.ofSeconds(5))
            .GET().build()
        return http.send(req, HttpResponse.BodyHandlers.ofString())
    }

    // =========================================================================
    // GET /pipe/poll — empty storage
    // =========================================================================

    def "returns 204 No Content when storage has no records"() {
        given:
        storage.read('price-topic', 0, 0L, 100) >> []

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=price-topic')

        then:
        resp.statusCode() == 204
    }

    // =========================================================================
    // GET /pipe/poll — records present
    // =========================================================================

    def "returns 200 with JSON array when storage has records"() {
        given:
        def records = [
            new MessageRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.now()),
            new MessageRecord('key-2', EventType.MESSAGE, '{"v":2}', Instant.now())
        ]
        storage.read('price-topic', 0, 0L, 100) >> records

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=price-topic')

        then:
        resp.statusCode() == 200
        def body = resp.body()
        body.startsWith('[')
        body.endsWith(']')
        body.contains('key-1')
        body.contains('key-2')
    }

    def "serialized JSON contains msgKey and eventType fields"() {
        given:
        def record = new MessageRecord('my-key', EventType.MESSAGE, '{"data":1}', Instant.now())
        storage.read('my-topic', 0, 5L, 10) >> [record]

        when:
        def resp = get('/pipe/poll?offset=5&limit=10&topic=my-topic')

        then:
        resp.statusCode() == 200
        def body = resp.body()
        body.contains('my-key')
        body.contains('MESSAGE')
    }

    def "DELETE event type is serialized correctly"() {
        given:
        def record = new MessageRecord('del-key', EventType.DELETE, null, Instant.now())
        storage.read('del-topic', 0, 0L, 100) >> [record]

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=del-topic')

        then:
        resp.statusCode() == 200
        def body = resp.body()
        body.contains('del-key')
        body.contains('DELETE')
    }

    // =========================================================================
    // GET /pipe/poll — offset and limit forwarded to storage
    // =========================================================================

    def "offset parameter is forwarded to storage.read"() {
        given:
        storage.read('offset-topic', 0, 42L, 100) >> []

        when:
        def resp = get('/pipe/poll?offset=42&limit=100&topic=offset-topic')

        then:
        resp.statusCode() == 204
        1 * storage.read('offset-topic', 0, 42L, 100) >> []
    }

    def "limit parameter is forwarded to storage.read"() {
        given:
        storage.read('limit-topic', 0, 0L, 5) >> []

        when:
        def resp = get('/pipe/poll?offset=0&limit=5&topic=limit-topic')

        then:
        resp.statusCode() == 204
        1 * storage.read('limit-topic', 0, 0L, 5) >> []
    }

    def "topic parameter is forwarded to storage.read"() {
        given:
        storage.read('custom-topic', 0, 0L, 100) >> []

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=custom-topic')

        then:
        resp.statusCode() == 204
        1 * storage.read('custom-topic', 0, 0L, 100) >> []
    }

    // =========================================================================
    // GET /pipe/poll — error handling
    // =========================================================================

    def "returns 500 when storage throws an exception"() {
        given:
        storage.read('err-topic', 0, 0L, 100) >> { throw new RuntimeException("disk failure") }

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=err-topic')

        then:
        resp.statusCode() == 500
    }

    def "error response body contains error description"() {
        given:
        storage.read('err-topic2', 0, 0L, 100) >> { throw new RuntimeException("segment corrupt") }

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=err-topic2')

        then:
        resp.statusCode() == 500
        resp.body().contains('segment corrupt')
    }

    // =========================================================================
    // GET /pipe/poll — single record
    // =========================================================================

    def "single record is returned as a JSON array with one element"() {
        given:
        storage.read('single-topic', 0, 0L, 100) >> [
            new MessageRecord('only-key', EventType.MESSAGE, '{}', Instant.now())
        ]

        when:
        def resp = get('/pipe/poll?offset=0&limit=100&topic=single-topic')

        then:
        resp.statusCode() == 200
        def body = resp.body()
        body.startsWith('[')
        body.endsWith(']')
        body.contains('only-key')
    }
}
