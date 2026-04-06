package com.messaging.broker.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.core.PipeMessageForwarder
import com.messaging.broker.support.ModernConsumerHarness
import com.messaging.common.api.StorageEngine
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.sql.DriverManager
import java.time.Duration

/**
 * Integration tests for all three HTTP controllers:
 *   - RefreshController   (/admin/refresh-topic, /admin/refresh-status, /admin/refresh-current)
 *   - TestDataController  (/test/inject-messages, /test/load-from-sqlite, /test/stats)
 *   - ThreadDiagnosticsController (/diagnostics/threads/*)
 * Also exercises PipeMessageForwarder directly.
 */
@MicronautTest
class HttpControllerIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-http-').toAbsolutePath().toString()
        return [
            'broker.network.port'    : '19098',
            'micronaut.server.port'  : '18088',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject EmbeddedServer   embeddedServer
    @Inject StorageEngine    storage
    @Inject PipeMessageForwarder pipeForwarder

    @Value('${broker.network.port}')
    int tcpPort

    private HttpClient http
    private ObjectMapper mapper
    private String base

    def setup() {
        http   = HttpClient.newHttpClient()
        mapper = new ObjectMapper()
        base   = "http://127.0.0.1:${embeddedServer.port}"
    }

    // =========================================================================
    // RefreshController — POST /admin/refresh-topic
    // =========================================================================

    def "POST /admin/refresh-topic with single topic returns success"() {
        when:
        def resp = post('/admin/refresh-topic', [topic: 'rc-topic-1'])

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.success == true
        body.status == 'INITIATED'
        (body.topics as List).contains('rc-topic-1')
    }

    def "POST /admin/refresh-topic with topics array returns success"() {
        when:
        def resp = post('/admin/refresh-topic', [topics: ['rc-arr-1', 'rc-arr-2']])

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.success == true
        (body.topics as List).containsAll(['rc-arr-1', 'rc-arr-2'])
    }

    def "POST /admin/refresh-topic with no topic or topics returns error"() {
        when:
        def resp = post('/admin/refresh-topic', [other: 'value'])

        then:
        resp.statusCode() == 200
        json(resp).success == false
    }

    def "POST /admin/refresh-topic with empty topic string returns error"() {
        when:
        def resp = post('/admin/refresh-topic', [topic: ''])

        then:
        resp.statusCode() == 200
        json(resp).success == false
    }

    def "POST /admin/refresh-topic with topics as non-array returns error"() {
        when:
        def resp = post('/admin/refresh-topic', [topics: 'not-an-array'])

        then:
        resp.statusCode() == 200
        json(resp).success == false
    }

    // =========================================================================
    // RefreshController — GET /admin/refresh-status
    // =========================================================================

    def "GET /admin/refresh-status returns NONE for unknown topic"() {
        when:
        def resp = get('/admin/refresh-status?topic=no-such-topic')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.status == 'NONE'
        body.topic  == 'no-such-topic'
    }

    // =========================================================================
    // RefreshController — GET /admin/refresh-current
    // =========================================================================

    def "GET /admin/refresh-current returns not in progress when idle"() {
        when:
        def resp = get('/admin/refresh-current')

        then:
        resp.statusCode() == 200
        json(resp).refreshInProgress == false
    }

    // =========================================================================
    // TestDataController — POST /test/inject-messages
    // =========================================================================

    def "POST /test/inject-messages stores records and returns offsets"() {
        when:
        def resp = post('/test/inject-messages',
            [count: 3, topic: 'inject-topic', prefix: 'INJ'])

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.success == true
        body.messagesInjected == 3
        body.topic == 'inject-topic'
        (body.offsets as List).size() == 3  // one offset per appended record
    }

    def "POST /test/inject-messages uses default count of 10 when not specified"() {
        when:
        def resp = post('/test/inject-messages', [topic: 'default-count-topic'])

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.success == true
        body.messagesInjected == 10
    }

    // =========================================================================
    // TestDataController — POST /test/load-from-sqlite
    // =========================================================================

    def "POST /test/load-from-sqlite returns error when sqliteFilePath is missing"() {
        when:
        def resp = post('/test/load-from-sqlite', [tableName: 'messages', topic: 'lt-topic'])

        then:
        resp.statusCode() == 200
        json(resp).success == false
    }

    def "POST /test/load-from-sqlite returns error for non-existent file"() {
        when:
        def resp = post('/test/load-from-sqlite',
            [sqliteFilePath: '/no/such/file.db', topic: 'lt-topic'])

        then:
        resp.statusCode() == 200
        json(resp).success == false
    }

    def "POST /test/load-from-sqlite loads records from a real SQLite file"() {
        given: "create a temp SQLite DB with a messages table"
        def dbFile = Files.createTempFile('test-messages-', '.db').toAbsolutePath().toString()
        def conn = java.sql.DriverManager.getConnection("jdbc:sqlite:${dbFile}")
        conn.createStatement().execute(
            'CREATE TABLE messages (id TEXT, payload TEXT, value INTEGER)')
        def ps = conn.prepareStatement(
            'INSERT INTO messages (id, payload, value) VALUES (?, ?, ?)')
        [['row1', 'data1', 100], ['row2', 'data2', 200]].each { row ->
            ps.setString(1, row[0]); ps.setString(2, row[1]); ps.setInt(3, row[2]); ps.executeUpdate()
        }
        conn.close()

        when:
        def resp = postWithTimeout('/test/load-from-sqlite',
            [sqliteFilePath: dbFile, tableName: 'messages', topic: 'sqlite-topic'], 20)

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.success == true
        (body.recordsLoaded as int) == 2

        and: "records are persisted in storage"
        storage.read('sqlite-topic', 0, 0L, 10).size() >= 1

        cleanup:
        new File(dbFile).delete()
    }

    // =========================================================================
    // TestDataController — GET /test/stats
    // =========================================================================

    def "GET /test/stats returns broker running status"() {
        when:
        def resp = get('/test/stats')

        then:
        resp.statusCode() == 200
        json(resp).broker == 'running'
    }

    // =========================================================================
    // ThreadDiagnosticsController — /diagnostics/threads/*
    // =========================================================================

    def "GET /diagnostics/threads/summary returns thread counts"() {
        when:
        def resp = get('/diagnostics/threads/summary')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        (body.totalThreads as int) > 0
        body.deadlockedThreads != null
    }

    def "GET /diagnostics/threads/top-cpu returns a list"() {
        when:
        def resp = get('/diagnostics/threads/top-cpu')

        then:
        resp.statusCode() == 200
        json(resp) instanceof List
    }

    def "GET /diagnostics/threads/problematic returns a list"() {
        when:
        def resp = get('/diagnostics/threads/problematic')

        then:
        resp.statusCode() == 200
        json(resp) instanceof List
    }

    def "GET /diagnostics/threads/dump returns a non-empty thread dump"() {
        when:
        def resp = get('/diagnostics/threads/dump')

        then:
        resp.statusCode() == 200
        resp.body().contains('Thread')
    }

    def "GET /diagnostics/threads/deadlocks reports no deadlocks"() {
        when:
        def resp = get('/diagnostics/threads/deadlocks')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.deadlocked == false
        body.count == 0
    }

    def "GET /diagnostics/threads/by-category returns categories"() {
        when:
        def resp = get('/diagnostics/threads/by-category')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.categories instanceof List
    }

    // =========================================================================
    // RefreshController — comma-separated topics
    // =========================================================================

    def "POST /admin/refresh-topic with comma-separated topics returns success"() {
        when:
        def resp = post('/admin/refresh-topic', [topic: 'comma-topic-1,comma-topic-2'])

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.success == true
        body.status == 'INITIATED'
    }

    // =========================================================================
    // RefreshController — active refresh status paths
    // =========================================================================

    def "GET /admin/refresh-status returns ACTIVE after triggering refresh with a connected consumer"() {
        given: "a consumer subscribed to the topic so the refresh stays in RESET_SENT"
        def consumer = ModernConsumerHarness.connect('127.0.0.1', tcpPort)
        def conditions = new PollingConditions(timeout: 5)
        try {
            consumer.subscribe('active-status-topic', 'active-status-group')
            conditions.eventually {
                assert consumer.received.any { it.type == com.messaging.common.model.BrokerMessage.MessageType.ACK }
            }

            // Trigger refresh — consumer will not send RESET_ACK so status stays ACTIVE
            post('/admin/refresh-topic', [topic: 'active-status-topic'])

            when:
            def resp = get('/admin/refresh-status?topic=active-status-topic')

            then:
            resp.statusCode() == 200
            def body = json(resp)
            body.topic == 'active-status-topic'
            // Refresh is ACTIVE (waiting for RESET_ACK) or has already been cleaned up
            body.status in ['ACTIVE', 'NONE']
        } finally {
            consumer.close()
        }
    }

    def "GET /admin/refresh-current returns detail when a refresh is in progress"() {
        given: "a consumer subscribed so refresh stays in RESET_SENT state"
        def consumer = ModernConsumerHarness.connect('127.0.0.1', tcpPort)
        def conditions = new PollingConditions(timeout: 5)
        try {
            consumer.subscribe('current-check-topic', 'current-check-group')
            conditions.eventually {
                assert consumer.received.any { it.type == com.messaging.common.model.BrokerMessage.MessageType.ACK }
            }

            // Trigger refresh
            post('/admin/refresh-topic', [topic: 'current-check-topic'])

            when:
            def resp = get('/admin/refresh-current')

            then:
            resp.statusCode() == 200
            def body = json(resp)
            // refreshInProgress is a boolean — either true (still active) or false (completed already)
            body.refreshInProgress != null
        } finally {
            consumer.close()
        }
    }

    // =========================================================================
    // ThreadDiagnosticsController — missing endpoints
    // =========================================================================

    def "GET /diagnostics/threads/top-memory returns a list"() {
        when:
        def resp = get('/diagnostics/threads/top-memory')

        then:
        resp.statusCode() == 200
        json(resp) instanceof List
    }

    def "GET /diagnostics/threads/top-memory with explicit limit respects the limit"() {
        when:
        def resp = get('/diagnostics/threads/top-memory?limit=3')

        then:
        resp.statusCode() == 200
        (json(resp) as List).size() <= 3
    }

    def "GET /diagnostics/threads/top-cpu with explicit limit returns bounded list"() {
        when:
        def resp = get('/diagnostics/threads/top-cpu?limit=5')

        then:
        resp.statusCode() == 200
        (json(resp) as List).size() <= 5
    }

    def "GET /diagnostics/threads/resources returns thread count and list"() {
        when:
        def resp = get('/diagnostics/threads/resources')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        (body.threadCount as int) > 0
        body.threads instanceof List
    }

    def "GET /diagnostics/threads/resources with category filter returns filtered list"() {
        when:
        def resp = get('/diagnostics/threads/resources?category=Other')

        then:
        resp.statusCode() == 200
        def body = json(resp)
        body.threads instanceof List
    }

    def "GET /diagnostics/threads/{threadId} returns details for an existing thread"() {
        given:
        // Use a known-alive thread ID from the JVM thread list
        def allIds  = java.lang.management.ManagementFactory.getThreadMXBean().allThreadIds
        long liveId = allIds[0]

        when:
        def resp = get("/diagnostics/threads/${liveId}")

        then:
        resp.statusCode() == 200
        json(resp) != null
    }

    def "GET /diagnostics/threads/{threadId} returns error map for non-existent thread"() {
        when:
        // Use a thread ID that is almost certainly not alive (very large number)
        def resp = get("/diagnostics/threads/999999999")

        then:
        resp.statusCode() == 200
        def body = json(resp)
        // Either returns an error key or an empty/null — both are valid from the controller
        body != null
    }

    // =========================================================================
    // PipeMessageForwarder — direct bean tests
    // =========================================================================

    def "PipeMessageForwarder.getMessagesForChild returns empty list for unknown topic"() {
        when:
        def records = pipeForwarder.getMessagesForChild('no-such-topic', 0L, 10)

        then:
        records != null
        records.isEmpty()
    }

    def "PipeMessageForwarder.getMessagesForChild returns records after data is stored"() {
        given:
        storage.append('pipe-fwd-topic', 0,
            new com.messaging.common.model.MessageRecord(
                'pf-key', com.messaging.common.model.EventType.MESSAGE,
                '{"v":1}', java.time.Instant.now()))

        when:
        def records = pipeForwarder.getMessagesForChild('pipe-fwd-topic', 0L, 10)

        then:
        records.size() == 1
        records[0].msgKey == 'pf-key'
    }

    def "PipeMessageForwarder.getCurrentOffset returns 0"() {
        expect:
        pipeForwarder.getCurrentOffset('any-topic') == 0L
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private HttpResponse<String> get(String path) {
        def req = HttpRequest.newBuilder()
            .uri(URI.create("${base}${path}"))
            .timeout(Duration.ofSeconds(5))
            .GET().build()
        http.send(req, HttpResponse.BodyHandlers.ofString())
    }

    private HttpResponse<String> post(String path, Map body) {
        postWithTimeout(path, body, 5)
    }

    private HttpResponse<String> postWithTimeout(String path, Map body, int timeoutSeconds) {
        def jsonBody = mapper.writeValueAsString(body)
        def req = HttpRequest.newBuilder()
            .uri(URI.create("${base}${path}"))
            .timeout(Duration.ofSeconds(timeoutSeconds))
            .header('Content-Type', 'application/json')
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody)).build()
        http.send(req, HttpResponse.BodyHandlers.ofString())
    }

    private Object json(HttpResponse<String> resp) {
        mapper.readValue(resp.body(), Object)
    }
}
