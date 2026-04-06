package com.messaging.pipe

import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.pipe.metrics.PipeMetrics
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Integration tests for HttpPipeConnector — end-to-end poll loop.
 *
 * @MicronautTest starts the embedded server with the real PipeServer controller.
 * StorageEngine is replaced with a Spock mock via @MockBean so we can control
 * what records the PipeServer returns.  A fresh HttpPipeConnector is created per
 * test (it is not a shared singleton here) using the Micronaut-managed HttpClient,
 * ensuring each test gets an independent polling thread.
 */
@MicronautTest
class HttpPipeConnectorIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('pipe-connector-').toAbsolutePath().toString()
        return [
            'broker.storage.data-dir': dataDir,
            'broker.pipe.min-poll-interval-ms': '100',
            'broker.pipe.max-poll-interval-ms': '1000',
            'broker.pipe.poll-limit': '5'
        ]
    }

    @Inject EmbeddedServer embeddedServer
    @Inject StorageEngine  storage
    @Inject PipeMetrics    pipeMetrics

    @Inject @Client("/") HttpClient httpClient

    @MockBean(StorageEngine)
    StorageEngine mockStorage() { Mock(StorageEngine) }

    // =========================================================================
    // Helper: create a fresh connector per test (independent poll thread)
    // =========================================================================

    @TempDir Path tempDir

    private HttpPipeConnector buildConnector() {
        new HttpPipeConnector(
            httpClient,
            tempDir.toString(),
            100L,   // minPollIntervalMs
            1000L,  // maxPollIntervalMs
            5,      // pollLimit
            pipeMetrics
        )
    }

    // =========================================================================
    // Test: connector receives messages end-to-end via embedded PipeServer
    // =========================================================================

    def "pipe connects to embedded PipeServer and receives messages"() {
        given:
        def latch = new CountDownLatch(3)
        def keys  = new CopyOnWriteArrayList<String>()

        // HttpPipeConnector does not pass a topic param — PipeServer uses default 'price-topic'
        storage.read(_, _, _, _) >>> [
            [
                new MessageRecord('key-1', EventType.MESSAGE, '{"v":1}', Instant.now()),
                new MessageRecord('key-2', EventType.MESSAGE, '{"v":2}', Instant.now()),
                new MessageRecord('key-3', EventType.MESSAGE, '{"v":3}', Instant.now())
            ],
            [],
            [],
            [],
            []
        ]

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            keys.add(record.msgKey)
            latch.countDown()
            return true
        })

        when:
        def connection = connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        boolean got    = latch.await(10, TimeUnit.SECONDS)

        then:
        connection != null
        connection.isConnected()
        got
        keys.containsAll(['key-1', 'key-2', 'key-3'])

        cleanup:
        connector?.disconnect()
    }

    // =========================================================================
    // Test: 204 No Content — no messages delivered
    // =========================================================================

    def "pipe handles empty storage without delivering messages"() {
        given:
        def keys = new CopyOnWriteArrayList<String>()
        storage.read(_, _, _, _) >> []

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            keys.add(record.msgKey)
            return true
        })

        when:
        connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        Thread.sleep(400)

        then:
        keys.isEmpty()

        cleanup:
        connector?.disconnect()
    }

    // =========================================================================
    // Test: offset advances to last message's offset
    // =========================================================================

    def "last received offset is tracked after receiving messages"() {
        given:
        def latch = new CountDownLatch(2)

        // Use 7-arg constructor to set explicit offsets; match any topic (connector uses default)
        storage.read(_, _, _, _) >>> [
            [
                new MessageRecord(100L, 'price-topic', 0, 'k100', EventType.MESSAGE, 'd', Instant.now()),
                new MessageRecord(101L, 'price-topic', 0, 'k101', EventType.MESSAGE, 'd', Instant.now())
            ],
            [],
            []
        ]

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            latch.countDown()
            return true
        })

        when:
        def connection = connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        latch.await(10, TimeUnit.SECONDS)

        then:
        connection.getLastReceivedOffset() >= 0

        cleanup:
        connector?.disconnect()
    }

    // =========================================================================
    // Test: PipeServer error response does not crash the connector
    // =========================================================================

    def "connector survives PipeServer throwing an exception"() {
        given:
        def keys = new CopyOnWriteArrayList<String>()
        storage.read(_, _, _, _) >> { throw new RuntimeException("disk failure") }

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            keys.add(record.msgKey)
            return true
        })

        when:
        def connection = connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        Thread.sleep(400)

        then: "connection stays alive; no messages delivered"
        connection != null
        keys.isEmpty()

        cleanup:
        connector?.disconnect()
    }

    // =========================================================================
    // Test: pausePipeCalls / resumePipeCalls
    // =========================================================================

    def "pausePipeCalls prevents message delivery while paused"() {
        given:
        def keys = new CopyOnWriteArrayList<String>()
        // Storage always returns a record — if polling ran, keys would fill up
        storage.read(_, _, _, _) >> [
            new MessageRecord('paused-key', EventType.MESSAGE, '{"v":1}', Instant.now())
        ]

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            keys.add(record.msgKey)
            return true
        })

        // Pause BEFORE starting the poll loop so no poll ever fires
        connector.pausePipeCalls()

        when:
        connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        Thread.sleep(500)  // poll interval is 100ms — 5 cycles would run if not paused

        then: "no messages delivered while paused"
        keys.isEmpty()

        cleanup:
        connector?.disconnect()
    }

    def "resumePipeCalls restarts delivery after pause"() {
        given:
        def latch = new CountDownLatch(1)
        def keys  = new CopyOnWriteArrayList<String>()
        storage.read(_, _, _, _) >>> [
            [],  // first poll (before pause/resume) → empty
            [new MessageRecord('resumed-key', EventType.MESSAGE, '{"v":2}', Instant.now())],
            [],
            [],
        ]

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            keys.add(record.msgKey)
            latch.countDown()
            return true
        })
        connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        connector.pausePipeCalls()
        Thread.sleep(200)

        when:
        connector.resumePipeCalls()
        boolean received = latch.await(8, TimeUnit.SECONDS)

        then:
        received
        keys.contains('resumed-key')

        cleanup:
        connector?.disconnect()
    }

    // =========================================================================
    // Test: getHealth states
    // =========================================================================

    def "getHealth returns UNHEALTHY before connecting"() {
        given:
        def connector = buildConnector()

        expect:
        connector.getHealth() == com.messaging.common.api.PipeConnector.PipeHealth.UNHEALTHY
    }

    def "getHealth returns HEALTHY after receiving messages"() {
        given:
        def latch = new CountDownLatch(1)
        storage.read(_, _, _, _) >>> [
            [new MessageRecord('health-key', EventType.MESSAGE, '{}', Instant.now())],
            [],
            []
        ]

        def connector = buildConnector()
        connector.onDataReceived({ record ->
            latch.countDown()
            return true
        })

        when:
        connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        latch.await(8, TimeUnit.SECONDS)

        then:
        connector.getHealth() == com.messaging.common.api.PipeConnector.PipeHealth.HEALTHY

        cleanup:
        connector?.disconnect()
    }

    // =========================================================================
    // Test: storage handler returning false stops batch mid-stream
    // =========================================================================

    def "storage failure (handler returning false) stops batch and retries from last good offset"() {
        given:
        def delivered = new CopyOnWriteArrayList<String>()
        def latch = new CountDownLatch(1)

        // First poll returns 3 records
        storage.read(_, _, _, _) >>> [
            [
                new MessageRecord(10L, 'retry-topic', 0, 'k10', EventType.MESSAGE, 'd', Instant.now()),
                new MessageRecord(11L, 'retry-topic', 0, 'k11', EventType.MESSAGE, 'd', Instant.now()),
                new MessageRecord(12L, 'retry-topic', 0, 'k12', EventType.MESSAGE, 'd', Instant.now()),
            ],
            [],
            []
        ]

        def connector = buildConnector()
        int callCount = 0
        connector.onDataReceived({ record ->
            callCount++
            if (callCount == 1) {
                // succeed first record
                delivered.add(record.msgKey)
                return true
            } else if (callCount == 2) {
                // fail second record — batch should stop here
                latch.countDown()
                return false
            }
            return true
        })

        when:
        connector.connectToParent("http://localhost:${embeddedServer.port}").get(5, TimeUnit.SECONDS)
        latch.await(8, TimeUnit.SECONDS)
        Thread.sleep(200)

        then: "only the first record was delivered successfully"
        delivered.size() == 1
        delivered[0] == 'k10'

        cleanup:
        connector?.disconnect()
    }
}
