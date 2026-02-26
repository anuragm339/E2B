package com.messaging.pipe

import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import com.messaging.pipe.metrics.PipeMetrics
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Integration tests for HttpPipeConnector - tests HTTP polling and data flow
 */
class HttpPipeConnectorIntegrationSpec extends Specification {

    @TempDir
    Path tempDir

    HttpServer mockParentServer
    int serverPort
    List<MessageRecord> receivedMessages
    CountDownLatch messageLatch

    def setup() {
        // Start mock HTTP server on random port
        mockParentServer = HttpServer.create(new InetSocketAddress(0), 0)
        serverPort = mockParentServer.getAddress().getPort()
        receivedMessages = new CopyOnWriteArrayList<>()
    }

    def cleanup() {
        mockParentServer?.stop(0)
    }

    def "integration test: pipe connects to parent and receives messages"() {
        given: "mock parent server returns test messages once"
        messageLatch = new CountDownLatch(3)
        def pollCount = 0

        mockParentServer.createContext("/pipe/poll", new HttpHandler() {
            @Override
            void handle(HttpExchange exchange) throws IOException {
                pollCount++

                // Only return data on first poll
                if (pollCount == 1) {
                    String response = """[
                        {"offset":1,"msgKey":"key1","data":"data1","eventType":"MESSAGE","createdAt":"${Instant.now()}"},
                        {"offset":2,"msgKey":"key2","data":"data2","eventType":"MESSAGE","createdAt":"${Instant.now()}"},
                        {"offset":3,"msgKey":"key3","data":"data3","eventType":"MESSAGE","createdAt":"${Instant.now()}"}
                    ]"""

                    exchange.sendResponseHeaders(200, response.bytes.length)
                    exchange.responseBody.write(response.bytes)
                    exchange.responseBody.close()
                } else {
                    // Return 204 No Content for subsequent polls
                    exchange.sendResponseHeaders(204, -1)
                    exchange.responseBody.close()
                }
            }
        })
        mockParentServer.start()

        and: "create pipe connector"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        and: "register handler to collect messages"
        connector.onDataReceived({ record ->
            receivedMessages.add(record)
            messageLatch.countDown()
        })

        when: "connecting to mock parent"
        def connection = connector.connectToParent("http://localhost:${serverPort}").get(5, TimeUnit.SECONDS)

        and: "wait for messages to be received"
        boolean receivedAll = messageLatch.await(10, TimeUnit.SECONDS)

        then: "connection is established"
        connection != null
        connection.isConnected()

        and: "messages are received"
        receivedAll
        receivedMessages.size() == 3
        receivedMessages[0].getMsgKey() == "key1"
        receivedMessages[1].getMsgKey() == "key2"
        receivedMessages[2].getMsgKey() == "key3"

        cleanup:
        connector?.disconnect()
    }

    def "integration test: pipe handles empty response from parent"() {
        given: "mock parent returns 204 No Content"
        mockParentServer.createContext("/pipe/poll", new HttpHandler() {
            @Override
            void handle(HttpExchange exchange) throws IOException {
                exchange.sendResponseHeaders(204, -1)
                exchange.responseBody.close()
            }
        })
        mockParentServer.start()

        and: "create pipe connector"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        and: "register handler"
        connector.onDataReceived({ record ->
            receivedMessages.add(record)
        })

        when: "connecting to mock parent"
        def connection = connector.connectToParent("http://localhost:${serverPort}").get(5, TimeUnit.SECONDS)

        and: "wait a bit to ensure polling happens"
        Thread.sleep(1000)

        then: "connection is established"
        connection != null

        and: "no messages received (empty response is normal)"
        receivedMessages.size() == 0

        cleanup:
        connector?.disconnect()
    }

    def "integration test: pipe handles offset tracking correctly"() {
        given: "mock parent returns messages once then empty"
        messageLatch = new CountDownLatch(2)
        def pollCount = 0

        mockParentServer.createContext("/pipe/poll", new HttpHandler() {
            @Override
            void handle(HttpExchange exchange) throws IOException {
                pollCount++

                // Only return data on first poll
                if (pollCount == 1) {
                    String response = """[
                        {"offset":100,"msgKey":"key100","data":"data100","eventType":"MESSAGE","createdAt":"${Instant.now()}"},
                        {"offset":101,"msgKey":"key101","data":"data101","eventType":"MESSAGE","createdAt":"${Instant.now()}"}
                    ]"""

                    exchange.sendResponseHeaders(200, response.bytes.length)
                    exchange.responseBody.write(response.bytes)
                    exchange.responseBody.close()
                } else {
                    // Return 204 No Content for subsequent polls
                    exchange.sendResponseHeaders(204, -1)
                    exchange.responseBody.close()
                }
            }
        })
        mockParentServer.start()

        and: "create pipe connector"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        and: "register handler"
        connector.onDataReceived({ record ->
            receivedMessages.add(record)
            messageLatch.countDown()
        })

        when: "connecting and receiving messages"
        def connection = connector.connectToParent("http://localhost:${serverPort}").get(5, TimeUnit.SECONDS)
        boolean receivedAll = messageLatch.await(10, TimeUnit.SECONDS)

        then: "messages received"
        receivedAll
        receivedMessages.size() == 2

        and: "last received offset is tracked"
        connection.getLastReceivedOffset() == 101

        cleanup:
        connector?.disconnect()
    }

    def "integration test: pipe handles server errors gracefully"() {
        given: "mock parent returns 500 error"
        mockParentServer.createContext("/pipe/poll", new HttpHandler() {
            @Override
            void handle(HttpExchange exchange) throws IOException {
                exchange.sendResponseHeaders(500, -1)
                exchange.responseBody.close()
            }
        })
        mockParentServer.start()

        and: "create pipe connector"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        connector.onDataReceived({ record ->
            receivedMessages.add(record)
        })

        when: "connecting to mock parent"
        def connection = connector.connectToParent("http://localhost:${serverPort}").get(5, TimeUnit.SECONDS)

        and: "wait for polling attempts"
        Thread.sleep(1000)

        then: "connection is established (errors don't break connection)"
        connection != null

        and: "no messages received due to server errors"
        receivedMessages.size() == 0

        cleanup:
        connector?.disconnect()
    }
}
