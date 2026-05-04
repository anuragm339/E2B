package com.messaging.broker.systemtest.support

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

import java.time.Instant

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Minimal HTTP/1.1 server that stands in for both the cloud registry and the pipe.
 *
 * Serves:
 *   GET /registry/topology  ← polled by broker's CloudRegistryClient
 *   GET /pipe/poll           ← polled by broker's HttpPipeConnector
 *
 * Uses raw ServerSocket instead of com.sun.net.httpserver.HttpServer to avoid
 * incompatibilities between the JDK server and Netty's HTTP/1.1 client.
 * Each response includes Content-Length and Connection: close so Netty does
 * not attempt to reuse connections.
 */
class MockCloudServer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())

    final int port
    final String baseUrl

    private final ServerSocket serverSocket
    private final AtomicBoolean running = new AtomicBoolean(false)
    private final def executor = Executors.newCachedThreadPool({ r ->
        def t = new Thread(r, 'MockCloudServer-handler')
        t.daemon = true
        t
    })

    private final ConcurrentLinkedQueue<byte[]> pipeQueue = new ConcurrentLinkedQueue<>()
    private final AtomicBoolean paused = new AtomicBoolean(false)
    private final AtomicInteger topologyFetchCount = new AtomicInteger()
    private final AtomicInteger pollCount = new AtomicInteger()

    private MockCloudServer(ServerSocket ss, int port) {
        this.serverSocket = ss
        this.port = port
        this.baseUrl = "http://127.0.0.1:${port}"
    }

    static MockCloudServer create() {
        int p = findFreePort()
        def ss = new ServerSocket()
        ss.reuseAddress = true
        ss.bind(new InetSocketAddress('127.0.0.1', p))
        new MockCloudServer(ss, p)
    }

    void start() {
        running.set(true)
        def acceptThread = new Thread({
            while (running.get()) {
                try {
                    def client = serverSocket.accept()
                    executor.submit { handleClient(client) }
                } catch (SocketException ignored) {
                    // Server was stopped
                }
            }
        }, 'MockCloudServer-accept')
        acceptThread.daemon = true
        acceptThread.start()
    }

    void stop() {
        running.set(false)
        executor.shutdownNow()
        try { serverSocket.close() } catch (ignored) {}
    }

    /**
     * Queue records so the next /pipe/poll response returns them.
     *
     * Normalises each record before serialisation:
     *  - Converts Groovy CharSequence/GString values to plain java.lang.String so
     *    Jackson (which has no Groovy module) doesn't serialise them as bean objects.
     *  - Adds a default "createdAt" timestamp if the caller omitted it, because the
     *    storage engine calls record.getCreatedAt().toEpochMilli() and will NPE otherwise.
     */
    void enqueueMessages(List<Map<String, Object>> records) {
        def now = Instant.now().toString()
        def normalized = records.collect { Map<String, Object> record ->
            def m = record.collectEntries { k, v ->
                [k.toString(), v instanceof CharSequence ? v.toString() : v]
            } as Map<String, Object>
            if (!m.containsKey('createdAt') && !m.containsKey('created')) {
                m['createdAt'] = now
            }
            m
        }
        pipeQueue.add(MAPPER.writeValueAsBytes(normalized))
    }

    /** Pause /pipe/poll — responds 204 until resumed. */
    void pausePipe() { paused.set(true) }

    /** Resume /pipe/poll. */
    void resumePipe() { paused.set(false) }

    int getTopologyFetchCount() { topologyFetchCount.get() }
    int getPollCount() { pollCount.get() }

    void reset() {
        pipeQueue.clear()
        paused.set(false)
        topologyFetchCount.set(0)
        pollCount.set(0)
    }

    // ─────────────────────────────────────────────────────────────────────────

    private void handleClient(Socket client) {
        try {
            client.soTimeout = 5000
            def reader = new BufferedReader(new InputStreamReader(client.inputStream, 'UTF-8'))

            // Read request line
            def requestLine = reader.readLine()
            if (!requestLine) return

            // Read and discard headers (we only need the path)
            String hdr
            while ((hdr = reader.readLine()) != null && !hdr.isEmpty()) { /* skip */ }

            def parts = requestLine.split(' ')
            def path = parts.length > 1 ? parts[1] : '/'

            def out = new DataOutputStream(client.outputStream)

            if (path.startsWith('/registry/topology')) {
                serveTopology(out)
            } else if (path.startsWith('/pipe/poll')) {
                servePipePoll(out)
            } else {
                sendText(out, 404, 'Not Found')
            }

            out.flush()
        } catch (SocketTimeoutException ignored) {
            // Client didn't send a complete request in time — ignore
        } catch (Exception e) {
            // Ignore other per-connection errors
        } finally {
            try { client.close() } catch (ignored) {}
        }
    }

    private void serveTopology(DataOutputStream out) {
        topologyFetchCount.incrementAndGet()
        def body = MAPPER.writeValueAsBytes([
            nodeId         : 'mock-cloud-node',
            role           : 'L2',
            requestToFollow: [baseUrl],
            topologyVersion: 'system-test-1',
            topics         : []
        ])
        sendJson(out, 200, body)
    }

    private void servePipePoll(DataOutputStream out) {
        pollCount.incrementAndGet()

        if (paused.get()) {
            sendNoContent(out)
            return
        }

        def payload = pipeQueue.poll()
        if (payload == null) {
            sendNoContent(out)
            return
        }

        sendJson(out, 200, payload)
    }

    private static void sendJson(DataOutputStream out, int status, byte[] body) {
        def statusText = status == 200 ? 'OK' : "${status}"
        out.writeBytes("HTTP/1.1 ${status} ${statusText}\r\n")
        out.writeBytes("Content-Type: application/json\r\n")
        out.writeBytes("Content-Length: ${body.length}\r\n")
        out.writeBytes("Connection: close\r\n")
        out.writeBytes("\r\n")
        out.write(body)
    }

    private static void sendNoContent(DataOutputStream out) {
        out.writeBytes("HTTP/1.1 204 No Content\r\n")
        out.writeBytes("Content-Length: 0\r\n")
        out.writeBytes("Connection: close\r\n")
        out.writeBytes("\r\n")
    }

    private static void sendText(DataOutputStream out, int status, String text) {
        def body = text.bytes
        out.writeBytes("HTTP/1.1 ${status} ${text}\r\n")
        out.writeBytes("Content-Type: text/plain\r\n")
        out.writeBytes("Content-Length: ${body.length}\r\n")
        out.writeBytes("Connection: close\r\n")
        out.writeBytes("\r\n")
        out.write(body)
    }

    private static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
