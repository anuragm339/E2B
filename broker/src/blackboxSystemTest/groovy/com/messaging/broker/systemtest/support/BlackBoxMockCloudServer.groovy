package com.messaging.broker.systemtest.support

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class BlackBoxMockCloudServer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())

    final int port
    final String baseUrl

    private final ServerSocket serverSocket
    private final AtomicBoolean running = new AtomicBoolean(false)
    private final def executor = Executors.newCachedThreadPool({ r ->
        def t = new Thread(r, 'BlackBoxMockCloudServer-handler')
        t.daemon = true
        t
    })
    private final ConcurrentLinkedQueue<byte[]> pipeQueue = new ConcurrentLinkedQueue<>()
    private final AtomicInteger pollCount = new AtomicInteger()

    private BlackBoxMockCloudServer(ServerSocket serverSocket, int port) {
        this.serverSocket = serverSocket
        this.port = port
        this.baseUrl = "http://127.0.0.1:${port}"
    }

    static BlackBoxMockCloudServer create() {
        int port = findFreePort()
        def serverSocket = new ServerSocket()
        serverSocket.reuseAddress = true
        serverSocket.bind(new InetSocketAddress('127.0.0.1', port))
        new BlackBoxMockCloudServer(serverSocket, port)
    }

    void start() {
        running.set(true)
        def acceptThread = new Thread({
            while (running.get()) {
                try {
                    def client = serverSocket.accept()
                    executor.submit { handleClient(client) }
                } catch (SocketException ignored) {
                }
            }
        }, 'BlackBoxMockCloudServer-accept')
        acceptThread.daemon = true
        acceptThread.start()
    }

    void stop() {
        running.set(false)
        executor.shutdownNow()
        try { serverSocket.close() } catch (ignored) {}
    }

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

    int getPollCount() {
        pollCount.get()
    }

    private void handleClient(Socket client) {
        try {
            client.soTimeout = 5000
            def reader = new BufferedReader(new InputStreamReader(client.inputStream, 'UTF-8'))
            def requestLine = reader.readLine()
            if (!requestLine) {
                return
            }
            String header
            while ((header = reader.readLine()) != null && !header.isEmpty()) {}

            def parts = requestLine.split(' ')
            def path = parts.length > 1 ? parts[1] : '/'
            def out = new DataOutputStream(client.outputStream)
            if (path.startsWith('/health')) {
                sendText(out, 200, 'OK')
            } else if (path.startsWith('/registry/topology')) {
                serveTopology(out)
            } else if (path.startsWith('/pipe/poll')) {
                servePipePoll(out)
            } else {
                sendText(out, 404, 'Not Found')
            }
            out.flush()
        } finally {
            try { client.close() } catch (ignored) {}
        }
    }

    private void serveTopology(DataOutputStream out) {
        def body = MAPPER.writeValueAsBytes([
            nodeId         : 'mock-cloud-node',
            role           : 'L2',
            requestToFollow: [baseUrl],
            topologyVersion: 'blackbox-system-test-1',
            topics         : []
        ])
        sendJson(out, 200, body)
    }

    private void servePipePoll(DataOutputStream out) {
        pollCount.incrementAndGet()
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
