package com.messaging.broker.support

import com.fasterxml.jackson.databind.ObjectMapper
import com.messaging.broker.core.PipeMessageForwarder
import com.messaging.common.api.StorageEngine
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration

abstract class BrokerHttpSpecSupport extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-http-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19098',
            'micronaut.server.port'  : '18088',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject EmbeddedServer embeddedServer
    @Inject StorageEngine storage
    @Inject PipeMessageForwarder pipeForwarder

    @Value('${broker.network.port}')
    int tcpPort

    HttpClient http
    ObjectMapper mapper
    String base

    def setup() {
        http = HttpClient.newHttpClient()
        mapper = new ObjectMapper()
        base = "http://127.0.0.1:${embeddedServer.port}"
    }

    protected HttpResponse<String> get(String path) {
        def req = HttpRequest.newBuilder()
            .uri(URI.create("${base}${path}"))
            .timeout(Duration.ofSeconds(5))
            .GET().build()
        http.send(req, HttpResponse.BodyHandlers.ofString())
    }

    protected HttpResponse<String> post(String path, Map body) {
        postWithTimeout(path, body, 5)
    }

    protected HttpResponse<String> postWithTimeout(String path, Map body, int timeoutSeconds) {
        def jsonBody = mapper.writeValueAsString(body)
        def req = HttpRequest.newBuilder()
            .uri(URI.create("${base}${path}"))
            .timeout(Duration.ofSeconds(timeoutSeconds))
            .header('Content-Type', 'application/json')
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody)).build()
        http.send(req, HttpResponse.BodyHandlers.ofString())
    }

    protected Object json(HttpResponse<String> resp) {
        mapper.readValue(resp.body(), Object)
    }
}
