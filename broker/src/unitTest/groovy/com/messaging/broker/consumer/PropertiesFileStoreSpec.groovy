package com.messaging.broker.consumer

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class PropertiesFileStoreSpec extends Specification {

    @TempDir
    Path tempDir

    def "flush persists a consistent snapshot that can be reloaded"() {
        given:
        def store = new PropertiesFileStore(tempDir.toString(), "state.properties", "test-state")
        store.put("offset", "42")
        store.put("inFlight", "true")

        when:
        store.flush()
        def reloaded = new PropertiesFileStore(tempDir.toString(), "state.properties", "test-state")

        then:
        reloaded.getAll() == [offset: "42", inFlight: "true"]
    }

    def "flush failure is propagated to the caller"() {
        given:
        def dataDir = tempDir.resolve("state")
        def store = new PropertiesFileStore(dataDir.toString(), "state.properties", "test-state")
        Files.delete(dataDir)

        when:
        store.flush()

        then:
        thrown(PropertiesStoreException)
    }
}
