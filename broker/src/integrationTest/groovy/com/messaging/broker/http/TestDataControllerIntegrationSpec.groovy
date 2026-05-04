package com.messaging.broker.http

import com.messaging.broker.support.BrokerHttpSpecSupport
import io.micronaut.test.extensions.spock.annotation.MicronautTest

import java.nio.file.Files

@MicronautTest
class TestDataControllerIntegrationSpec extends BrokerHttpSpecSupport {

    def "POST inject messages stores records and defaults count when omitted"() {
        when:
        def explicit = post('/test/inject-messages', [count: 3, topic: 'inject-topic', prefix: 'INJ'])
        def implicit = post('/test/inject-messages', [topic: 'default-count-topic'])

        then:
        json(explicit).success == true
        json(explicit).messagesInjected == 3
        (json(explicit).offsets as List).size() == 3
        json(implicit).success == true
        json(implicit).messagesInjected == 10
    }

    def "POST load from sqlite validates input and loads real sqlite data"() {
        given:
        def dbFile = Files.createTempFile('test-messages-', '.db').toAbsolutePath().toString()
        def conn = java.sql.DriverManager.getConnection("jdbc:sqlite:${dbFile}")
        conn.createStatement().execute('CREATE TABLE messages (id TEXT, payload TEXT, value INTEGER)')
        def ps = conn.prepareStatement('INSERT INTO messages (id, payload, value) VALUES (?, ?, ?)')
        [['row1', 'data1', 100], ['row2', 'data2', 200]].each { row ->
            ps.setString(1, row[0]); ps.setString(2, row[1]); ps.setInt(3, row[2]); ps.executeUpdate()
        }
        conn.close()

        when:
        def missing = post('/test/load-from-sqlite', [tableName: 'messages', topic: 'lt-topic'])
        def absent = post('/test/load-from-sqlite', [sqliteFilePath: '/no/such/file.db', topic: 'lt-topic'])
        def loaded = postWithTimeout('/test/load-from-sqlite',
            [sqliteFilePath: dbFile, tableName: 'messages', topic: 'sqlite-topic'], 20)

        then:
        json(missing).success == false
        json(absent).success == false
        json(loaded).success == true
        (json(loaded).recordsLoaded as int) == 2
        storage.read('sqlite-topic', 0, 0L, 10).size() >= 1

        cleanup:
        new File(dbFile).delete()
    }

    def "GET test stats returns broker running status"() {
        expect:
        json(get('/test/stats')).broker == 'running'
    }
}
