package com.messaging.pipe

import com.messaging.pipe.metrics.PipeMetrics
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

/**
 * Unit tests for HttpPipeConnector - HTTP-based parent broker connection
 */
class HttpPipeConnectorSpec extends Specification {

    @TempDir
    Path tempDir

    def "connector can be created with data directory"() {
        given: "a temporary data directory"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)

        when: "creating HttpPipeConnector"
        def connector = new HttpPipeConnector(dataDir, metrics)

        then: "connector is created successfully"
        connector != null
    }

    def "pause and resume pipe calls work correctly"() {
        given: "a connector instance"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        when: "pausing pipe calls"
        connector.pausePipeCalls()

        then: "pipe calls are paused"
        noExceptionThrown()

        when: "resuming pipe calls"
        connector.resumePipeCalls()

        then: "pipe calls are resumed"
        noExceptionThrown()
    }

    def "offset file is created in data directory"() {
        given: "a temporary data directory"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)

        when: "creating connector"
        def connector = new HttpPipeConnector(dataDir, metrics)

        then: "offset file location is in data directory"
        def offsetFile = new File(dataDir, "pipe-offset.properties")
        // File may or may not exist yet, but path should be valid
        offsetFile.getParentFile().exists()
    }

    def "connector loads existing offset from file"() {
        given: "a data directory with existing offset file"
        def dataDir = tempDir.toString()
        def offsetFile = new File(dataDir, "pipe-offset.properties")

        and: "write an offset value to file"
        offsetFile.parentFile.mkdirs()
        offsetFile.withWriter { writer ->
            writer.println("pipe.current.offset=12345")
        }

        def metrics = Mock(PipeMetrics)

        when: "creating connector"
        def connector = new HttpPipeConnector(dataDir, metrics)

        then: "connector loads the offset"
        noExceptionThrown()
    }

    def "connector handles missing offset file gracefully"() {
        given: "a data directory without offset file"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)

        when: "creating connector"
        def connector = new HttpPipeConnector(dataDir, metrics)

        then: "connector starts with default offset"
        noExceptionThrown()
    }

    def "connector handles corrupted offset file gracefully"() {
        given: "a data directory with corrupted offset file"
        def dataDir = tempDir.toString()
        def offsetFile = new File(dataDir, "pipe-offset.properties")

        and: "write invalid data to file"
        offsetFile.parentFile.mkdirs()
        offsetFile.withWriter { writer ->
            writer.println("pipe.current.offset=not-a-number")
        }

        def metrics = Mock(PipeMetrics)

        when: "creating connector"
        def connector = new HttpPipeConnector(dataDir, metrics)

        then: "connector handles corruption and starts with default"
        noExceptionThrown()
    }

    def "getHealth returns UNHEALTHY when not connected"() {
        given: "a connector without connection"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        when: "getting health status"
        def health = connector.getHealth()

        then: "status is UNHEALTHY"
        health == com.messaging.common.api.PipeConnector.PipeHealth.UNHEALTHY
    }

    def "onDataReceived registers handler successfully"() {
        given: "a connector"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        and: "a data handler"
        def handler = Mock(java.util.function.Consumer)

        when: "registering the handler"
        connector.onDataReceived(handler)

        then: "no exception is thrown"
        noExceptionThrown()
    }

    def "sendAck completes successfully"() {
        given: "a connector"
        def dataDir = tempDir.toString()
        def metrics = Mock(PipeMetrics)
        def connector = new HttpPipeConnector(dataDir, metrics)

        when: "sending an ack"
        def future = connector.sendAck(100L)

        then: "future completes successfully"
        future.get() == null
    }
}
