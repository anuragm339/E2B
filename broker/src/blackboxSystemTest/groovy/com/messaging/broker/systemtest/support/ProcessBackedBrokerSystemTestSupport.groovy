package com.messaging.broker.systemtest.support

import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties

abstract class ProcessBackedBrokerSystemTestSupport extends Specification {

    @Shared Path sandboxDir
    @Shared Path brokerDataDir
    @Shared Path consumerDataDir
    @Shared BlackBoxMockCloudServer cloudServer
    @Shared ManagedJavaProcess brokerProcess
    @Shared ManagedJavaProcess consumerProcess
    @Shared Path brokerConfigFile
    @Shared Path consumerConfigFile
    @Shared int brokerTcpPort
    @Shared int brokerHttpPort
    @Shared int consumerHttpPort

    def setupSpec() {
        sandboxDir = Files.createTempDirectory('blackbox-system-test-')
        brokerDataDir = sandboxDir.resolve('broker-data')
        consumerDataDir = sandboxDir.resolve('consumer-data')
        Files.createDirectories(brokerDataDir)
        Files.createDirectories(consumerDataDir)

        cloudServer = BlackBoxMockCloudServer.create()
        cloudServer.start()

        brokerTcpPort = findFreePort()
        brokerHttpPort = findFreePort()
        consumerHttpPort = findFreePort()
        brokerConfigFile = sandboxDir.resolve('broker-test.properties')
        consumerConfigFile = sandboxDir.resolve('consumer-test.properties')
        writePropertiesFile(brokerConfigFile, brokerConfig())
        writePropertiesFile(consumerConfigFile, consumerConfig())

        brokerProcess = ManagedJavaProcess.start(
            'broker',
            'com.messaging.broker.Application',
            sandboxDir.resolve('broker-work'),
            sandboxDir.resolve('logs/broker-stdout.log'),
            brokerEnv(),
            brokerProcessClasspath(),
            [
                "-Dmicronaut.config.files=${brokerConfigFile}",
                "-Dlogback.configurationFile=${brokerLogbackFile()}"
            ]
        )
        awaitHttpEndpoint("http://127.0.0.1:${brokerHttpPort}/health")
        awaitBrokerPortOpen()

        consumerProcess = ManagedJavaProcess.start(
            'consumer',
            'com.example.consumer.ConsumerApplication',
            sandboxDir.resolve('consumer-work'),
            sandboxDir.resolve('logs/consumer-stdout.log'),
            consumerEnv(),
            consumerProcessClasspath(),
            [
                "-Dmicronaut.config.files=${consumerConfigFile}",
                "-Dlogback.configurationFile=${consumerLogbackFile()}"
            ]
        )
        waitForLogContains(consumerProcess, 'Starting Consumer Application')
        awaitPortOpen(consumerProcess, consumerHttpPort)
    }

    def cleanupSpec() {
        consumerProcess?.stop()
        brokerProcess?.stop()
        cloudServer?.stop()
    }

    protected Map<String, String> brokerEnv() {
        [
            'DATA_DIR' : brokerDataDir.toString()
        ]
    }

    protected Map<String, String> consumerEnv() {
        [
            'CONSUMER_TYPE'   : defaultConsumerType(),
            'CONSUMER_TOPICS' : defaultTopic(),
            'CONSUMER_GROUP'  : defaultGroup(),
            'CONSUMER_PORT'   : "${consumerHttpPort}",
            'LEGACY_MODE'     : 'false',
            'BROKER_HOST'     : '127.0.0.1',
            'BROKER_PORT'     : "${brokerTcpPort}",
            'STORAGE_DATA_DIR': consumerDataDir.toString()
        ]
    }

    protected Map<String, String> brokerConfig() {
        [
            'micronaut.server.port'                 : "${brokerHttpPort}",
            'broker.network.type'                   : 'tcp',
            'broker.network.port'                   : "${brokerTcpPort}",
            'broker.registry.url'                   : cloudServer.baseUrl,
            'broker.storage.type'                   : 'filechannel',
            'broker.storage.dataDir'                : brokerDataDir.toString(),
            'ack-store.rocksdb.path'                : brokerDataDir.resolve('ack-store').toString(),
            'broker.pipe.min-poll-interval-ms'      : '100',
            'broker.pipe.max-poll-interval-ms'      : '500',
            'broker.pipe.poll-limit'                : '20',
            'ack-store.reconciliation.enabled'      : 'false',
            'ack-store.live-replay.enabled'         : 'false',
            'ack-store.seed-on-startup.enabled'     : 'false',
            'compaction.enabled'                    : 'false',
            'broker.consumer.max-message-size-per-consumer': '131072'
        ]
    }

    protected Map<String, String> consumerConfig() {
        [
            'micronaut.server.port'    : "${consumerHttpPort}",
            'consumer.type'            : defaultConsumerType(),
            'consumer.topics'          : defaultTopic(),
            'consumer.group'           : defaultGroup(),
            'consumer.legacy.enabled'  : 'false',
            'messaging.broker.host'    : '127.0.0.1',
            'messaging.broker.port'    : "${brokerTcpPort}",
            'broker.storage.data-dir'  : consumerDataDir.toString(),
            'storage.data-dir'         : consumerDataDir.toString()
        ]
    }

    protected String defaultTopic() { 'prices-v1' }
    protected String defaultGroup() { 'blackbox-group' }
    protected String defaultConsumerType() { 'blackbox' }

    protected void awaitPortOpen(ManagedJavaProcess proc, int port, int timeoutSecs = 30) {
        new PollingConditions(timeout: timeoutSecs, delay: 0.2).eventually {
            assert proc.isAlive(): "${proc.name} exited early. Log:\n${proc.readLog()}"
            try {
                def socket = new Socket('127.0.0.1', port)
                socket.close()
            } catch (IOException e) {
                assert false: "Port ${port} not open yet"
            }
        }
    }

    protected void awaitHttpEndpoint(String endpoint, int timeoutSecs = 30) {
        new PollingConditions(timeout: timeoutSecs, delay: 0.2).eventually {
            assert brokerProcess.isAlive(): "Broker exited early. Log:\n${brokerProcess.readLog()}"
            HttpURLConnection connection = (HttpURLConnection) new URL(endpoint).openConnection()
            connection.setConnectTimeout(1000)
            connection.setReadTimeout(1000)
            connection.setRequestMethod('GET')
            assert connection.responseCode < 500
        }
    }

    protected void awaitBrokerPortOpen(int timeoutSecs = 30) {
        awaitPortOpen(brokerProcess, brokerTcpPort, timeoutSecs)
    }

    protected void waitForLogContains(ManagedJavaProcess proc, String token, int timeoutSecs = 30) {
        new PollingConditions(timeout: timeoutSecs, delay: 0.2).eventually {
            assert proc.isAlive()
            assert proc.readLog().contains(token)
        }
    }

    protected void waitForFileProperty(Path file, String key, Closure<Boolean> matcher, int timeoutSecs = 30) {
        new PollingConditions(timeout: timeoutSecs, delay: 0.2).eventually {
            assert Files.exists(file)
            def props = new Properties()
            Files.newInputStream(file).withCloseable { props.load(it) }
            assert props.containsKey(key)
            assert matcher.call(props.getProperty(key))
        }
    }

    protected void waitForFileSize(Path file, Closure<Boolean> matcher, int timeoutSecs = 30) {
        new PollingConditions(timeout: timeoutSecs, delay: 0.2).eventually {
            assert Files.exists(file)
            assert matcher.call(Files.size(file))
        }
    }

    protected static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }

    protected static void writePropertiesFile(Path path, Map<String, String> values) {
        Files.createDirectories(path.parent)
        def props = new Properties()
        values.each { k, v -> props.setProperty(k, v) }
        Files.newOutputStream(path).withCloseable { props.store(it, null) }
    }

    protected static String brokerLogbackFile() {
        Path base = Path.of(System.getProperty('user.dir'))
        Path direct = base.resolve('src/main/resources/logback.xml')
        Files.exists(direct) ? direct.toString() : base.resolve('broker/src/main/resources/logback.xml').toString()
    }

    protected static String consumerLogbackFile() {
        Path base = Path.of(System.getProperty('user.dir'))
        Path sibling = base.resolve('../test-consumer/src/main/resources/logback.xml').normalize()
        Files.exists(sibling) ? sibling.toString() : base.resolve('test-consumer/src/main/resources/logback.xml').toString()
    }

    protected static String brokerProcessClasspath() {
        System.getProperty('blackbox.broker.classpath', System.getProperty('java.class.path'))
    }

    protected static String consumerProcessClasspath() {
        System.getProperty('blackbox.consumer.classpath', System.getProperty('java.class.path'))
    }
}
