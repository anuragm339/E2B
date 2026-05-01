package com.messaging.broker.systemtest.support

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

/**
 * Base class for all broker system (journey) tests.
 *
 * Starts — once per spec class — three components:
 *  1. MockCloudServer  (in-process JDK HttpServer)
 *  2. Broker ApplicationContext  (real Micronaut + NettyTcpServer)
 *  3. test-consumer ApplicationContext  (real Micronaut + ClientConsumerManager)
 *
 * Sub-classes configure topics/groups via overriding consumerProperties().
 */
abstract class BrokerSystemTestSupport extends Specification {

    @Shared MockCloudServer cloudServer
    @Shared ApplicationContext brokerCtx
    @Shared ApplicationContext consumerCtx
    @Shared int brokerTcpPort
    @Shared Path dataDir

    def setupSpec() {
        dataDir = Files.createTempDirectory('sys-test-')

        // ── 1. Mock cloud server ──────────────────────────────────────────────
        cloudServer = MockCloudServer.create()
        cloudServer.start()

        // ── 2. Broker context — pick a free TCP port before binding ───────────
        brokerTcpPort = findFreePort()
        brokerCtx = ApplicationContext.run(brokerProperties() as Map<String, Object>)

        // Micronaut 4.x caches event publishers at creation time; BrokerService may be
        // missed if the ServerStartupEvent publisher was created before its bean was resolved.
        // Manually trigger onApplicationEvent() to ensure the TCP server starts.
        triggerBrokerServiceStartup(brokerCtx)

        // The topology manager's registry fetch uses @Client("/") which does not reliably
        // resolve absolute URLs in programmatic ApplicationContext startup.  Directly wire
        // the pipe connector to the mock cloud server so pipe polling starts immediately.
        triggerPipeConnection(brokerCtx, cloudServer.baseUrl)

        // ── 3. Consumer context ───────────────────────────────────────────────
        consumerCtx = ApplicationContext.run(consumerProperties() as Map<String, Object>)

        // Micronaut 4.x caches event publishers at creation time; ClientConsumerManager may be
        // missed if the ServerStartupEvent publisher was created before its bean was resolved.
        // Manually trigger onApplicationEvent() to ensure the consumer connects.
        triggerConsumerManagerStartup(consumerCtx)

        // Give the consumer time to connect and subscribe
        sleep(3000)
    }

    def cleanupSpec() {
        consumerCtx?.close()
        brokerCtx?.close()
        cloudServer?.stop()
        dataDir?.toFile()?.deleteDir()
    }

    def setup() {
        cloudServer.reset()
    }

    // ─────────────────────────────────────────────────────────────────────────

    TestRecordCollector collector() {
        consumerCtx.getBean(TestRecordCollector)
    }

    // ─────────────────────────────────────────────────────────────────────────

    protected Map<String, String> brokerProperties() {
        [
            'broker.network.port'                    : "${brokerTcpPort}",
            'broker.network.type'                    : 'tcp',
            'broker.registry.url'                    : cloudServer.baseUrl,
            // Set BOTH forms so Micronaut relaxed binding resolves the correct temp dir
            // regardless of whether @Value uses camelCase or kebab-case notation.
            'broker.storage.type'                    : 'filechannel',
            'broker.storage.dataDir'                 : dataDir.toString(),
            'broker.storage.data-dir'                : dataDir.toString(),
            'ack-store.rocksdb.path'                 : "${dataDir}/ack-store",
            'broker.pipe.min-poll-interval-ms'       : '200',
            'broker.pipe.max-poll-interval-ms'       : '1000',
            'broker.pipe.poll-limit'                 : '20',
            'broker.consumer.ack-timeout'                          : '30000',
            'broker.consumer.send-timeout-base-seconds'            : '1',
            'broker.consumer.send-timeout-per-mb-seconds'          : '2',
            'broker.consumer.adaptive-polling.min-delay-ms'        : '50',
            'broker.consumer.adaptive-polling.max-delay-ms'        : '500',
            'micronaut.server.port'                  : "${findFreePort()}",
            'data-refresh.enabled'                   : 'false',
            // Suppress test-consumer beans (GenericConsumerHandler/@Consumer) in broker context
            'consumer.legacy.enabled'                : 'true',
            // Explicitly configure legacy-clients so the broker context has the right mapping
            // regardless of which application.yml is loaded first from the multi-module classpath.
            'legacy-clients.enabled'                                : 'true',
            'legacy-clients.service-topics.price-quote-service[0]' : 'prices-v1',
            'legacy-clients.service-topics.price-quote-service[1]' : 'reference-data-v5',
            'legacy-clients.service-topics.price-quote-service[2]' : 'non-promotable-products',
            'legacy-clients.service-topics.price-quote-service[3]' : 'prices-v4',
            'legacy-clients.service-topics.price-quote-service[4]' : 'minimum-price',
            'legacy-clients.service-topics.price-quote-service[5]' : 'deposit',
        ]
    }

    protected Map<String, String> consumerProperties() {
        [
            'messaging.broker.host'  : '127.0.0.1',
            'messaging.broker.port'  : "${brokerTcpPort}",
            'consumer.topics'        : defaultTopic(),
            'consumer.group'         : defaultGroup(),
            'consumer.legacy.enabled': 'false',
            'consumer.type'          : 'system-test',
            'micronaut.server.port'  : "${findFreePort()}",
            'broker.storage.dataDir' : "${dataDir}/consumer",
        ]
    }

    protected String defaultTopic() { 'prices-v1' }
    protected String defaultGroup() { 'system-test-group' }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Micronaut 4.x's ApplicationEventPublisherFactory caches publishers with a fixed
     * listener array at creation time.  In a programmatic ApplicationContext (not @MicronautTest),
     * BrokerService may not be in the cached listener array for ServerStartupEvent, so the
     * TCP server never starts.
     *
     * This helper explicitly invokes onApplicationEvent() so the broker initialises its TCP
     * server (and storage recovery, handler registration, etc.) regardless of event ordering.
     */
    protected void triggerBrokerServiceStartup(ApplicationContext ctx) {
        try {
            def brokerServiceClass = Class.forName("com.messaging.broker.core.BrokerService")
            def brokerService = ctx.getBean(brokerServiceClass)
            def embeddedServer = ctx.getBean(EmbeddedServer)
            def eventClass = Class.forName("io.micronaut.runtime.server.event.ServerStartupEvent")
            def ctor = eventClass.getDeclaredConstructors()[0]
            def event = ctor.newInstance(embeddedServer)
            brokerService.onApplicationEvent(event)
        } catch (Exception e) {
            throw new RuntimeException("Failed to trigger BrokerService startup: " + e.message, e)
        }
    }

    /**
     * Directly connects the broker's PipeConnector to parentUrl, bypassing the
     * CloudRegistryClient → TopologyManager path.
     *
     * The @Client("/") HttpClient injected into CloudRegistryClient / HttpPipeConnector is
     * bound to the embedded-server base URL and does not reliably follow absolute URIs in
     * a programmatic (non-@MicronautTest) ApplicationContext.  For system tests we therefore
     * skip the registry fetch and wire the pipe connector directly to the mock cloud server.
     *
     * Uses reflection to retrieve TopologyManager.messageHandler (the private Function that
     * BrokerService registered for inbound pipe messages) so the real production handler is
     * used — no stub or mock.
     */
    protected void triggerPipeConnection(ApplicationContext ctx, String parentUrl) {
        try {
            def topologyManagerClass = Class.forName("com.messaging.broker.core.TopologyManager")
            def topologyManager = ctx.getBean(topologyManagerClass)

            // Retrieve the private messageHandler field BrokerService registered on TopologyManager
            def messageHandlerField = topologyManagerClass.getDeclaredField("messageHandler")
            messageHandlerField.accessible = true
            def messageHandler = messageHandlerField.get(topologyManager)

            if (messageHandler == null) {
                throw new IllegalStateException("TopologyManager.messageHandler is null — was triggerBrokerServiceStartup called first?")
            }

            def pipeConnectorClass = Class.forName("com.messaging.common.api.PipeConnector")
            def pipeConnector = ctx.getBean(pipeConnectorClass)

            pipeConnector.connectToParent(parentUrl).get()
            pipeConnector.onDataReceived(messageHandler)
        } catch (Exception e) {
            throw new RuntimeException("Failed to trigger pipe connection to ${parentUrl}: " + e.message, e)
        }
    }

    /**
     * Micronaut 4.x's ApplicationEventPublisherFactory caches publishers with a fixed
     * listener array at creation time.  In a programmatic ApplicationContext (not @MicronautTest),
     * the publisher for ServerStartupEvent may be built before ClientConsumerManager's @Requires
     * conditions are fully resolved, resulting in an empty listener array.
     *
     * This helper explicitly invokes onApplicationEvent() so the manager discovers consumers
     * and connects to the broker regardless of event-publisher ordering.
     */
    protected void triggerConsumerManagerStartup(ApplicationContext ctx) {
        try {
            def mgrClass = Class.forName("com.messaging.client.ClientConsumerManager")
            def mgr = ctx.getBean(mgrClass)
            if (!mgr.isConnected()) {
                def embeddedServer = ctx.getBean(EmbeddedServer)
                def eventClass = Class.forName("io.micronaut.runtime.server.event.ServerStartupEvent")
                def ctor = eventClass.getDeclaredConstructors()[0]
                def event = ctor.newInstance(embeddedServer)
                mgr.onApplicationEvent(event)
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to trigger ClientConsumerManager startup: " + e.message, e)
        }
    }

    private static int findFreePort() {
        def s = new ServerSocket(0)
        try { s.localPort } finally { s.close() }
    }
}
