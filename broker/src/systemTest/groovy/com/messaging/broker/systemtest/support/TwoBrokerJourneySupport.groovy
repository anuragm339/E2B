package com.messaging.broker.systemtest.support

import com.messaging.broker.core.TopologyManager
import com.messaging.common.api.StorageEngine
import com.messaging.common.model.EventType
import com.messaging.common.model.MessageRecord
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

/**
 * Harness for two-broker download-refresh journeys: boots a PARENT and a CHILD broker as separate
 * in-process {@link ApplicationContext}s (own storage, ports, temp data dir), with consumers/legacy
 * and the schedulers disabled. The HTTP servers run, so the child can pull from the parent's
 * {@code /pipe/poll} / {@code /pipe/snapshot} over the wire and ingest into its OWN storage.
 *
 * A single @MicronautTest cannot model this — the bootstrap client ingests into the injected
 * StorageEngine, so parent and child must be distinct contexts.
 */
abstract class TwoBrokerJourneySupport extends Specification {

    @Shared EmbeddedServer parent
    @Shared EmbeddedServer child
    @Shared Path parentDir
    @Shared Path childDir

    def setupSpec() {
        parentDir = Files.createTempDirectory('two-broker-parent-')
        childDir = Files.createTempDirectory('two-broker-child-')
        parent = ApplicationContext.run(EmbeddedServer, brokerProps(parentDir))
        child = ApplicationContext.run(EmbeddedServer, brokerProps(childDir))
    }

    def cleanupSpec() {
        parent?.close()
        child?.close()
        parentDir?.toFile()?.deleteDir()
        childDir?.toFile()?.deleteDir()
    }

    protected String parentUrl() {
        parent.getURL().toString()
    }

    protected <T> T parentBean(Class<T> type) {
        parent.applicationContext.getBean(type)
    }

    protected <T> T childBean(Class<T> type) {
        child.applicationContext.getBean(type)
    }

    /** Point the child's topology at the parent so the orchestrator's source selection sees it. */
    protected void pointChildAtParent(String url) {
        def tm = childBean(TopologyManager)
        def f = TopologyManager.getDeclaredField('currentParentUrl')
        f.setAccessible(true)
        f.set(tm, url)
    }

    protected Map<String, Object> brokerProps(Path dir) {
        [
                'broker.network.port'              : "${findFreePort()}".toString(),
                'broker.network.type'              : 'tcp',
                'broker.registry.url'              : '',
                'broker.bootstrap.fresh-install.enabled' : 'false',
                'broker.storage.type'              : 'filechannel',
                'broker.storage.dataDir'           : dir.toString(),
                'broker.storage.data-dir'          : dir.toString(),
                'ack-store.rocksdb.path'           : "${dir}/ack-store".toString(),
                'broker.pipe.min-poll-interval-ms' : '999999999',
                'broker.pipe.max-poll-interval-ms' : '999999999',
                'broker.consumer.ack-timeout'                   : '30000',
                'broker.consumer.send-timeout-base-seconds'     : '1',
                'broker.consumer.send-timeout-per-mb-seconds'   : '2',
                'broker.consumer.adaptive-polling.min-delay-ms' : '50',
                'broker.consumer.adaptive-polling.max-delay-ms' : '500',
                'compaction.enabled'               : 'false',
                'compaction.max-process-cpu-usage' : '100.0',
                'compaction.max-heap-usage'        : '100.0',
                'data-refresh.enabled'             : 'false',
                'broker.snapshot.enabled'          : 'false',
                'consumer.legacy.enabled'          : 'false',
                'legacy-clients.enabled'           : 'false',
                'micronaut.server.port'            : "${findFreePort()}".toString(),
        ] as Map<String, Object>
    }

    protected static int findFreePort() {
        def s = new ServerSocket(0)
        try {
            return s.localPort
        } finally {
            s.close()
        }
    }

    protected static void append(StorageEngine storage, String topic, List<Long> offsets) {
        offsets.each { off ->
            storage.append(topic, 0,
                    new MessageRecord(off, topic, 0, "key-${off}".toString(), EventType.MESSAGE,
                            "{\"v\":${off}}".toString(), Instant.now()))
        }
    }

    protected static List<Long> offsetsOf(StorageEngine storage, String topic) {
        long earliest = storage.getEarliestOffset(topic, 0)
        if (earliest < 0) {
            return []
        }
        return storage.read(topic, 0, earliest, 1000).collect { it.offset }.sort()
    }
}
