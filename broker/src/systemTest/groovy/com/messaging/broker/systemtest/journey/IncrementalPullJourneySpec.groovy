package com.messaging.broker.systemtest.journey

import com.messaging.broker.snapshot.HttpBootstrapSourceClient
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
 * Two-broker journey for the incremental (no-snapshot) download-refresh path.
 *
 * Boots two real broker contexts in-process — a PARENT (serves /pipe/poll over HTTP from its own
 * storage) and a CHILD (separate storage + port). The child pulls ALL topics from the parent with
 * the real {@link HttpBootstrapSourceClient} over HTTP, ingesting into its OWN storage. Asserts the
 * k-way merge delivers every record across topics with disjoint offset ranges, and — the key
 * property — that re-pulling never duplicates.
 *
 * (A single @MicronautTest cannot test this: the client ingests into the injected StorageEngine, so
 * parent and child must be separate contexts.)
 */
class IncrementalPullJourneySpec extends Specification {

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

    private Map<String, Object> brokerProps(Path dir) {
        [
                'broker.network.port'              : "${findFreePort()}".toString(),
                'broker.network.type'              : 'tcp',
                'broker.registry.url'              : '',
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
                // No consumers needed for a pure parent->child pull; disable to avoid legacy
                // consumer startup (which needs a registry this isolated context does not have).
                'consumer.legacy.enabled'          : 'false',
                'legacy-clients.enabled'           : 'false',
                'micronaut.server.port'            : "${findFreePort()}".toString(),
        ] as Map<String, Object>
    }

    private static int findFreePort() {
        def s = new ServerSocket(0)
        try {
            return s.localPort
        } finally {
            s.close()
        }
    }

    private static void append(StorageEngine storage, String topic, List<Long> offsets) {
        offsets.each { off ->
            storage.append(topic, 0,
                    new MessageRecord(off, topic, 0, "key-${off}".toString(), EventType.MESSAGE,
                            "{\"v\":${off}}".toString(), Instant.now()))
        }
    }

    private static List<Long> offsetsOf(StorageEngine storage, String topic) {
        long earliest = storage.getEarliestOffset(topic, 0)
        if (earliest < 0) {
            return []
        }
        return storage.read(topic, 0, earliest, 1000).collect { it.offset }.sort()
    }

    def "child k-way-merge-pulls all topics from the parent over HTTP, with no duplicates"() {
        given: "parent storage holds records across two topics with disjoint offset ranges"
        def parentStorage = parent.applicationContext.getBean(StorageEngine)
        append(parentStorage, 'prices-v1', [10000L, 10002L, 11000L])
        append(parentStorage, 'reference-data-v5', [20000L, 21000L])

        when: "the child pulls everything from the parent over HTTP"
        def childClient = child.applicationContext.getBean(HttpBootstrapSourceClient)
        childClient.bulkFetchFromParent(parent.getURL().toString(), childDir.toString())

        then: "the child's storage now holds exactly the parent's records (k-way merge, all topics)"
        def childStorage = child.applicationContext.getBean(StorageEngine)
        offsetsOf(childStorage, 'prices-v1') == [10000L, 10002L, 11000L]
        offsetsOf(childStorage, 'reference-data-v5') == [20000L, 21000L]

        when: "a brand-new record arrives on the parent and the child pulls again"
        append(parentStorage, 'prices-v1', [12000L])
        childClient.bulkFetchFromParent(parent.getURL().toString(), childDir.toString())

        then: "only the new record is added — earlier records are NOT duplicated"
        offsetsOf(childStorage, 'prices-v1') == [10000L, 10002L, 11000L, 12000L]
        offsetsOf(childStorage, 'reference-data-v5') == [20000L, 21000L]
    }
}
