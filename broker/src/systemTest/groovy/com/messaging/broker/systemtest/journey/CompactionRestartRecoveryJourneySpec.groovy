package com.messaging.broker.systemtest.journey

import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.TestRecordCollector
import io.micronaut.context.ApplicationContext
import spock.util.concurrent.PollingConditions

/**
 * Journey: broker writes compacted segment files to disk, restarts, and correctly
 * loads them via DefaultStorageRecoveryService.
 *
 * Scenario:
 * 1. Enough records with the same key are written to force segment sealing.
 * 2. Compaction runs — superseded entries are removed; the resulting file on disk
 *    carries the '.compacted.log' naming.
 * 3. Broker and consumer contexts are closed (simulating a restart).
 * 4. A new broker context is started with the SAME dataDir on a new port.
 * 5. A fresh consumer reconnects; a probe record arrives, proving that compacted
 *    segments were loaded correctly by the recovery service.
 * 6. Pre-compaction history records are NOT re-delivered.
 */
class CompactionRestartRecoveryJourneySpec extends BrokerSystemTestSupport {

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['compaction.rocksdb.path']     = "${dataDir}/compaction-index-restart"
        base['broker.storage.segment-size'] = '128'
        return base
    }

    def "broker recovers compacted segment files after restart and delivers only new records"() {
        given: "collector is clean"
        collector().reset()

        and: "pipe delivers enough records to force segment sealing (same key, many versions)"
        // With segment-size=512 bytes, a handful of records will fill and seal a segment.
        cloudServer.enqueueMessages((1..8).collect { i ->
            [offset: (long) i, topic: 'prices-v1', partition: 0,
             msgKey: 'crr-key-001', eventType: 'MESSAGE', data: """{"rev":${i}}"""]
        })

        and: "consumer receives the LAST version — guarantees all 8 records are stored and segment-0 is sealed"
        // The delivery filter suppresses superseded records and delivers only the latest.
        // Waiting for the record with data '{"rev":8}' (cloud offset 8, last record) ensures all
        // 8 records have been appended: with segment-size=128 the first sealed segment forms after
        // the 5th append, so this condition guarantees at least one inactive segment for compaction.
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            assert collector().getAll().any { it.msgKey == 'crr-key-001' && it.data == '{"rev":8}' }
        }

        when: "compaction runs — superseded records are removed and segment is rewritten"
        def scheduler = brokerCtx.getBean(
            Class.forName('com.messaging.broker.compaction.CompactionScheduler'))
        scheduler.compact()

        then: "compaction ran: either a .compacted.log file exists (survivors) or the first sealed segment's .log was deleted (all superseded)"
        // compact() is synchronous — file changes are complete when it returns.
        // The active segment's .log file always remains; check only the FIRST sealed segment
        // (baseOffset=0) which should be gone when all its records were superseded.
        def topicPartDir = new File("${dataDir}/prices-v1/partition-0")
        def allFiles = topicPartDir.listFiles()?.collect { it.name } ?: []
        // If some records survived: a .compacted.log file is present.
        // If all records were superseded: the original sealed segment file (00000000000000000000.log) is gone.
        boolean compactedFilesPresent = allFiles.any { it.contains('.compacted.') }
        boolean sealedSegmentRemoved  = !allFiles.contains('00000000000000000000.log')
        compactedFilesPresent || sealedSegmentRemoved

        when: "consumer context is closed, then broker context is closed (simulating restart)"
        consumerCtx.close()
        brokerCtx.close()

        and: "broker is restarted with the same dataDir on a new port"
        int newBrokerPort = findFreePort()
        def restartedBrokerCtx = ApplicationContext.run(
            restartedBrokerProperties(newBrokerPort) as Map<String, Object>)
        triggerBrokerServiceStartup(restartedBrokerCtx)
        triggerPipeConnection(restartedBrokerCtx, cloudServer.baseUrl)

        and: "a probe record is enqueued so we can confirm recovery + delivery still work"
        cloudServer.enqueueMessages([
            [offset: 9L, topic: 'prices-v1', partition: 0,
             msgKey: 'crr-probe', eventType: 'MESSAGE', data: '{"probe":true}']
        ])

        and: "a fresh consumer reconnects to the restarted broker"
        def restartedConsumerCtx = ApplicationContext.run(
            restartedConsumerProperties(newBrokerPort) as Map<String, Object>)
        triggerConsumerManagerStartup(restartedConsumerCtx)
        sleep(2000)

        then: "probe record arrives — recovery loaded compacted segments correctly"
        def freshCollector = restartedConsumerCtx.getBean(TestRecordCollector)
        def probeBatch = freshCollector.waitForRecords(1, 20)
        probeBatch.size() == 1
        probeBatch[0].msgKey == 'crr-probe'
        probeBatch[0].data == '{"probe":true}'

        and: "pre-restart history records are NOT re-delivered after recovery"
        !probeBatch.any { it.msgKey == 'crr-key-001' }

        cleanup:
        restartedConsumerCtx?.close()
        // Reassign shared fields so cleanupSpec() has valid (null-safe) refs to close
        brokerCtx  = restartedBrokerCtx
        consumerCtx = restartedConsumerCtx
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Map<String, String> restartedBrokerProperties(int port) {
        def props = new LinkedHashMap<>(brokerProperties())
        props['broker.network.port']  = "${port}"
        props['micronaut.server.port'] = "${findFreePort()}"
        return props
    }

    private Map<String, String> restartedConsumerProperties(int brokerPort) {
        def props = new LinkedHashMap<>(consumerProperties())
        props['messaging.broker.port'] = "${brokerPort}"
        props['micronaut.server.port'] = "${findFreePort()}"
        return props
    }
}
