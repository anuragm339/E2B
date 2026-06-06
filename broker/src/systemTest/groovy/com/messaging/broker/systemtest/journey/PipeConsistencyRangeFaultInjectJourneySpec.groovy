package com.messaging.broker.systemtest.journey

import com.messaging.broker.consistency.BrokerPipeConsistencyController
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.common.api.StorageEngine
import spock.util.concurrent.PollingConditions

/**
 * Journey: end-to-end Fix 2 verification.
 *
 * Verifies that {@code /pipe/consistency/range} on the broker correctly serves
 * records that arrived via the real pipe poll path, with cursor pagination semantics:
 *
 *  1. Cloud emits a batch → broker ingests via pipe → broker storage persists.
 *  2. Walking the broker's range controller by cursor recovers exactly the
 *     ingested set, ordered, no duplicates, no skips — even when more records
 *     exist than fit in a single page.
 *  3. Fault inject: a deliberately narrow {@code to} window correctly clamps
 *     the response — no records outside the window leak, no records inside
 *     the window are silently dropped.
 *
 * Exercises real beans (controller + StorageEngine + the real pipe poll → append
 * pathway) but invokes the controller directly rather than over HTTP — the wire
 * transport is covered by the integration spec
 * {@code BrokerPipeConsistencyRangeIntegrationSpec}; this journey focuses on
 * the storage-side correctness after a real pipe ingestion.
 */
class PipeConsistencyRangeFaultInjectJourneySpec extends BrokerSystemTestSupport {

    static final String TOPIC = 'prices-v1'

    def "broker exposes ingested pipe records via cursor-paged range with no dupes or skips"() {
        given: 'consumer collector is reset; consumer delivery is incidental here'
        collector().reset()

        and: 'cloud emits 25 records that flow through the pipe to broker storage'
        def emitted = (1L..25L).collect { i ->
            [offset: i, topic: TOPIC, partition: 0,
             msgKey: "k${i}".toString(), eventType: 'MESSAGE', data: "{\"i\":${i}}".toString()]
        } as List<Map<String, Object>>
        cloudServer.enqueueMessages(emitted)

        and: 'all 25 records land in broker storage'
        def storage = brokerCtx.getBean(StorageEngine)
        new PollingConditions(timeout: 15, delay: 0.2).eventually {
            assert storage.getCurrentOffset(TOPIC, 0) >= 25L
        }

        and: 'pull the controller bean — exercises real wiring (HashView, HashCache, etc.)'
        def controller = brokerCtx.getBean(BrokerPipeConsistencyController)

        when: 'walk the range by cursor, pageSize=7 (forces multiple pages)'
        def collected = []
        Long cursor = null
        for (int i = 0; i < 10; i++) {
            def resp = controller.range(TOPIC, 0L, 1_000_000L,
                    cursor == null ? Optional.empty() : Optional.of(cursor), 7)
            def body = resp.body.get() as Map
            def recs = body.records ?: []
            if (recs.isEmpty()) break
            collected.addAll(recs)
            if (body.nextCursor == null) break
            cursor = body.nextCursor as Long
        }

        then: 'collected exactly 25 ordered records with no dupes'
        collected.size() == 25
        collected*.msgKey == (1..25).collect { "k${it}".toString() }
        collected.collect { it.offset as Long }.toSet().size() == 25

        when: 'fault inject — request a narrow window simulating a downstream gap query'
        // Records at offsets 10..14 only.
        def narrow = controller.range(TOPIC, 0L, /*to*/ 14L, Optional.of(9L), 100)
        def narrowBody = narrow.body.get() as Map

        then: 'controller returns exactly the in-range slice (10..14) with no overflow'
        (narrowBody.records as List).size() == 5
        (narrowBody.records as List)*.offset as List<Long> == (10L..14L).toList()

        when: 'cursor beyond max emitted offset'
        def beyond = controller.range(TOPIC, 0L, 2_000_000L, Optional.of(1_000_000L), 100)
        def beyondBody = beyond.body.get() as Map

        then: 'no phantom records, no NPE'
        (beyondBody.records ?: []).isEmpty()
        beyondBody.nextCursor == null
    }
}
