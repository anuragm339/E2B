package com.messaging.broker.systemtest.journey

import com.messaging.broker.consistency.PipeConsistencyReport
import com.messaging.broker.consistency.PipeConsistencyService
import com.messaging.broker.systemtest.support.BrokerSystemTestSupport
import com.messaging.broker.systemtest.support.MockCloudServer
import spock.util.concurrent.PollingConditions

/**
 * Journey: a reshuffle leaves this broker AHEAD of its parent (the parent's head is below
 * our watermark — only possible after re-parenting). The check must:
 *   1. report CONSISTENT_UP_TO + verificationPending on the first clamped check (no probes),
 *   2. on the second consecutive clamp, escalate to the registry-provided in-store verifier
 *      candidates: probe heads cheaply, skip a behind candidate, obtain the FULL verdict
 *      from the first candidate whose head covers the watermark,
 *   3. never involve the cloud.
 *
 * The real broker runs with its real index, scheduler-grade service, and HTTP client; the
 * stuck parent and the two verifier candidates are MockCloudServer stubs whose digest math
 * delegates to the production KeyspaceDigest.
 */
class PipeConsistencyEscalationJourneySpec extends BrokerSystemTestSupport {

    static final String TOPIC = 'pce-topic'

    @Override
    protected Map<String, String> brokerProperties() {
        def base = super.brokerProperties()
        base['pipe.consistency.enabled'] = 'true'
        base['pipe.consistency.escalation.after-clamped-checks'] = '2'
        base['pipe.consistency.schedule.enabled'] = 'false'   // journeys drive checks directly
        return base
    }

    def "persistent clamp escalates to an in-store verifier and yields a full verdict"() {
        given: 'two verifier candidate stubs: one behind the watermark, one fully caught up'
        def behindVerifier = MockCloudServer.create()
        behindVerifier.start()
        def goodVerifier = MockCloudServer.create()
        goodVerifier.start()

        and: 'the registry (mock cloud) advertises them as this store\'s verifier candidates'
        cloudServer.verifierCandidates = [behindVerifier.baseUrl, goodVerifier.baseUrl]

        and: 'the broker ingests 5 records through the pipe (offsets 1..5)'
        cloudServer.enqueueMessages((1..5).collect { i ->
            [offset: (long) i, topic: TOPIC, partition: 0,
             msgKey: "pce-key-$i", eventType: 'MESSAGE', data: "{\"v\":$i}"]
        })
        new PollingConditions(timeout: 20, delay: 0.3).eventually {
            def storage = brokerCtx.getBean(com.messaging.common.api.StorageEngine)
            assert storage.getCurrentOffset(TOPIC, 0) == 5L
        }

        and: 'the topology has refreshed so the candidates are cached on the broker (the registry poll runs every 30s — the first poll predates the candidate configuration)'
        def topologyManager = brokerCtx.getBean(com.messaging.broker.core.TopologyManager)
        new PollingConditions(timeout: 45, delay: 1).eventually {
            assert topologyManager.getVerifierCandidates().size() == 2
        }

        and: 'the PARENT (mock cloud) is stuck behind: head 3 < broker watermark 5'
        cloudServer.consistencyHead = 3L
        (1..3).each { cloudServer.consistencyEntries["pce-key-$it".toString()] = (long) it }

        and: 'the behind candidate has head 2; the good candidate matches the broker through 5'
        behindVerifier.consistencyHead = 2L
        goodVerifier.consistencyHead = 9L
        (1..5).each { goodVerifier.consistencyEntries["pce-key-$it".toString()] = (long) it }

        def service = brokerCtx.getBean(PipeConsistencyService)

        when: 'first check — parent clamps to 3'
        def first = service.runCheck(TOPIC, 'parent')[0]

        then: 'verified up to the clamp, tail pending — and NO candidate probes yet'
        first.state == PipeConsistencyReport.State.CONSISTENT_UP_TO
        first.watermark == 5L
        first.effectiveWatermark == 3L
        first.verificationPending
        behindVerifier.headProbeCount == 0
        goodVerifier.headProbeCount == 0

        when: 'second consecutive clamped check — escalation fires'
        def second = service.runCheck(TOPIC, 'parent')[0]

        then: 'the behind candidate was probed and skipped; the good one delivered a FULL verdict'
        behindVerifier.headProbeCount == 1
        behindVerifier.digestCount == 0          // never asked to scan — head probe filtered it
        goodVerifier.headProbeCount == 1
        goodVerifier.digestCount == 1

        second.state == PipeConsistencyReport.State.CONSISTENT
        second.watermark == 5L
        second.effectiveWatermark == 5L          // full watermark verified — no clamp
        second.escalatedFrom == cloudServer.baseUrl
        second.target == goodVerifier.baseUrl
        !second.verificationPending

        when: 'a later check against a caught-up parent goes back to the normal path'
        cloudServer.consistencyHead = 9L
        (1..5).each { cloudServer.consistencyEntries["pce-key-$it".toString()] = (long) it }
        def third = service.runCheck(TOPIC, 'parent')[0]

        then: 'no further probes — streak was reset by the escalated full verdict'
        third.state == PipeConsistencyReport.State.CONSISTENT
        third.escalatedFrom == null
        behindVerifier.headProbeCount == 1
        goodVerifier.headProbeCount == 1

        cleanup:
        behindVerifier?.stop()
        goodVerifier?.stop()
    }
}
