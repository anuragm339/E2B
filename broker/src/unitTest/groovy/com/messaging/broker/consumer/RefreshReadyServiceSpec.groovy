package com.messaging.broker.consumer

import com.messaging.broker.monitoring.DataRefreshMetrics
import com.messaging.broker.monitoring.RefreshEventLogger
import com.messaging.common.api.PipeConnector
import spock.lang.Specification

class RefreshReadyServiceSpec extends Specification {

    ConsumerRegistry remoteConsumers = Mock()
    PipeConnector pipeConnector = Mock()
    DataRefreshMetrics metrics = Mock()
    RefreshStateStore stateStore = Mock()
    RefreshEventLogger refreshLogger = Mock()
    Map<String, RefreshContext> activeRefreshes = [:]

    RefreshReadyService service = new RefreshReadyService(
            remoteConsumers, pipeConnector, metrics, stateStore, refreshLogger)

    def setup() {
        service.setSharedState(activeRefreshes, "batch-1")
    }

    def "sendReady marks state records metrics and persists"() {
        given:
        def context = context("prices-v1", ["group-a:prices-v1", "group-b:prices-v1"] as Set, "batch-1")
        context.getReceivedResetAcks().addAll(context.expectedConsumers)

        when:
        service.sendReady("prices-v1", context)

        then:
        context.state == RefreshState.READY_SENT
        context.readySentTime != null
        1 * remoteConsumers.sendReadyToAckedConsumers("prices-v1", context.receivedResetAcks)
        2 * metrics.recordReadySent("prices-v1", _ as String, "batch-1")
        1 * refreshLogger.logReadySent(_)
        1 * stateStore.saveState(context)
    }

    def "handleReadyAck validates state expected consumer and duplicates"() {
        given:
        def context = context("prices-v1", ["group-a:prices-v1"] as Set, "batch-1")

        expect:
        !service.handleReadyAck("group-a:prices-v1", "prices-v1", context, "trace-1")

        when:
        context.setState(RefreshState.READY_SENT)

        then:
        !service.handleReadyAck("group-b:prices-v1", "prices-v1", context, "trace-2")

        when:
        context.recordReadyAck("group-a:prices-v1")

        then:
        !service.handleReadyAck("group-a:prices-v1", "prices-v1", context, "trace-3")
    }

    def "handleReadyAck records ack and reports completion when all acks arrive"() {
        given:
        def context = context("prices-v1", ["group-a:prices-v1", "group-b:prices-v1"] as Set, "batch-1")
        context.setState(RefreshState.READY_SENT)
        context.getReceivedReadyAcks().add("group-a:prices-v1")

        when:
        def completed = service.handleReadyAck("group-b:prices-v1", "prices-v1", context, "trace-4")

        then:
        completed
        context.receivedReadyAcks.contains("group-b:prices-v1")
        1 * metrics.recordReadyAckReceived("prices-v1", "group-b:prices-v1", "batch-1")
        1 * refreshLogger.logReadyAckReceived(_)
        1 * stateStore.saveState(context)
    }

    def "checkReadyAckTimeout resends only when context is waiting for missing acks"() {
        given:
        def context = context("prices-v1", ["group-a:prices-v1"] as Set, "batch-1")
        context.setState(RefreshState.READY_SENT)

        when:
        service.checkReadyAckTimeout("prices-v1", null)
        service.checkReadyAckTimeout("prices-v1", context)

        then:
        1 * remoteConsumers.sendReadyToAckedConsumers("prices-v1", context.receivedResetAcks)
        1 * metrics.recordReadySent("prices-v1", "group-a:prices-v1", "batch-1")
        1 * refreshLogger.logReadySent(_)
        1 * stateStore.saveState(context)
    }

    def "completeRefresh resumes pipe only when the batch is fully complete"() {
        given:
        def first = context("prices-v1", ["group-a:prices-v1"] as Set, "batch-1")
        first.setResetSentTime(first.startTime)
        def sibling = context("orders-v1", ["group-b:orders-v1"] as Set, "batch-1")
        sibling.setState(RefreshState.READY_SENT)
        activeRefreshes.put("prices-v1", first)
        activeRefreshes.put("orders-v1", sibling)

        when:
        service.completeRefresh("prices-v1", first)

        then:
        first.state == RefreshState.COMPLETED
        1 * stateStore.saveState(first)
        1 * stateStore.clearState("prices-v1")
        1 * metrics.recordRefreshCompleted("prices-v1", "LOCAL", "SUCCESS", "batch-1", first)
        1 * refreshLogger.logRefreshCompleted(_)
        0 * pipeConnector.resumePipeCalls()
        0 * refreshLogger.logPipeResumed(_)

        when:
        sibling.setResetSentTime(sibling.startTime)
        service.completeRefresh("orders-v1", sibling)

        then:
        sibling.state == RefreshState.COMPLETED
        1 * pipeConnector.resumePipeCalls()
        1 * refreshLogger.logPipeResumed(_)
    }

    private static RefreshContext context(String topic, Set<String> consumers, String refreshId) {
        def context = new RefreshContext(topic, consumers)
        context.setRefreshId(refreshId)
        context
    }
}
