package com.messaging.broker.monitoring

import com.messaging.broker.consumer.RefreshContext
import com.messaging.broker.consumer.RefreshCoordinator
import com.messaging.broker.consumer.RefreshState
import com.messaging.broker.snapshot.BootstrapProgressTracker
import io.micronaut.context.env.Environment
import io.micronaut.core.type.Argument
import io.micronaut.management.health.indicator.HealthResult
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicReference

class RefreshHealthIndicatorSpec extends Specification {

    RefreshCoordinator coordinator = Mock()
    BootstrapProgressTracker progress = new BootstrapProgressTracker() // IDLE by default
    Environment environment = Mock()

    def "without critical topics any active refresh makes health down"() {
        given:
        environment.getProperty("broker.refresh.health-critical-topics", _ as Argument) >> Optional.empty()
        def context = context("optional-topic", RefreshState.REPLAYING)
        coordinator.getCurrentRefreshContext() >> context
        def indicator = new RefreshHealthIndicator(coordinator, progress, environment)

        expect:
        result(indicator).status.name == "DOWN"
    }

    def "with critical topics non-critical active refresh does not make health down"() {
        given:
        environment.getProperty("broker.refresh.health-critical-topics", _ as Argument) >> Optional.of(["prices-v1"])
        coordinator.getActiveRefreshesSnapshot() >> [
                "optional-topic": context("optional-topic", RefreshState.REPLAYING)
        ]
        def indicator = new RefreshHealthIndicator(coordinator, progress, environment)

        expect:
        result(indicator).status.name == "UP"
    }

    def "with critical topics active critical refresh makes health down"() {
        given:
        environment.getProperty("broker.refresh.health-critical-topics", _ as Argument) >> Optional.of(["prices-v1"])
        coordinator.getActiveRefreshesSnapshot() >> [
                "prices-v1": context("prices-v1", RefreshState.READY_SENT)
        ]
        def indicator = new RefreshHealthIndicator(coordinator, progress, environment)

        expect:
        result(indicator).status.name == "DOWN"
    }

    def "an in-progress bootstrap re-source makes health down with no RefreshContext yet"() {
        given: "the destructive wipe window: bootstrap downloading, no active refresh context"
        environment.getProperty("broker.refresh.health-critical-topics", _ as Argument) >> Optional.empty()
        progress.start("download-refresh", BootstrapProgressTracker.Phase.DOWNLOADING)
        def indicator = new RefreshHealthIndicator(coordinator, progress, environment)

        expect:
        result(indicator).status.name == "DOWN"
    }

    def "an in-progress bootstrap re-source is node-wide DOWN even when health-critical-topics is set"() {
        given:
        environment.getProperty("broker.refresh.health-critical-topics", _ as Argument) >> Optional.of(["prices-v1"])
        progress.start("download-refresh", BootstrapProgressTracker.Phase.INGESTING)
        def indicator = new RefreshHealthIndicator(coordinator, progress, environment)

        expect:
        result(indicator).status.name == "DOWN"
    }

    def "a completed bootstrap re-source no longer forces health down"() {
        given:
        environment.getProperty("broker.refresh.health-critical-topics", _ as Argument) >> Optional.empty()
        progress.start("download-refresh", BootstrapProgressTracker.Phase.DOWNLOADING)
        progress.done()
        coordinator.getCurrentRefreshContext() >> null
        def indicator = new RefreshHealthIndicator(coordinator, progress, environment)

        expect:
        result(indicator).status.name == "UP"
    }

    private static RefreshContext context(String topic, RefreshState state) {
        def context = new RefreshContext(topic, ["group-a:${topic}"] as Set)
        context.setState(state)
        context
    }

    private static HealthResult result(RefreshHealthIndicator indicator) {
        def ref = new AtomicReference<HealthResult>()
        indicator.getResult().subscribe(new Subscriber<HealthResult>() {
            @Override
            void onSubscribe(Subscription s) {
                s.request(1)
            }

            @Override
            void onNext(HealthResult healthResult) {
                ref.set(healthResult)
            }

            @Override
            void onError(Throwable t) {
                throw new RuntimeException(t)
            }

            @Override
            void onComplete() {
            }
        })
        ref.get()
    }
}
