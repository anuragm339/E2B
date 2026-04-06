package com.messaging.broker.config

import com.messaging.broker.handler.BatchAckHandler
import com.messaging.broker.handler.CommitOffsetHandler
import com.messaging.broker.handler.DataHandler
import com.messaging.broker.handler.MessageHandlerRegistry
import com.messaging.broker.handler.ReadyAckHandler
import com.messaging.broker.handler.ResetAckHandler
import com.messaging.broker.handler.SubscribeHandler
import com.messaging.common.model.BrokerMessage
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import jakarta.inject.Named
import spock.lang.Specification

import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Integration tests for the config package:
 *   - HandlerInitializer  — all 6 message handlers registered on startup
 *   - ExecutorFactory     — all 5 named executors created and functional
 *   - MessageHandlerRegistry — getHandler / hasHandler contract
 */
@MicronautTest
class ConfigIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-config-').toAbsolutePath().toString()
        return [
            'broker.network.port'    : '19100',
            'micronaut.server.port'  : '18090',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject MessageHandlerRegistry handlerRegistry

    @Inject @Named('ackExecutor')          ExecutorService          ackExecutor
    @Inject @Named('storageExecutor')      ExecutorService          storageExecutor
    @Inject @Named('consumerScheduler')    ScheduledExecutorService consumerScheduler
    @Inject @Named('dataRefreshScheduler') ScheduledExecutorService dataRefreshScheduler
    @Inject @Named('flushScheduler')       ScheduledExecutorService flushScheduler

    // =========================================================================
    // HandlerInitializer — all handlers registered on startup
    // =========================================================================

    def "HandlerInitializer registers DATA handler"() {
        expect:
        handlerRegistry.hasHandler(BrokerMessage.MessageType.DATA)
        handlerRegistry.getHandler(BrokerMessage.MessageType.DATA) instanceof DataHandler
    }

    def "HandlerInitializer registers SUBSCRIBE handler"() {
        expect:
        handlerRegistry.hasHandler(BrokerMessage.MessageType.SUBSCRIBE)
        handlerRegistry.getHandler(BrokerMessage.MessageType.SUBSCRIBE) instanceof SubscribeHandler
    }

    def "HandlerInitializer registers COMMIT_OFFSET handler"() {
        expect:
        handlerRegistry.hasHandler(BrokerMessage.MessageType.COMMIT_OFFSET)
        handlerRegistry.getHandler(BrokerMessage.MessageType.COMMIT_OFFSET) instanceof CommitOffsetHandler
    }

    def "HandlerInitializer registers RESET_ACK handler"() {
        expect:
        handlerRegistry.hasHandler(BrokerMessage.MessageType.RESET_ACK)
        handlerRegistry.getHandler(BrokerMessage.MessageType.RESET_ACK) instanceof ResetAckHandler
    }

    def "HandlerInitializer registers READY_ACK handler"() {
        expect:
        handlerRegistry.hasHandler(BrokerMessage.MessageType.READY_ACK)
        handlerRegistry.getHandler(BrokerMessage.MessageType.READY_ACK) instanceof ReadyAckHandler
    }

    def "HandlerInitializer registers BATCH_ACK handler"() {
        expect:
        handlerRegistry.hasHandler(BrokerMessage.MessageType.BATCH_ACK)
        handlerRegistry.getHandler(BrokerMessage.MessageType.BATCH_ACK) instanceof BatchAckHandler
    }

    def "MessageHandlerRegistry returns null for unregistered type"() {
        expect:
        !handlerRegistry.hasHandler(BrokerMessage.MessageType.HEARTBEAT)
        handlerRegistry.getHandler(BrokerMessage.MessageType.HEARTBEAT) == null
    }

    // =========================================================================
    // ExecutorFactory — executors are non-null, live, and accept tasks
    // =========================================================================

    def "ackExecutor is live and executes tasks"() {
        given:
        def latch = new CountDownLatch(1)

        when:
        ackExecutor.execute { latch.countDown() }

        then:
        latch.await(3, TimeUnit.SECONDS)
        !ackExecutor.shutdown
    }

    def "storageExecutor is live and executes tasks"() {
        given:
        def latch = new CountDownLatch(1)

        when:
        storageExecutor.execute { latch.countDown() }

        then:
        latch.await(3, TimeUnit.SECONDS)
        !storageExecutor.shutdown
    }

    def "consumerScheduler is live and schedules tasks"() {
        given:
        def latch = new CountDownLatch(1)

        when:
        consumerScheduler.schedule({ latch.countDown() }, 0, TimeUnit.MILLISECONDS)

        then:
        latch.await(3, TimeUnit.SECONDS)
        !consumerScheduler.shutdown
    }

    def "dataRefreshScheduler is live and schedules tasks"() {
        given:
        def latch = new CountDownLatch(1)

        when:
        dataRefreshScheduler.schedule({ latch.countDown() }, 0, TimeUnit.MILLISECONDS)

        then:
        latch.await(3, TimeUnit.SECONDS)
        !dataRefreshScheduler.shutdown
    }

    def "flushScheduler is live and schedules tasks"() {
        given:
        def latch = new CountDownLatch(1)

        when:
        flushScheduler.schedule({ latch.countDown() }, 0, TimeUnit.MILLISECONDS)

        then:
        latch.await(3, TimeUnit.SECONDS)
        !flushScheduler.shutdown
    }

    def "all five executors are distinct instances"() {
        expect:
        !ackExecutor.is(storageExecutor)
        !consumerScheduler.is(dataRefreshScheduler)
        !consumerScheduler.is(flushScheduler)
        !dataRefreshScheduler.is(flushScheduler)
    }
}
