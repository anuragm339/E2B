package com.messaging.broker.consumer

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

import java.nio.file.Files

@MicronautTest
class RefreshStateMachineIntegrationSpec extends Specification implements TestPropertyProvider {

    @Override
    Map<String, String> getProperties() {
        def dataDir = Files.createTempDirectory('broker-refresh-state-').toAbsolutePath().toString()
        [
            'broker.network.port'    : '19098',
            'micronaut.server.port'  : '18088',
            'broker.storage.data-dir': dataDir,
            'ack-store.rocksdb.path' : "${dataDir}/ack-store"
        ]
    }

    @Inject RefreshStateMachine stateMachine

    def "refresh state machine enforces forward terminal and abort transition rules"() {
        expect:
        stateMachine.isValidTransition(RefreshState.RESET_SENT, RefreshState.REPLAYING)
        stateMachine.isValidTransition(RefreshState.REPLAYING, RefreshState.READY_SENT)
        stateMachine.isValidTransition(RefreshState.READY_SENT, RefreshState.COMPLETED)
        stateMachine.isValidTransition(RefreshState.RESET_SENT, RefreshState.ABORTED)
        stateMachine.isValidTransition(RefreshState.REPLAYING, RefreshState.REPLAYING)
        !stateMachine.isValidTransition(RefreshState.REPLAYING, RefreshState.RESET_SENT)
        !stateMachine.isValidTransition(RefreshState.RESET_SENT, RefreshState.COMPLETED)
        stateMachine.isTerminalState(RefreshState.COMPLETED)
        stateMachine.isTerminalState(RefreshState.ABORTED)
        !stateMachine.isTerminalState(RefreshState.RESET_SENT)
        stateMachine.getNextState(RefreshState.RESET_SENT) == RefreshState.REPLAYING
        stateMachine.getNextState(RefreshState.REPLAYING) == RefreshState.READY_SENT
        stateMachine.getNextState(RefreshState.READY_SENT) == RefreshState.COMPLETED
        stateMachine.transition(RefreshState.RESET_SENT, RefreshState.REPLAYING).success
        !stateMachine.transition(RefreshState.COMPLETED, RefreshState.RESET_SENT).success
    }
}
