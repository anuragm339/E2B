package com.messaging.broker.consumer

import spock.lang.Specification

class RefreshStateMachineSpec extends Specification {

    def stateMachine = new RefreshStateMachine()

    def "should allow valid transition from RESET_SENT to REPLAYING"() {
        when:
        def result = stateMachine.transition(RefreshState.RESET_SENT, RefreshState.REPLAYING)

        then:
        result.isSuccess()
        result.fromState == RefreshState.RESET_SENT
        result.toState == RefreshState.REPLAYING
    }

    def "should allow valid transition from REPLAYING to READY_SENT"() {
        when:
        def result = stateMachine.transition(RefreshState.REPLAYING, RefreshState.READY_SENT)

        then:
        result.isSuccess()
        result.fromState == RefreshState.REPLAYING
        result.toState == RefreshState.READY_SENT
    }

    def "should allow valid transition from READY_SENT to COMPLETED"() {
        when:
        def result = stateMachine.transition(RefreshState.READY_SENT, RefreshState.COMPLETED)

        then:
        result.isSuccess()
        result.fromState == RefreshState.READY_SENT
        result.toState == RefreshState.COMPLETED
    }

    def "should reject invalid transition from RESET_SENT to COMPLETED"() {
        when:
        def result = stateMachine.transition(RefreshState.RESET_SENT, RefreshState.COMPLETED)

        then:
        !result.isSuccess()
        result.reason.contains("Invalid transition")
    }

    def "should allow transition to ABORTED from any state"() {
        expect:
        stateMachine.transition(fromState, RefreshState.ABORTED).isSuccess()

        where:
        fromState << [RefreshState.RESET_SENT, RefreshState.REPLAYING, RefreshState.READY_SENT]
    }

    def "should allow idempotent transitions (same state)"() {
        expect:
        stateMachine.transition(state, state).isSuccess()

        where:
        state << RefreshState.values()
    }

    def "should identify terminal states correctly"() {
        expect:
        stateMachine.isTerminalState(state) == isTerminal

        where:
        state                           | isTerminal
        RefreshState.RESET_SENT     | false
        RefreshState.REPLAYING      | false
        RefreshState.READY_SENT     | false
        RefreshState.COMPLETED      | true
        RefreshState.ABORTED        | true
    }

    def "should return correct next state"() {
        expect:
        stateMachine.getNextState(currentState) == expectedNext

        where:
        currentState                    | expectedNext
        RefreshState.RESET_SENT     | RefreshState.REPLAYING
        RefreshState.REPLAYING      | RefreshState.READY_SENT
        RefreshState.READY_SENT     | RefreshState.COMPLETED
        RefreshState.COMPLETED      | RefreshState.COMPLETED
        RefreshState.ABORTED        | RefreshState.ABORTED
    }

    def "should validate transition correctly"() {
        expect:
        stateMachine.isValidTransition(from, to) == isValid

        where:
        from                            | to                               | isValid
        RefreshState.RESET_SENT     | RefreshState.REPLAYING       | true
        RefreshState.RESET_SENT     | RefreshState.COMPLETED       | false
        RefreshState.REPLAYING      | RefreshState.READY_SENT      | true
        RefreshState.REPLAYING      | RefreshState.RESET_SENT      | false
        RefreshState.READY_SENT     | RefreshState.COMPLETED       | true
        RefreshState.READY_SENT     | RefreshState.REPLAYING       | false
        RefreshState.COMPLETED      | RefreshState.COMPLETED       | true
        RefreshState.COMPLETED      | RefreshState.RESET_SENT      | false
    }
}
