package com.messaging.broker.consumer;

/**
 * State machine for the refresh workflow.
 *
 * Validates state transitions and enforces workflow rules.
 *
 * Valid state transitions:
 * - RESET_SENT → REPLAYING (on first RESET_ACK)
 * - REPLAYING → READY_SENT (when all consumers caught up)
 * - READY_SENT → COMPLETED (when all READY_ACKs received)
 * - Any state → ABORTED (on error)
 */
public interface RefreshWorkflow {
    /**
     * Validate if a state transition is allowed.
     *
     * @param from Current state
     * @param to Target state
     * @return true if transition is valid
     */
    boolean isValidTransition(RefreshState from, RefreshState to);

    /**
     * Get the next expected state.
     *
     * @param current Current state
     * @return Next expected state in the workflow
     */
    RefreshState getNextState(RefreshState current);

    /**
     * Check if a state is terminal (no further transitions expected).
     *
     * @param state State to check
     * @return true if state is terminal (COMPLETED or ABORTED)
     */
    boolean isTerminalState(RefreshState state);

    /**
     * Validate and transition to new state.
     *
     * @param from Current state
     * @param to Target state
     * @return Transition result
     * @throws IllegalStateException if transition is invalid
     */
    StateTransitionResult transition(RefreshState from, RefreshState to);

    /**
     * Result of a state transition attempt.
     */
    interface StateTransitionResult {
        boolean isSuccess();
        RefreshState getFromState();
        RefreshState getToState();
        String getReason();

        static StateTransitionResult success(RefreshState from, RefreshState to) {
            return new StateTransitionResult() {
                @Override
                public boolean isSuccess() { return true; }

                @Override
                public RefreshState getFromState() { return from; }

                @Override
                public RefreshState getToState() { return to; }

                @Override
                public String getReason() { return "Valid transition"; }
            };
        }

        static StateTransitionResult failure(RefreshState from, RefreshState to, String reason) {
            return new StateTransitionResult() {
                @Override
                public boolean isSuccess() { return false; }

                @Override
                public RefreshState getFromState() { return from; }

                @Override
                public RefreshState getToState() { return to; }

                @Override
                public String getReason() { return reason; }
            };
        }
    }
}
