package com.messaging.broker.consumer;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Enforces valid refresh state transitions and workflow rules.
 */
@Singleton
public class RefreshStateMachine implements RefreshWorkflow {
    private static final Logger log = LoggerFactory.getLogger(RefreshStateMachine.class);

    // Valid transitions map
    private final Map<RefreshState, Set<RefreshState>> validTransitions;

    public RefreshStateMachine() {
        this.validTransitions = buildTransitionMap();
    }

    @Override
    public boolean isValidTransition(RefreshState from, RefreshState to) {
        Set<RefreshState> allowedStates = validTransitions.get(from);
        return allowedStates != null && allowedStates.contains(to);
    }

    @Override
    public RefreshState getNextState(RefreshState current) {
        switch (current) {
            case RESET_SENT:
                return RefreshState.REPLAYING;
            case REPLAYING:
                return RefreshState.READY_SENT;
            case READY_SENT:
                return RefreshState.COMPLETED;
            case COMPLETED:
            case ABORTED:
                return current; // Terminal states
            default:
                log.warn("Unknown state: {}", current);
                return current;
        }
    }

    @Override
    public boolean isTerminalState(RefreshState state) {
        return state == RefreshState.COMPLETED || state == RefreshState.ABORTED;
    }

    @Override
    public StateTransitionResult transition(RefreshState from, RefreshState to) {
        if (from == to) {
            // Same state is always valid (idempotent)
            return StateTransitionResult.success(from, to);
        }

        if (!isValidTransition(from, to)) {
            String reason = String.format("Invalid transition from %s to %s", from, to);
            log.warn(reason);
            return StateTransitionResult.failure(from, to, reason);
        }

        log.debug("Valid transition: {} → {}", from, to);
        return StateTransitionResult.success(from, to);
    }

    /**
     * Build the valid state transition map.
     *
     * Valid transitions:
     * - RESET_SENT → REPLAYING, ABORTED
     * - REPLAYING → READY_SENT, ABORTED
     * - READY_SENT → COMPLETED, ABORTED
     * - COMPLETED → (terminal)
     * - ABORTED → (terminal)
     */
    private Map<RefreshState, Set<RefreshState>> buildTransitionMap() {
        Map<RefreshState, Set<RefreshState>> map = new EnumMap<>(RefreshState.class);

        // RESET_SENT can transition to REPLAYING or ABORTED
        map.put(RefreshState.RESET_SENT, EnumSet.of(
                RefreshState.RESET_SENT,  // Idempotent (can resend RESET)
                RefreshState.REPLAYING,
                RefreshState.ABORTED
        ));

        // REPLAYING can transition to READY_SENT or ABORTED
        map.put(RefreshState.REPLAYING, EnumSet.of(
                RefreshState.REPLAYING,  // Idempotent (can continue replay)
                RefreshState.READY_SENT,
                RefreshState.ABORTED
        ));

        // READY_SENT can transition to COMPLETED or ABORTED
        map.put(RefreshState.READY_SENT, EnumSet.of(
                RefreshState.READY_SENT,  // Idempotent (can resend READY)
                RefreshState.COMPLETED,
                RefreshState.ABORTED
        ));

        // COMPLETED is terminal (idempotent only)
        map.put(RefreshState.COMPLETED, EnumSet.of(
                RefreshState.COMPLETED
        ));

        // ABORTED is terminal (idempotent only)
        map.put(RefreshState.ABORTED, EnumSet.of(
                RefreshState.ABORTED
        ));

        return map;
    }
}
