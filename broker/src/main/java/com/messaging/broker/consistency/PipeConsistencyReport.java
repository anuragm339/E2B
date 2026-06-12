package com.messaging.broker.consistency;

import java.util.List;

/**
 * Outcome of one pipe-consistency check for a single topic. Detect-only: the broker never
 * acts on a report; {@code refreshRecommended} is a hint for operators / a future scheduler.
 *
 * <p>Serialized as-is (public fields) by Jackson for the admin report endpoint.
 */
public class PipeConsistencyReport {

    public enum State {
        /** Child holds everything the parent holds up to the child's watermark. */
        CONSISTENT,
        /** Consistent, but only verifiable up to the parent's (lower) head — post-reshuffle case. */
        CONSISTENT_UP_TO,
        /** Real divergence: missing, stale, or zombie keys (see counts). */
        INCONSISTENT,
        /**
         * Too many buckets diverged to drill down AND the watermark was clamped below this
         * node's head — the divergence may be entirely benign child-ahead keys (e.g. the dev
         * cloud's loopback replay). Cannot be decided within the configured budgets.
         */
        INCONCLUSIVE,
        /** Parent did not answer (offline / network). */
        UNREACHABLE,
        /** Parent answered 404 — older build without consistency endpoints, or feature disabled there. */
        UNSUPPORTED_PARENT,
        /** Local failure (index/storage error) — see error field. */
        ERROR
    }

    public String topic;
    public State state;
    public String target;             // parent/cloud URL the check ran against
    public long watermark;            // child head used for the comparison
    public long effectiveWatermark;   // == watermark unless clamped to a lagging parent's head
    public long keysScanned;          // child-side index entries visited

    // Real inconsistencies (drive state == INCONSISTENT)
    public int missingKeys;           // parent has key<=W (record physically present), child has nothing
    public int staleKeys;             // parent has key@o<=W (record present), child has older offset
    public int zombieKeys;            // parent's latest for key was compacted away (deleted upstream), child still holds a version

    // Benign / informational
    public int laggingKeys;           // child-extra keys whose parent latest is beyond W — pure lag
    public int extraKeys;             // child holds keys the parent has no entry for (lineage/reshuffle artifact)
    public int childNewerKeys;        // child's offset newer than parent's for same key (parent behind)

    /** Up to a few human-debuggable samples, e.g. "missing keyHash=... parentOffset=...". */
    public List<String> samples;

    public boolean refreshRecommended; // any real inconsistency cannot self-heal via pipe replay
    public long durationMs;
    public long checkedAtMs;
    public String error;

    public static PipeConsistencyReport of(String topic, State state, String target) {
        PipeConsistencyReport r = new PipeConsistencyReport();
        r.topic = topic;
        r.state = state;
        r.target = target;
        r.checkedAtMs = System.currentTimeMillis();
        return r;
    }

    public boolean isInconsistent() {
        return state == State.INCONSISTENT;
    }
}
