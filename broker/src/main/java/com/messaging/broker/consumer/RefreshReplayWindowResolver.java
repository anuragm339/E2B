package com.messaging.broker.consumer;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.exception.DataRefreshException;
import com.messaging.common.exception.ErrorCode;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Resolves the configured refresh replay window to concrete storage offsets.
 */
@Singleton
public class RefreshReplayWindowResolver {
    private static final int READ_BATCH_SIZE = 500;

    private final StorageEngine storage;
    private final long replayWindowHours;
    // READY settle window: consumers must catch up to the last record OLDER than this (the settled
    // history) before READY. Records created within the window are still settling and are delivered
    // via the normal flow afterwards. 0 = require full catch-up to head.
    private final long readySettleWindowMs;

    @Inject
    public RefreshReplayWindowResolver(
            StorageEngine storage,
            @Value("${broker.refresh.replay.window-hours:0}") long replayWindowHours,
            @Value("${broker.refresh.ready-settle-window-ms:21600000}") long readySettleWindowMs) {
        this.storage = storage;
        this.replayWindowHours = replayWindowHours;
        this.readySettleWindowMs = readySettleWindowMs;
    }

    private RefreshReplayWindowResolver() {
        this.storage = null;
        this.replayWindowHours = 0;
        this.readySettleWindowMs = 0;
    }

    static RefreshReplayWindowResolver unbounded() {
        return new RefreshReplayWindowResolver();
    }

    /** Backward-compatible entry point — treats the refresh as non-LOCAL (settle window applies). */
    public RefreshReplayWindow resolve(String topic) {
        return resolve(topic, null);
    }

    /**
     * Resolve the replay start + READY target for a topic.
     *
     * <p>The settle window (created_time-based READY target) applies only to <b>non-LOCAL</b>
     * refreshes — those re-source data and may go green before the still-arriving recent tail is
     * delivered. A <b>LOCAL</b> refresh re-pushes existing local segments and must catch consumers
     * up to the latest record, so it always targets the head (settle window ignored).
     */
    public RefreshReplayWindow resolve(String topic, String refreshType) {
        if (storage == null) {
            return RefreshReplayWindow.unbounded();
        }

        long head = storage.getCurrentOffset(topic, 0);
        if (head < 0) {
            return new RefreshReplayWindow(0, -1, null);
        }

        long earliest = Math.max(0, storage.getEarliestOffset(topic, 0));

        // START: how far back to re-deliver (replay.window-hours). 0 = from the earliest record.
        long start = (replayWindowHours <= 0)
                ? earliest
                : findFirstOffsetAtOrAfter(topic, earliest, head,
                        Instant.now().minus(Duration.ofHours(replayWindowHours)));

        // TARGET (the READY gate): the last record whose created_time is OLDER than the settle
        // window — the "settled" history the consumer must catch up to. Records inside the window
        // are still settling and are NOT required for READY; they arrive via normal delivery after.
        boolean settleApplies = !"LOCAL".equals(refreshType);
        long target;
        Instant settleCutoff = null;
        if (!settleApplies || readySettleWindowMs <= 0) {
            target = head; // LOCAL refresh, or gate disabled → require full catch-up to the latest record
        } else {
            settleCutoff = Instant.now().minusMillis(readySettleWindowMs);
            long firstUnsettled = findFirstOffsetAtOrAfter(topic, earliest, head, settleCutoff);
            // firstUnsettled is the first record INSIDE the window. If every record is within the
            // window (firstUnsettled == earliest) nothing is settled → target -1 (nothing to wait
            // for, READY immediately). Otherwise the settled target is the record just before it.
            target = (firstUnsettled <= earliest) ? -1 : firstUnsettled - 1;
        }

        // Coherence: replay must cover the target so a just-wiped consumer can actually reach it.
        // (With replay.window-hours=0, start=earliest, so this never triggers.)
        if (target >= 0 && start > target) {
            start = earliest;
        }
        return new RefreshReplayWindow(start, target, settleCutoff);
    }

    /**
     * Re-evaluate the X-hour settled target against the CURRENT storage head — used during an async
     * (pipe-fed) bootstrap/refresh where the head grows as data streams in, so READY must track the
     * live settled point rather than a stale snapshot taken when storage was empty.
     *
     * <p>Cheap in the common case: if the head record is already older than the settle window
     * (a bootstrap re-sourcing old history), the whole topic is settled → return the head with a
     * single read. The expensive forward scan only runs when the head is recent (an actively-updating
     * topic with a within-window tail). Mirrors the target rule in {@link #resolve}.
     *
     * @return last offset whose created_time is older than the settle window; {@code head} when the
     *         gate is disabled; {@code -1} for an empty topic or when every record is within the window.
     */
    public long settledTarget(String topic) {
        if (storage == null) {
            return -1;
        }
        long head = storage.getCurrentOffset(topic, 0);
        if (head < 0) {
            return -1;
        }
        if (readySettleWindowMs <= 0) {
            return head; // gate disabled → full catch-up to head
        }
        Instant cutoff = Instant.now().minusMillis(readySettleWindowMs);
        Instant headTime = createdAtOf(topic, head);
        if (headTime == null || !headTime.isAfter(cutoff)) {
            return head; // head already settled → everything is settled (cheap path)
        }
        long earliest = Math.max(0, storage.getEarliestOffset(topic, 0));
        long firstUnsettled = findFirstOffsetAtOrAfter(topic, earliest, head, cutoff);
        return (firstUnsettled <= earliest) ? -1 : firstUnsettled - 1;
    }

    private Instant createdAtOf(String topic, long offset) {
        List<MessageRecord> recs = storage.read(topic, 0, offset, 1);
        return (recs == null || recs.isEmpty()) ? null : recs.get(0).getCreatedAt();
    }

    private long findFirstOffsetAtOrAfter(String topic, long earliest, long head, Instant cutoff) {
        long offset = earliest;
        while (offset <= head) {
            List<MessageRecord> records = storage.read(topic, 0, offset, READ_BATCH_SIZE);
            if (records == null || records.isEmpty()) {
                break;
            }

            for (MessageRecord record : records) {
                Instant createdAt = record.getCreatedAt();
                if (createdAt == null || !createdAt.isBefore(cutoff)) {
                    return record.getOffset();
                }
            }

            long lastOffset = records.get(records.size() - 1).getOffset();
            if (lastOffset < offset) {
                throw new DataRefreshException(ErrorCode.DATA_REFRESH_REPLAY_FAILED,
                        "Storage replay-window scan did not advance")
                        .withTopic(topic)
                        .withContext("offset", offset);
            }
            offset = lastOffset + 1;
        }
        return head + 1;
    }

    public record RefreshReplayWindow(long startOffset, long targetOffset, Instant cutoff) {
        static RefreshReplayWindow unbounded() {
            return new RefreshReplayWindow(0, Long.MIN_VALUE, null);
        }
    }
}
