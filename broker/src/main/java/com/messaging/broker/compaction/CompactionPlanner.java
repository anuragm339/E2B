package com.messaging.broker.compaction;

import com.messaging.storage.segment.Segment;
import jakarta.inject.Singleton;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Selects the next window of dirty sealed segments for an incremental compaction run.
 *
 * <p>A segment is "dirty" (eligible for compaction) if its base offset is strictly greater
 * than the {@code lastCheckpoint} recorded in {@link CompactionCheckpointStore}.
 * The window is bounded by {@code windowSize} so each compaction run is bounded and
 * predictable, regardless of how many dirty segments have accumulated.
 */
@Singleton
public class CompactionPlanner {

    /**
     * Select the next batch of segments to compact.
     *
     * @param sealedSegments all inactive (sealed) segments for a topic-partition
     * @param lastCheckpoint base offset of the last compacted segment (-1 = no prior compaction)
     * @param windowSize     maximum number of segments to include in one run
     * @return segments to compact, sorted ascending by base offset, at most {@code windowSize} entries
     */
    public List<Segment> selectDirtyWindow(
            List<Segment> sealedSegments,
            long lastCheckpoint,
            int windowSize) {

        return sealedSegments.stream()
                .filter(s -> s.getBaseOffset() > lastCheckpoint)
                .sorted(Comparator.comparingLong(Segment::getBaseOffset))
                .limit(windowSize)
                .collect(Collectors.toList());
    }
}
