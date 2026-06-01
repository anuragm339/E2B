package com.messaging.broker.monitoring;

import com.messaging.common.api.StorageEngine;
import com.messaging.storage.segment.SegmentAccess;
import com.messaging.storage.segment.SegmentManager;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class StorageSegmentMetricsMonitor {
    private static final Logger log = LoggerFactory.getLogger(StorageSegmentMetricsMonitor.class);

    private final StorageEngine storageEngine;
    private final SegmentAccess segmentAccess;
    private final BrokerMetrics metrics;

    public StorageSegmentMetricsMonitor(StorageEngine storageEngine,
                                        SegmentAccess segmentAccess,
                                        BrokerMetrics metrics) {
        this.storageEngine = storageEngine;
        this.segmentAccess = segmentAccess;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelay = "15s", initialDelay = "15s")
    public void sample() {
        long totalStorageBytes = 0L;
        long totalActiveSegments = 0L;

        for (String topic : storageEngine.getTopicNames()) {
            SegmentManager manager = segmentAccess.getSegmentManager(topic, 0);
            if (manager == null) {
                continue;
            }

            long activeBytes = manager.getActiveSegmentSizeBytes();
            long sealedBytes = manager.getSealedSegmentBytes();
            long activeCount = manager.getActiveSegment() == null ? 0L : 1L;
            long sealedCount = manager.getSealedSegmentCount();
            long largestSegmentBytes = manager.getLargestSegmentBytes();

            totalStorageBytes += activeBytes + sealedBytes;
            totalActiveSegments += activeCount;

            metrics.updateTopicSegmentMetrics(
                    topic,
                    activeBytes,
                    sealedBytes,
                    activeCount,
                    sealedCount,
                    largestSegmentBytes
            );
        }

        metrics.updateStorageSize(totalStorageBytes);
        metrics.updateActiveSegments(totalActiveSegments);
        log.trace("Sampled storage segment metrics: topics={}, storageBytes={}, activeSegments={}",
                storageEngine.getTopicNames().size(), totalStorageBytes, totalActiveSegments);
    }
}
