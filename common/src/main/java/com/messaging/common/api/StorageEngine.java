package com.messaging.common.api;

import com.messaging.common.exception.MessagingException;
import com.messaging.common.exception.StorageException;
import com.messaging.common.model.MessageRecord;
import java.util.List;

/**
 * Storage abstraction interface.
 * Implementations: MMapStorageEngine, DiskStorageEngine, InMemoryStorageEngine
 */
public interface StorageEngine {

    /**
     * Append a message record to storage
     * @param topic Topic name
     * @param partition Partition number
     * @param record Message record to store
     * @return Assigned offset
     */
    long append(String topic, int partition, MessageRecord record) throws MessagingException;

    /**
     * Read messages from storage
     * @param topic Topic name
     * @param partition Partition number
     * @param fromOffset Starting offset (inclusive)
     * @param maxRecords Maximum number of records to return
     * @return List of message records
     */
    List<MessageRecord> read(String topic, int partition, long fromOffset, int maxRecords) throws MessagingException;

    /**
     * Get current (highest) offset for a topic-partition
     * @param topic Topic name
     * @param partition Partition number
     * @return Current offset, or -1 if no messages
     */
    long getCurrentOffset(String topic, int partition);

    /**
     * Get maximum offset from persistent segment metadata (for data refresh)
     * This reads from the metadata database and returns the true max offset,
     * unlike getCurrentOffset() which returns the in-memory write head.
     * @param topic Topic name
     * @param partition Partition number
     * @return Maximum offset from metadata, or -1 if no segments
     */
    long getMaxOffsetFromMetadata(String topic, int partition);

    /**
     * Get earliest (lowest) available offset for a topic-partition
     * This is the base offset of the first segment, not necessarily 0.
     * For example, after compaction or deletion, earliest offset might be 1000000.
     * @param topic Topic name
     * @param partition Partition number
     * @return Earliest offset, or 0 if no segments exist
     */
    long getEarliestOffset(String topic, int partition);

    /**
     * Compact segments for a topic-partition
     * Removes obsolete records, keeps only latest value per key
     * @param topic Topic name
     * @param partition Partition number
     */
    void compact(String topic, int partition);

    /**
     * Validate segment integrity
     * @param topic Topic name
     * @param partition Partition number
     * @return Validation result
     */
    ValidationResult validate(String topic, int partition);

    /**
     * Recover state from disk on startup
     */
    void recover() throws MessagingException;

    /**
     * Gracefully shutdown and flush all data
     */
    void close() throws MessagingException;

    /**
     * Validation result
     */
    class ValidationResult {
        private final boolean valid;
        private final List<String> errors;

        public ValidationResult(boolean valid, List<String> errors) {
            this.valid = valid;
            this.errors = errors;
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}
