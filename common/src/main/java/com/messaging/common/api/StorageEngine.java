package com.messaging.common.api;

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
    long append(String topic, int partition, MessageRecord record);

    /**
     * Read messages from storage
     * @param topic Topic name
     * @param partition Partition number
     * @param fromOffset Starting offset (inclusive)
     * @param maxRecords Maximum number of records to return
     * @return List of message records
     */
    List<MessageRecord> read(String topic, int partition, long fromOffset, int maxRecords);

    /**
     * Get current (highest) offset for a topic-partition
     * @param topic Topic name
     * @param partition Partition number
     * @return Current offset, or -1 if no messages
     */
    long getCurrentOffset(String topic, int partition);

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
    void recover();

    /**
     * Gracefully shutdown and flush all data
     */
    void close();

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
