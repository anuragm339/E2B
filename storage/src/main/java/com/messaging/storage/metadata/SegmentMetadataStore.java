package com.messaging.storage.metadata;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.ExceptionLogger;
import com.messaging.common.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores segment metadata in SQLite for a specific topic
 * Each topic has its own metadata database: {topicDir}/segment_metadata.db
 */
public class SegmentMetadataStore {
    private static final Logger log = LoggerFactory.getLogger(SegmentMetadataStore.class);

    private final String dbPath;
    private Connection connection;

    public SegmentMetadataStore(Path topicDir) throws StorageException {
        this.dbPath = topicDir.resolve("segment_metadata.db").toString();
        initDatabase();
    }

    private void initDatabase() throws StorageException {
        try {
            // Ensure parent directory exists
            Path dbFilePath = Path.of(dbPath);
            if (dbFilePath.getParent() != null) {
                java.nio.file.Files.createDirectories(dbFilePath.getParent());
            }

            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            createTables();
            log.info("Initialized SegmentMetadataStore: {}", dbPath);
        } catch (Exception e) {
            StorageException ex = new StorageException(ErrorCode.STORAGE_METADATA_ERROR,
                "Failed to initialize metadata store", e);
            ex.withContext("dbPath", dbPath);
            ExceptionLogger.logError(log, ex);
            throw ex;
        }
    }

    private void createTables() throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS segment_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                partition INTEGER NOT NULL,
                base_offset BIGINT NOT NULL,
                max_offset BIGINT NOT NULL,
                log_file_path TEXT NOT NULL,
                index_file_path TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                record_count BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(topic, partition, base_offset)
            )
        """;

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);

            // Create indices
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_topic_partition ON segment_metadata(topic, partition)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_base_offset ON segment_metadata(base_offset)");
        }
    }

    /**
     * Save or update segment metadata
     */
    public void saveSegment(SegmentMetadata metadata) throws StorageException {
        String sql = """
            INSERT OR REPLACE INTO segment_metadata 
            (topic, partition, base_offset, max_offset, log_file_path, index_file_path, 
             size_bytes, record_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, metadata.getTopic());
            stmt.setInt(2, metadata.getPartition());
            stmt.setLong(3, metadata.getBaseOffset());
            stmt.setLong(4, metadata.getMaxOffset());
            stmt.setString(5, metadata.getLogFilePath());
            stmt.setString(6, metadata.getIndexFilePath());
            stmt.setLong(7, metadata.getSizeBytes());
            stmt.setLong(8, metadata.getRecordCount());
            stmt.setString(9, metadata.getCreatedAt().toString());
            stmt.setString(10, Instant.now().toString());
            
            stmt.executeUpdate();
            log.debug("Saved segment metadata: topic={}, partition={}, baseOffset={}",
                    metadata.getTopic(), metadata.getPartition(), metadata.getBaseOffset());
        } catch (SQLException e) {
            StorageException ex = new StorageException(ErrorCode.STORAGE_METADATA_ERROR,
                "Failed to save segment metadata", e);
            ex.withTopic(metadata.getTopic());
            ex.withPartition(metadata.getPartition());
            ex.withContext("baseOffset", metadata.getBaseOffset());
            ExceptionLogger.logError(log, ex);
            throw ex;
        }
    }

    /**
     * Get all segments for a topic-partition
     */
    public List<SegmentMetadata> getSegments(String topic, int partition) throws StorageException {
        String sql = """
            SELECT topic, partition, base_offset, max_offset, log_file_path, index_file_path,
                   size_bytes, record_count, created_at, updated_at
            FROM segment_metadata
            WHERE topic = ? AND partition = ?
            ORDER BY base_offset ASC
        """;

        List<SegmentMetadata> segments = new ArrayList<>();
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topic);
            stmt.setInt(2, partition);
            
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                segments.add(SegmentMetadata.builder()
                        .topic(rs.getString("topic"))
                        .partition(rs.getInt("partition"))
                        .baseOffset(rs.getLong("base_offset"))
                        .maxOffset(rs.getLong("max_offset"))
                        .logFilePath(rs.getString("log_file_path"))
                        .indexFilePath(rs.getString("index_file_path"))
                        .sizeBytes(rs.getLong("size_bytes"))
                        .recordCount(rs.getLong("record_count"))
                        .createdAt(Instant.parse(rs.getString("created_at")))
                        .build());
            }
        } catch (SQLException e) {
            StorageException ex = new StorageException(ErrorCode.STORAGE_METADATA_ERROR,
                "Failed to get segments", e);
            ex.withTopic(topic);
            ex.withPartition(partition);
            ExceptionLogger.logError(log, ex);
            throw ex;
        }

        return segments;
    }

    /**
     * Get the maximum offset for a topic-partition
     * This is used as the watermark to check if new data is available
     *
     * @param topic The topic name
     * @param partition The partition number
     * @return The maximum offset, or -1 if no data exists
     */
    public long getMaxOffset(String topic, int partition) {
        String sql = "SELECT MAX(max_offset) FROM segment_metadata WHERE topic = ? AND partition = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topic);
            stmt.setInt(2, partition);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long maxOffset = rs.getLong(1);
                    // If no rows found, getLong returns 0; check if it was NULL
                    if (rs.wasNull()) {
                        log.trace("No segments found for topic={}, partition={}", topic, partition);
                        return -1;
                    }
                    log.trace("Max offset for topic={}, partition={}: {}", topic, partition, maxOffset);
                    return maxOffset;
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get max offset for topic={}, partition={}", topic, partition, e);
        }

        return -1;  // No data or error
    }

    /**
     * Delete segment metadata
     */
    public void deleteSegment(String topic, int partition, long baseOffset) throws StorageException {
        String sql = "DELETE FROM segment_metadata WHERE topic = ? AND partition = ? AND base_offset = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topic);
            stmt.setInt(2, partition);
            stmt.setLong(3, baseOffset);

            stmt.executeUpdate();
            log.info("Deleted segment metadata: topic={}, partition={}, baseOffset={}", topic, partition, baseOffset);
        } catch (SQLException e) {
            StorageException ex = new StorageException(ErrorCode.STORAGE_METADATA_ERROR,
                "Failed to delete segment metadata", e);
            ex.withTopic(topic);
            ex.withPartition(partition);
            ex.withContext("baseOffset", baseOffset);
            ExceptionLogger.logError(log, ex);
            throw ex;
        }
    }

    /**
     * Close the database connection
     */
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                log.info("Closed metadata store");
            }
        } catch (SQLException e) {
            log.error("Failed to close metadata store", e);
        }
    }
}
