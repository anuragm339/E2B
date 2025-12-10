package com.messaging.storage.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores segment metadata in SQLite
 */
public class SegmentMetadataStore {
    private static final Logger log = LoggerFactory.getLogger(SegmentMetadataStore.class);
    
    private final String dbPath;
    private Connection connection;

    public SegmentMetadataStore(Path dataDir) {
        this.dbPath = dataDir.resolve("segment_metadata.db").toString();
        initDatabase();
    }

    private void initDatabase() {
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
            log.error("Failed to initialize metadata store", e);
            throw new RuntimeException("Failed to initialize metadata store", e);
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
    public void saveSegment(SegmentMetadata metadata) {
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
            log.error("Failed to save segment metadata", e);
            throw new RuntimeException("Failed to save segment metadata", e);
        }
    }

    /**
     * Get all segments for a topic-partition
     */
    public List<SegmentMetadata> getSegments(String topic, int partition) {
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
            log.error("Failed to get segments", e);
            throw new RuntimeException("Failed to get segments", e);
        }

        return segments;
    }

    /**
     * Delete segment metadata
     */
    public void deleteSegment(String topic, int partition, long baseOffset) {
        String sql = "DELETE FROM segment_metadata WHERE topic = ? AND partition = ? AND base_offset = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topic);
            stmt.setInt(2, partition);
            stmt.setLong(3, baseOffset);
            
            stmt.executeUpdate();
            log.info("Deleted segment metadata: topic={}, partition={}, baseOffset={}", topic, partition, baseOffset);
        } catch (SQLException e) {
            log.error("Failed to delete segment metadata", e);
            throw new RuntimeException("Failed to delete segment metadata", e);
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
