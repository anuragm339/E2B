package com.messaging.broker.controller;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import com.messaging.broker.consumer.RemoteConsumerRegistry;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test endpoint to load data from SQLite file and publish to consumers
 * Useful for testing data ingestion and consumer delivery
 */
@Controller("/test")
public class TestDataController {
    private static final Logger log = LoggerFactory.getLogger(TestDataController.class);

    private final StorageEngine storage;
    private final RemoteConsumerRegistry remoteConsumers;

    @Inject
    public TestDataController(StorageEngine storage, RemoteConsumerRegistry remoteConsumers) {
        this.storage = storage;
        this.remoteConsumers = remoteConsumers;
    }

    /**
     * Load data from SQLite file and publish to broker storage
     * POST /test/load-from-sqlite
     * Body: {"sqliteFilePath": "/path/to/data.db", "tableName": "messages", "topic": "test-topic"}
     */
    @Post("/load-from-sqlite")
    public Map<String, Object> loadFromSqlite(@Body Map<String, String> request) {
        String sqliteFilePath = request.get("sqliteFilePath");
        String tableName = request.getOrDefault("tableName", "messages");
        String topic = request.getOrDefault("topic", "price-topic");

        if (sqliteFilePath == null || sqliteFilePath.isEmpty()) {
            return Map.of(
                "success", false,
                "error", "sqliteFilePath is required"
            );
        }

        try {
            int recordsLoaded = loadDataFromSqlite(sqliteFilePath, tableName, topic);

            return Map.of(
                "success", true,
                "recordsLoaded", recordsLoaded,
                "topic", topic,
                "message", "Data loaded successfully and pushed to consumers"
            );

        } catch (Exception e) {
            log.error("Failed to load data from SQLite", e);
            return Map.of(
                "success", false,
                "error", e.getMessage()
            );
        }
    }

    /**
     * Inject test messages directly
     * POST /test/inject-messages
     * Body: {"count": 10, "topic": "test-topic", "prefix": "TEST"}
     */
    @Post("/inject-messages")
    public Map<String, Object> injectMessages(@Body Map<String, Object> request) {
        int count = (int) request.getOrDefault("count", 10);
        String topic = (String) request.getOrDefault("topic", "price-topic");
        String prefix = (String) request.getOrDefault("prefix", "TEST");

        try {
            List<Long> offsets = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                MessageRecord record = new MessageRecord(
                    prefix + "_" + i,
                    EventType.MESSAGE,
                    String.format("{\"id\":\"%s_%d\",\"value\":%d,\"timestamp\":\"%s\"}",
                        prefix, i, i * 100, Instant.now()),
                    Instant.now()
                );

                long offset = storage.append(topic, 0, record);
                offsets.add(offset);

                // Push notification to consumers
                remoteConsumers.notifyNewMessage(topic, offset);
            }

            return Map.of(
                "success", true,
                "messagesInjected", count,
                "topic", topic,
                "offsets", offsets,
                "message", "Messages injected and pushed to consumers"
            );

        } catch (Exception e) {
            log.error("Failed to inject messages", e);
            return Map.of(
                "success", false,
                "error", e.getMessage()
            );
        }
    }

    /**
     * Get broker statistics
     * GET /test/stats
     */
    @Get("/stats")
    public Map<String, Object> getStats() {
        // TODO: Implement actual stats gathering
        return Map.of(
            "broker", "running",
            "timestamp", Instant.now().toString()
        );
    }

    /**
     * Load data from SQLite database
     */
    private int loadDataFromSqlite(String filePath, String tableName, String topic) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + filePath;
        int count = 0;

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Auto-detect columns
            String query = "SELECT * FROM " + tableName;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    // Build JSON from all columns
                    Map<String, Object> data = new HashMap<>();
                    String msgKey = null;

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);

                        // Use first column or 'id' column as message key
                        if (i == 1 || "id".equalsIgnoreCase(columnName) || "key".equalsIgnoreCase(columnName)) {
                            msgKey = value != null ? value.toString() : "row_" + count;
                        }

                        data.put(columnName, value);
                    }

                    // Create message record
                    MessageRecord record = new MessageRecord(
                        msgKey != null ? msgKey : "row_" + count,
                        EventType.MESSAGE,
                        convertToJson(data),
                        Instant.now()
                    );

                    // Append to storage
                    long offset = storage.append(topic, 0, record);
                    count++;

                    // Push notification to consumers
                    remoteConsumers.notifyNewMessage(topic, offset);

                    if (count % 100 == 0) {
                        log.info("Loaded {} records from SQLite", count);
                    }
                }
            }
        }

        log.info("Successfully loaded {} records from SQLite file: {}", count, filePath);
        return count;
    }

    /**
     * Simple JSON conversion
     */
    private String convertToJson(Map<String, Object> data) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!first) json.append(",");
            first = false;

            json.append("\"").append(entry.getKey()).append("\":");

            Object value = entry.getValue();
            if (value == null) {
                json.append("null");
            } else if (value instanceof String) {
                json.append("\"").append(value.toString().replace("\"", "\\\"")).append("\"");
            } else if (value instanceof Number || value instanceof Boolean) {
                json.append(value);
            } else {
                json.append("\"").append(value.toString()).append("\"");
            }
        }

        json.append("}");
        return json.toString();
    }
}
