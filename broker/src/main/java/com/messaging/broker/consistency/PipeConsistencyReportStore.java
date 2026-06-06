package com.messaging.broker.consistency;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

/**
 * Persists the latest PipeConsistencyReport per (topic, partition, mode) to the broker
 * SQLite at {@code <dataDir>/pipe_consistency.db}.
 *
 * <p>Only the latest report per key is retained — older runs are overwritten. Operators
 * read the latest via {@code GET /admin/consistency/pipe/{topic}/{partition}/latest-report?mode=}.
 */
@Singleton
public class PipeConsistencyReportStore {
    private static final Logger LOG = LoggerFactory.getLogger(PipeConsistencyReportStore.class);

    private final String dbPath;
    private final ObjectMapper mapper = new ObjectMapper();

    public PipeConsistencyReportStore(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        try {
            Path dir = Paths.get(dataDir);
            Files.createDirectories(dir);
            this.dbPath = dir.resolve("pipe_consistency.db").toString();
            initSchema();
        } catch (Exception e) {
            throw new RuntimeException("Failed to init PipeConsistencyReportStore at " + dataDir, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Connection conn = open();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS pipe_consistency_report (
                    topic TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    checked_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    PRIMARY KEY(topic, partition, mode)
                )
            """);
        }
    }

    public synchronized void save(PipeConsistencyReport report) {
        String json;
        try {
            json = mapper.writeValueAsString(report.toJson());
        } catch (Exception e) {
            LOG.warn("Failed to serialize report: {}", e.getMessage());
            return;
        }
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO pipe_consistency_report(topic, partition, mode, checked_at, status, report_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(topic, partition, mode) DO UPDATE SET
                    checked_at = excluded.checked_at,
                    status = excluded.status,
                    report_json = excluded.report_json
             """)) {
            ps.setString(1, report.topic);
            ps.setInt(2, report.partition);
            ps.setString(3, report.mode.name().toLowerCase());
            ps.setString(4, report.checkedAt.toString());
            ps.setString(5, report.status.name().toLowerCase());
            ps.setString(6, json);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("Failed to persist report: {}", e.getMessage());
        }
    }

    /**
     * Return the raw JSON of the latest report for the key, or null if none.
     */
    public synchronized String getLatestJson(String topic, int partition, PipeConsistencyReport.Mode mode) {
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT report_json FROM pipe_consistency_report WHERE topic = ? AND partition = ? AND mode = ?")) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setString(3, mode.name().toLowerCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to load report: {}", e.getMessage());
        }
        return null;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
    }
}
