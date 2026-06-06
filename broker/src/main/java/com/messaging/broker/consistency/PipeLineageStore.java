package com.messaging.broker.consistency;

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
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tracks which upstream parent produced which offset range on this broker.
 *
 * <p>Append-only: each parent switch closes the previously-open row by setting
 * {@code offset_end_exclusive} to the first offset that belongs to the new parent,
 * then inserts a new open row. The PipeConsistency checker uses {@link #resolve}
 * to split an audit range into per-parent subranges.
 *
 * <p>This store is broker-wide (not per-topic) and lives in its own SQLite file at
 * {@code <dataDir>/pipe_consistency.db}.
 */
@Singleton
public class PipeLineageStore {
    private static final Logger log = LoggerFactory.getLogger(PipeLineageStore.class);

    private final String dbPath;
    private final Connection connection;

    public PipeLineageStore(@Value("${broker.storage.data-dir:./data}") String dataDir) {
        this(Paths.get(dataDir));
    }

    public PipeLineageStore(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
            this.dbPath = dataDir.resolve("pipe_consistency.db").toString();
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            createSchema();
            log.info("Initialized PipeLineageStore at {}", dbPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PipeLineageStore at " + dataDir, e);
        }
    }

    private void createSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS pipe_lineage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    offset_start BIGINT NOT NULL,
                    offset_end_exclusive BIGINT,
                    parent_url TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                )
            """);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_pipe_lineage_offset " +
                    "ON pipe_lineage(offset_start, offset_end_exclusive)");
        }
    }

    /**
     * Seed the lineage with a single open row if it's empty. Call this on broker startup
     * once the current parent URL is known. No-op if the table already has rows.
     */
    public synchronized void seedIfEmpty(String currentParentUrl) {
        if (currentParentUrl == null) {
            return;
        }
        try (PreparedStatement check = connection.prepareStatement(
                "SELECT COUNT(*) FROM pipe_lineage")) {
            try (ResultSet rs = check.executeQuery()) {
                if (rs.next() && rs.getLong(1) > 0) {
                    return;
                }
            }
            insertOpenRow(0L, currentParentUrl);
            log.info("Seeded pipe_lineage with currentParentUrl={} at offset_start=0", currentParentUrl);
        } catch (SQLException e) {
            log.error("Failed to seed pipe_lineage", e);
        }
    }

    /**
     * Close the currently-open lineage row at firstOffsetFromNewParent and open a new row
     * with the new parent. If no row is open (cold-start race), just insert the new row.
     */
    public synchronized void recordParentSwitch(String newParentUrl, long firstOffsetFromNewParent) {
        if (newParentUrl == null) {
            return;
        }
        try {
            try (PreparedStatement closeOpen = connection.prepareStatement(
                    "UPDATE pipe_lineage SET offset_end_exclusive = ? " +
                            "WHERE offset_end_exclusive IS NULL")) {
                closeOpen.setLong(1, firstOffsetFromNewParent);
                closeOpen.executeUpdate();
            }
            insertOpenRow(firstOffsetFromNewParent, newParentUrl);
            log.info("Recorded parent switch: newParent={} firstOffset={}",
                    newParentUrl, firstOffsetFromNewParent);
        } catch (SQLException e) {
            log.error("Failed to record parent switch", e);
        }
    }

    private void insertOpenRow(long offsetStart, String parentUrl) throws SQLException {
        try (PreparedStatement ins = connection.prepareStatement(
                "INSERT INTO pipe_lineage(offset_start, offset_end_exclusive, parent_url, recorded_at) " +
                        "VALUES (?, ?, ?, ?)")) {
            ins.setLong(1, offsetStart);
            ins.setNull(2, Types.BIGINT);
            ins.setString(3, parentUrl);
            ins.setString(4, Instant.now().toString());
            ins.executeUpdate();
        }
    }

    /**
     * Split [fromOffset, toOffsetInclusive] into one or more subranges, one per lineage row
     * that overlaps the query range. Subranges are returned in offset order.
     *
     * If the range falls entirely outside any recorded lineage, returns an empty list.
     */
    public synchronized List<Subrange> resolve(long fromOffset, long toOffsetInclusive) {
        if (toOffsetInclusive < fromOffset) {
            return Collections.emptyList();
        }
        List<PipeLineageEntry> rows = allEntries();
        List<Subrange> out = new ArrayList<>();
        for (PipeLineageEntry row : rows) {
            long rowStart = row.getOffsetStart();
            long rowEnd = row.maxOffsetInclusive();
            long lo = Math.max(rowStart, fromOffset);
            long hi = Math.min(rowEnd, toOffsetInclusive);
            if (lo <= hi) {
                out.add(new Subrange(lo, hi, row.getParentUrl()));
            }
        }
        return out;
    }

    public synchronized PipeLineageEntry currentLineage() {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT id, offset_start, offset_end_exclusive, parent_url, recorded_at " +
                        "FROM pipe_lineage WHERE offset_end_exclusive IS NULL " +
                        "ORDER BY id DESC LIMIT 1")) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapRow(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to read current lineage", e);
        }
        return null;
    }

    public synchronized List<PipeLineageEntry> allEntries() {
        List<PipeLineageEntry> result = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT id, offset_start, offset_end_exclusive, parent_url, recorded_at " +
                        "FROM pipe_lineage ORDER BY offset_start ASC, id ASC")) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to read all lineage", e);
        }
        return result;
    }

    private PipeLineageEntry mapRow(ResultSet rs) throws SQLException {
        long endExclusive = rs.getLong("offset_end_exclusive");
        Long endBoxed = rs.wasNull() ? null : endExclusive;
        return new PipeLineageEntry(
                rs.getLong("id"),
                rs.getLong("offset_start"),
                endBoxed,
                rs.getString("parent_url"),
                Instant.parse(rs.getString("recorded_at")));
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            log.warn("Failed to close PipeLineageStore", e);
        }
    }

    /** A contiguous subrange [fromOffsetInclusive, toOffsetInclusive] produced by a single parent. */
    public static final class Subrange {
        public final long fromOffsetInclusive;
        public final long toOffsetInclusive;
        public final String parentUrl;

        public Subrange(long fromOffsetInclusive, long toOffsetInclusive, String parentUrl) {
            this.fromOffsetInclusive = fromOffsetInclusive;
            this.toOffsetInclusive = toOffsetInclusive;
            this.parentUrl = parentUrl;
        }
    }
}
