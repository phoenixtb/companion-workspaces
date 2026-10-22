package com.dispatchly.part1;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

// The change capture every team writes first: remember the newest updated_at you have seen,
// then ask for rows changed after it.
final class UpdatedAtPoller {

    record Row(String trackingCode, String status, OffsetDateTime updatedAt) {
        @Override
        public String toString() {
            return trackingCode + " " + status;
        }
    }

    private final Connection connection;
    private OffsetDateTime watermark;

    UpdatedAtPoller(Connection connection) throws SQLException {
        this.connection = connection;
        try (var statement = connection.createStatement();
             var rs = statement.executeQuery("SELECT max(updated_at) FROM shipments")) {
            rs.next();
            watermark = rs.getObject(1, OffsetDateTime.class);
        }
    }

    List<Row> poll() throws SQLException {
        var rows = new ArrayList<Row>();
        try (var query = connection.prepareStatement("""
                SELECT tracking_code, status, updated_at
                FROM shipments
                WHERE updated_at > ?
                ORDER BY updated_at""")) {
            query.setObject(1, watermark);
            try (var rs = query.executeQuery()) {
                while (rs.next()) {
                    var row = new Row(rs.getString(1), rs.getString(2), rs.getObject(3, OffsetDateTime.class));
                    rows.add(row);
                    watermark = row.updatedAt();   // the high-water mark only ever moves forward
                }
            }
        }
        return rows;
    }
}
