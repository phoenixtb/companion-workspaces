package com.dispatchly.part1;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

// Part 1, step 4: the same writes, watched by an updated_at poller and by a replication slot.
// Run against the compose database:  ./gradlew :naive-poller:run
public final class PollerVsLog {

    record Report(List<String> pollerSaw, List<String> slotSaw) {}

    public static void main(String[] args) throws SQLException {
        var url = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/dispatchly");
        run(() -> DriverManager.getConnection(url, "dispatchly", "dispatchly"), System.out);
    }

    interface Connections {
        Connection open() throws SQLException;
    }

    static Report run(Connections db, PrintStream out) throws SQLException {
        try (var admin = db.open(); var writerA = db.open(); var writerB = db.open(); var reader = db.open()) {
            resetShipments(admin);
            var pollerSaw = new ArrayList<String>();

            try (var slot = new SlotReader(admin, "part1_poller_vs_log")) {
                var poller = new UpdatedAtPoller(reader);

                // 1. Two status changes between polls, and a cancelled shipment.
                setStatus(writerB, "DSP-1001", "AT_HUB");
                out.println("writer  DSP-1001 -> AT_HUB");
                setStatus(writerB, "DSP-1001", "OUT_FOR_DELIVERY");
                out.println("writer  DSP-1001 -> OUT_FOR_DELIVERY");
                execute(writerB, "DELETE FROM shipments WHERE tracking_code = 'DSP-1002'");
                out.println("writer  DSP-1002 deleted");
                pollerSaw.addAll(poll(poller, 1, out));

                // 2. A slow transaction takes its timestamp first but commits last.
                writerA.setAutoCommit(false);
                setStatus(writerA, "DSP-1003", "AT_HUB");
                out.println("writer  tx A: DSP-1003 -> AT_HUB (not committed yet)");
                setStatus(writerB, "DSP-1004", "PICKED_UP");
                out.println("writer  tx B: DSP-1004 -> PICKED_UP (committed)");
                pollerSaw.addAll(poll(poller, 2, out));
                writerA.commit();
                out.println("writer  tx A committed");
                pollerSaw.addAll(poll(poller, 3, out));

                // 3. What the database's own log recorded, in commit order.
                var slotSaw = slot.read();
                out.println();
                slotSaw.forEach(change -> out.println("slot    " + change));
                out.println();
                out.printf("The poller saw %d of %d committed changes.%n", pollerSaw.size(), slotSaw.size());
                return new Report(pollerSaw, slotSaw);
            }
        }
    }

    // Like an application would: bump the version and stamp updated_at in the same statement.
    // now() is the time the transaction started, not the time it commits.
    private static void setStatus(Connection connection, String trackingCode, String status) throws SQLException {
        try (var update = connection.prepareStatement("""
                UPDATE shipments
                SET status = ?, version = version + 1, updated_at = now()
                WHERE tracking_code = ?""")) {
            update.setString(1, status);
            update.setString(2, trackingCode);
            update.executeUpdate();
        }
    }

    private static List<String> poll(UpdatedAtPoller poller, int number, PrintStream out) throws SQLException {
        var rows = poller.poll().stream().map(Object::toString).toList();
        out.println("poller  poll " + number + ": " + rows);
        return rows;
    }

    // Puts the four seed shipments back, so every run starts from the same state.
    private static void resetShipments(Connection connection) throws SQLException {
        execute(connection, "TRUNCATE shipments RESTART IDENTITY");
        execute(connection, """
                INSERT INTO shipments (tracking_code, status, hub_id, eta) VALUES
                    ('DSP-1001', 'PICKED_UP', 'BLR-07', '2026-10-23 18:00+05:30'),
                    ('DSP-1002', 'CREATED',   'BLR-07', '2026-10-24 12:00+05:30'),
                    ('DSP-1003', 'PICKED_UP', 'BLR-12', '2026-10-23 20:00+05:30'),
                    ('DSP-1004', 'CREATED',   'BLR-12', '2026-10-24 15:00+05:30')""");
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
