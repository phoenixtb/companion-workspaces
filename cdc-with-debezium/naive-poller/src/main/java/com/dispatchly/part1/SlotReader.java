package com.dispatchly.part1;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

// Reads committed changes from a logical replication slot with the test_decoding plugin,
// and condenses each line to "OPERATION tracking_code status" (or the key, when that is all the log has).
final class SlotReader implements AutoCloseable {

    private static final Pattern CHANGE = Pattern.compile("^table public\\.shipments: (INSERT|UPDATE|DELETE): (.*)$");
    private static final Pattern CODE = Pattern.compile("tracking_code\\[text]:'([^']*)'");
    private static final Pattern STATUS = Pattern.compile("status\\[text]:'([^']*)'");
    private static final Pattern ID = Pattern.compile("id\\[bigint]:(\\d+)");

    private final Connection connection;
    private final String slot;

    SlotReader(Connection connection, String slot) throws SQLException {
        this.connection = connection;
        this.slot = slot;
        try (var create = connection.prepareStatement(
                "SELECT pg_create_logical_replication_slot(?, 'test_decoding')")) {
            create.setString(1, slot);
            create.execute();
        }
    }

    // Consumes everything committed since the last call, in commit order.
    List<String> read() throws SQLException {
        var changes = new ArrayList<String>();
        try (var get = connection.prepareStatement(
                "SELECT data FROM pg_logical_slot_get_changes(?, NULL, NULL, 'skip-empty-xacts', '1')")) {
            get.setString(1, slot);
            try (var rs = get.executeQuery()) {
                while (rs.next()) {
                    var change = CHANGE.matcher(rs.getString(1));
                    if (change.matches()) {               // skip the BEGIN and COMMIT lines
                        changes.add(describe(change.group(1), change.group(2)));
                    }
                }
            }
        }
        return changes;
    }

    private static String describe(String operation, String columns) {
        var code = CODE.matcher(columns);
        var status = STATUS.matcher(columns);
        if (code.find() && status.find()) {
            return operation + " " + code.group(1) + " " + status.group(1);
        }
        var id = ID.matcher(columns);
        return operation + " id=" + (id.find() ? id.group(1) : "?");   // REPLICA IDENTITY DEFAULT: key only
    }

    @Override
    public void close() throws SQLException {
        try (var drop = connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
            drop.setString(1, slot);
            drop.execute();
        }
    }
}
