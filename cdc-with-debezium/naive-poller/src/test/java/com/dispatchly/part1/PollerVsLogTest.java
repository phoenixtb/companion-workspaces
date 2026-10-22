package com.dispatchly.part1;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class PollerVsLogTest {

    // Same image, settings and schema as compose.yaml.
    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6")
            .withDatabaseName("dispatchly")
            .withUsername("dispatchly")
            .withPassword("dispatchly")
            .withCopyFileToContainer(
                    MountableFile.forHostPath("../postgres/init/01-dispatchly-schema.sql"),
                    "/docker-entrypoint-initdb.d/01-dispatchly-schema.sql")
            .withCommand("postgres", "-c", "wal_level=logical");

    @Test
    void pollerMissesWhatTheSlotRecords() throws Exception {
        var output = new ByteArrayOutputStream();
        var report = PollerVsLog.run(
                () -> DriverManager.getConnection(postgres.getJdbcUrl(), "dispatchly", "dispatchly"),
                new PrintStream(output, true, StandardCharsets.UTF_8));

        // The poller sees only the latest state of DSP-1001, never the delete, and never tx A.
        assertThat(report.pollerSaw()).containsExactly("DSP-1001 OUT_FOR_DELIVERY", "DSP-1004 PICKED_UP");

        // The slot sees every committed change, in commit order: tx B before tx A.
        assertThat(report.slotSaw()).containsExactly(
                "UPDATE DSP-1001 AT_HUB",
                "UPDATE DSP-1001 OUT_FOR_DELIVERY",
                "DELETE id=2",
                "UPDATE DSP-1004 PICKED_UP",
                "UPDATE DSP-1003 AT_HUB");

        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("poller  poll 3: []")
                .contains("The poller saw 2 of 5 committed changes.");
    }

    @Test
    void slotIsDroppedAfterTheRun() throws Exception {
        PollerVsLog.run(
                () -> DriverManager.getConnection(postgres.getJdbcUrl(), "dispatchly", "dispatchly"),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));

        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(), "dispatchly", "dispatchly");
             var rs = connection.createStatement().executeQuery("SELECT count(*) FROM pg_replication_slots")) {
            rs.next();
            assertThat(rs.getInt(1)).isZero();
        }
    }
}
