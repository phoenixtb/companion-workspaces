package com.dispatchly.tracking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

// The whole Part 2 pipeline in containers: PostgreSQL -> Debezium on Kafka Connect -> Kafka -> this
// application -> tracking_view. It registers the same connectors/shipments-cdc.json the post uses.
@Testcontainers
@SpringBootTest
class ShipmentPipelineTest {

    static final Network network = Network.newNetwork();

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6")
            .withNetwork(network)
            .withNetworkAliases("postgres")   // the hostname in shipments-cdc.json
            .withDatabaseName("dispatchly")
            .withUsername("dispatchly")
            .withPassword("dispatchly")
            .withCopyFileToContainer(MountableFile.forHostPath("../postgres/init"), "/docker-entrypoint-initdb.d")
            .withCommand("postgres", "-c", "wal_level=logical");

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2")
            .withNetwork(network)
            .withListener("kafka:19092");     // a second listener for containers on the network

    // DebeziumContainer's withKafka() only accepts a Strimzi test cluster in 3.7, so the
    // bootstrap address of the apache/kafka container is passed in directly.
    @Container
    static final DebeziumContainer connect = new DebeziumContainer("quay.io/debezium/connect:3.7.0.Final")
            .withNetwork(network)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
            .dependsOn(kafka, postgres);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl().replace("/dispatchly", "/tracking"));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void registerConnector() throws Exception {
        try (var db = DriverManager.getConnection(postgres.getJdbcUrl(), "dispatchly", "dispatchly")) {
            db.createStatement().execute(Files.readString(Path.of("../part2/01-debezium-user.sql")));
        }
        Map<String, Object> config = JsonMapper.shared()
                .readValue(Path.of("../connectors/shipments-cdc.json").toFile(), new TypeReference<>() {});
        connect.registerConnector("shipments-cdc", ConnectorConfiguration.from(config));
        await().atMost(Duration.ofMinutes(1))
                .until(() -> connect.getConnectorTaskState("shipments-cdc", 0) == Connector.State.RUNNING);
    }

    @Autowired
    JdbcClient tracking;

    @Autowired
    TrackingView view;

    @Test
    void snapshotAndStatusChangesReachTheView() throws Exception {
        execute("UPDATE shipments SET status = 'OUT_FOR_DELIVERY', version = version + 1, updated_at = now() "
                + "WHERE tracking_code = 'DSP-1001'");

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(statusAndVersion("DSP-1001")).isEqualTo("OUT_FOR_DELIVERY v1");
            assertThat(statusAndVersion("DSP-1004")).isEqualTo("CREATED v0");   // from the initial snapshot
        });
    }

    @Test
    void deleteIsTerminalAndOlderChangesAreIgnored() throws Exception {
        execute("DELETE FROM shipments WHERE tracking_code = 'DSP-1002'");

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(
                tracking.sql("SELECT deleted FROM tracking_view WHERE shipment_id = 2").query(Boolean.class).single())
                .isTrue());

        // A replayed snapshot event for the deleted shipment changes nothing.
        var replayed = new ShipmentChange.Row(2, "DSP-1002", "CREATED", "BLR-07", null, 0L, null, null);
        assertThat(view.upsert(replayed, new ShipmentChange.Source(1L, 1L, 0L, "true"))).isFalse();
    }

    @Test
    void anUpdateThatForgetsVersionStillReachesTheView() throws Exception {
        execute("UPDATE shipments SET hub_id = 'BLR-03' WHERE tracking_code = 'DSP-1003'");   // a hand-written hot-fix

        // The trigger raised version from 0 to 1, so the guard applies the change instead of skipping it.
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(
                tracking.sql("SELECT hub_id || ' v' || version FROM tracking_view WHERE tracking_code = 'DSP-1003'")
                        .query(String.class).single())
                .isEqualTo("BLR-03 v1"));
    }

    private String statusAndVersion(String trackingCode) {
        return tracking.sql("SELECT status || ' v' || version FROM tracking_view WHERE tracking_code = ? AND NOT deleted")
                .param(trackingCode)
                .query(String.class)
                .optional()
                .orElse("missing");
    }

    private static void execute(String sql) throws Exception {
        try (var db = DriverManager.getConnection(postgres.getJdbcUrl(), "dispatchly", "dispatchly")) {
            db.createStatement().execute(sql);
        }
    }
}
