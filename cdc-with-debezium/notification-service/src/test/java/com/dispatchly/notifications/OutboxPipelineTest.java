package com.dispatchly.notifications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
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

// The outbox path in containers: an outbox row -> Debezium's EventRouter -> dispatchly.shipment.events
// -> this service's inbox. Registers the same connectors/outbox.json the post uses.
@Testcontainers
@SpringBootTest(properties = {
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer"})
class OutboxPipelineTest {

    static final Network network = Network.newNetwork();

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("dispatchly")
            .withUsername("dispatchly")
            .withPassword("dispatchly")
            .withCopyFileToContainer(MountableFile.forHostPath("../postgres/init"), "/docker-entrypoint-initdb.d")
            .withCopyFileToContainer(MountableFile.forHostPath("../part2"), "/part2")
            .withCopyFileToContainer(MountableFile.forHostPath("../part3"), "/part3")
            .withCommand("postgres", "-c", "wal_level=logical");

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2")
            .withNetwork(network)
            .withListener("kafka:19092");

    @Container
    static final DebeziumContainer connect = new DebeziumContainer("quay.io/debezium/connect:3.7.0.Final")
            .withNetwork(network)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("CONNECT_CONFIG_PROVIDERS", "env")   // outbox.json reads its password from the environment
            .withEnv("CONNECT_CONFIG_PROVIDERS_ENV_CLASS", "org.apache.kafka.common.config.provider.EnvVarConfigProvider")
            .withEnv("DEBEZIUM_DB_PASSWORD", "debezium")
            .dependsOn(kafka, postgres);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl().replace("/dispatchly", "/notifications"));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void registerConnector() throws Exception {
        // Run the scripts with psql, as the post does: 01-outbox.sql creates a database, which can't
        // happen inside the single transaction a JDBC multi-statement execute would use.
        for (var script : new String[] {"/part2/01-debezium-user.sql", "/part3/01-outbox.sql"}) {
            var result = postgres.execInContainer("psql", "-v", "ON_ERROR_STOP=1", "-U", "dispatchly", "-d", "dispatchly", "-f", script);
            assertThat(result.getExitCode()).as(result.getStderr()).isZero();
        }
        Map<String, Object> config = JsonMapper.shared()
                .readValue(Path.of("../connectors/outbox.json").toFile(), new TypeReference<>() {});
        connect.registerConnector("outbox", ConnectorConfiguration.from(config));
        await().atMost(Duration.ofMinutes(1))
                .until(() -> connect.getConnectorTaskState("outbox", 0) == Connector.State.RUNNING);
    }

    @Autowired
    JdbcClient notifications;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void anOutboxEventIsHandledOnceEvenWhenDeliveredTwice() throws Exception {
        var eventId = UUID.randomUUID();
        try (var db = DriverManager.getConnection(postgres.getJdbcUrl(), "dispatchly", "dispatchly")) {
            db.setAutoCommit(false);   // insert and delete in one transaction, like shipment-service's Outbox
            var insert = db.prepareStatement("""
                    INSERT INTO outbox_event (id, aggregatetype, aggregateid, type, payload)
                    VALUES (?, 'shipment', 'DSP-1001', 'ShipmentStatusChanged', '{"status":"OUT_FOR_DELIVERY"}')""");
            insert.setObject(1, eventId);
            insert.executeUpdate();
            var delete = db.prepareStatement("DELETE FROM outbox_event WHERE id = ?");
            delete.setObject(1, eventId);
            delete.executeUpdate();
            db.commit();
        }
        await().atMost(Duration.ofSeconds(60)).until(() -> handled(eventId) == 1);

        // Deliver the same event again, as a replay after a Connect crash would.
        var duplicate = new ProducerRecord<>("dispatchly.shipment.events", "DSP-1001", "{\"status\":\"OUT_FOR_DELIVERY\"}");
        duplicate.headers().add("id", eventId.toString().getBytes(StandardCharsets.UTF_8));
        duplicate.headers().add("eventType", "ShipmentStatusChanged".getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(duplicate).get();

        Thread.sleep(3000);
        assertThat(handled(eventId)).isEqualTo(1);
    }

    private int handled(UUID eventId) {
        return notifications.sql("SELECT count(*) FROM processed_event WHERE event_id = ?")
                .param(eventId).query(Integer.class).single();
    }
}
