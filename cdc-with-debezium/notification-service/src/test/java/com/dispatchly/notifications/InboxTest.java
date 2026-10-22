package com.dispatchly.notifications;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class InboxTest {

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6");

    static Inbox inbox;

    @BeforeAll
    static void createInbox() throws Exception {
        var jdbc = JdbcClient.create(new DriverManagerDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
        jdbc.sql(Files.readString(Path.of("src/main/resources/schema.sql"))).update();
        inbox = new Inbox(jdbc);
    }

    @Test
    void anEventIdIsHandledOnce() {
        var eventId = UUID.randomUUID();

        assertThat(inbox.firstTime(eventId, "ShipmentStatusChanged")).isTrue();
        assertThat(inbox.firstTime(eventId, "ShipmentStatusChanged")).isFalse();   // a redelivery
    }
}
