package com.dispatchly.shipments;

import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

// The transactional outbox. An event is a row written in the caller's transaction, so it commits or
// rolls back with the business change. The row is deleted straight away: Debezium reads the INSERT from
// the WAL, and the table never grows.
@Component
class Outbox {

    private final JdbcClient jdbc;
    private final JsonMapper json;

    Outbox(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    void publish(String aggregateType, String aggregateId, String type, Object payload) {
        var id = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO outbox_event (id, aggregatetype, aggregateid, type, payload)
                VALUES (?, ?, ?, ?, ?::jsonb)""")
                .params(id, aggregateType, aggregateId, type, json.writeValueAsString(payload))
                .update();
        jdbc.sql("DELETE FROM outbox_event WHERE id = ?").param(id).update();
    }
}
