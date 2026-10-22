package com.dispatchly.notifications;

import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

// The idempotent-consumer half of the outbox pattern: remember every event id already handled.
@Repository
class Inbox {

    private final JdbcClient jdbc;

    Inbox(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // True the first time an id is seen. Call it in the same transaction as the work it guards.
    boolean firstTime(UUID eventId, String eventType) {
        return jdbc.sql("INSERT INTO processed_event (event_id, event_type) VALUES (?, ?) ON CONFLICT DO NOTHING")
                .params(eventId, eventType)
                .update() == 1;
    }
}
