-- Part 3, drill 1: the outbox table, and a publication that carries only it.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part3/01-outbox.sql

-- Debezium's outbox event router expects these column names by default.
CREATE TABLE outbox_event (
    id            uuid         PRIMARY KEY,   -- becomes the Kafka header "id": the consumer's dedupe key
    aggregatetype varchar(255) NOT NULL,      -- picks the topic: dispatchly.<aggregatetype>.events
    aggregateid   varchar(255) NOT NULL,      -- becomes the Kafka key: order per shipment
    type          varchar(255) NOT NULL,      -- the event type, sent as a header
    payload       jsonb        NOT NULL       -- the event itself
);
GRANT SELECT ON outbox_event TO debezium;

-- A separate connector reads the outbox through its own publication and slot.
CREATE PUBLICATION dbz_outbox FOR TABLE outbox_event;

-- notification-service's own database, for its inbox of processed event ids.
CREATE DATABASE notifications;
