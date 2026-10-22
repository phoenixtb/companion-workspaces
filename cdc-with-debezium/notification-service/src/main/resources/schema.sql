-- The inbox: one row per event this service has handled.
CREATE TABLE IF NOT EXISTS processed_event (
    event_id     uuid        PRIMARY KEY,
    event_type   text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
