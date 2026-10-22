-- tracking-projection's read model, in its own database. Columns are nullable because a delete can
-- arrive for a shipment the view has never seen (the d event then carries only the id).
CREATE TABLE IF NOT EXISTS tracking_view (
    shipment_id   bigint PRIMARY KEY,
    tracking_code text,
    status        text,
    hub_id        text,
    eta           timestamptz,
    version       bigint,
    deleted       boolean     NOT NULL DEFAULT false,
    source_lsn    bigint      NOT NULL,   -- where in the WAL the last applied change came from
    changed_at    timestamptz NOT NULL    -- source.ts_ms: when the change happened in shipment-service's database
);
