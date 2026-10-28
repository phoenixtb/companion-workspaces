-- Part 13: the store-operations service's table of monthly sales targets, in Larkspur's OLTP PostgreSQL.
-- The lab has no store-ops service, so `scripts.part13_engines seed` creates the table and fills it:
-- one target per store and month for Jul-Dec 2026, 104% of the same month a year earlier (synthetic).
DROP SCHEMA IF EXISTS store_ops CASCADE;
CREATE SCHEMA store_ops;
CREATE TABLE store_ops.sales_targets (
    store_id     text        NOT NULL,
    month        date        NOT NULL,          -- first day of the month, the store's local calendar
    currency     char(3)     NOT NULL,
    target_minor bigint      NOT NULL,          -- money in minor units, like every fact table in the lake
    set_by       text        NOT NULL,          -- the regional manager who set it
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (store_id, month)
);

-- Trino logs in as this role: read-only, and only on this schema.
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trino_reader') THEN
        CREATE ROLE trino_reader LOGIN;
    END IF;
END $$;
ALTER ROLE trino_reader PASSWORD 'trino-reader-lab-secret';
GRANT USAGE ON SCHEMA store_ops TO trino_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA store_ops TO trino_reader;
