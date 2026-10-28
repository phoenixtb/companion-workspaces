-- Part 16: the order service's tables, as the services own them, in Larkspur's OLTP PostgreSQL.
-- The lab has no order service, so `scripts.part16_cdc setup` creates them and the replayer writes to them.
DROP PUBLICATION IF EXISTS larkspur_shop;
DROP SCHEMA IF EXISTS shop CASCADE;
CREATE SCHEMA shop;

CREATE TABLE shop.orders (
    order_id     text        PRIMARY KEY,
    customer_id  text        NOT NULL,
    country      char(2)     NOT NULL,
    currency     char(3)     NOT NULL,
    channel      text        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('placed', 'fulfilled')),
    total_minor  bigint      NOT NULL,          -- money in minor units of `currency`
    tender_type  text        NOT NULL,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    fulfilled_at timestamptz
);
-- A cancelled order is hard-deleted. FULL puts the whole old row in the WAL for every UPDATE and DELETE, so the
-- delete event says what was deleted, not just which key.
ALTER TABLE shop.orders REPLICA IDENTITY FULL;

CREATE TABLE shop.payments (                     -- the payment ledger: in-store card/wallet/UPI and online
    payment_id    text        PRIMARY KEY,
    source        text        NOT NULL CHECK (source IN ('pos', 'online')),
    reference     text        NOT NULL,          -- basket_id or order_id
    country       char(2)     NOT NULL,
    currency      char(3)     NOT NULL,
    tender_type   text        NOT NULL,
    amount_minor  bigint      NOT NULL,
    status        text        NOT NULL CHECK (status IN ('authorised', 'captured', 'voided')),
    authorised_at timestamptz NOT NULL,
    captured_at   timestamptz,
    voided_at     timestamptz                    -- a soft delete: the row stays, the status says it's void
);

CREATE TABLE shop.refunds (                      -- insert-only
    refund_id          text        PRIMARY KEY,
    original_channel   text        NOT NULL,
    original_reference text        NOT NULL,
    country            char(2)     NOT NULL,
    store_id           text,
    currency           char(3)     NOT NULL,
    tender_type        text        NOT NULL,
    refund_ts          timestamptz NOT NULL,
    amount_minor       bigint      NOT NULL,
    reason             text        NOT NULL
);
-- payments and refunds keep REPLICA IDENTITY DEFAULT: their UPDATE events carry the key, which is all a MERGE needs.

-- Debezium logs in as this role: LOGIN and REPLICATION, SELECT for the initial snapshot, nothing else.
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium LOGIN REPLICATION;
    END IF;
END $$;
ALTER ROLE debezium PASSWORD 'debezium-lab-secret';
GRANT USAGE ON SCHEMA shop TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO debezium;

-- The DBA decides what is published; the connector runs with publication.autocreate.mode=disabled.
CREATE PUBLICATION larkspur_shop FOR TABLE shop.orders, shop.payments, shop.refunds;
