-- Part 32: the support service's tables in Larkspur's OLTP PostgreSQL (database larkspur). The lab has no support
-- service, so `scripts.part32_models setup` creates them and loads the generated tickets and reviews.
DROP PUBLICATION IF EXISTS larkspur_support;
DROP SCHEMA IF EXISTS support CASCADE;
CREATE SCHEMA support;

CREATE TABLE support.tickets (
    ticket_id   text        PRIMARY KEY,
    customer_id text        NOT NULL,
    order_id    text        NOT NULL,
    country     char(2)     NOT NULL,
    city        text        NOT NULL,
    issue       text        NOT NULL,
    sku         text        NOT NULL,
    subject     text        NOT NULL,
    body        text        NOT NULL,          -- the thread: "Customer: …" and "Agent: …" messages, blank-line separated
    status      text        NOT NULL CHECK (status IN ('open', 'pending', 'closed')),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE TABLE support.reviews (
    review_id   text        PRIMARY KEY,
    customer_id text        NOT NULL,
    sku         text        NOT NULL,
    country     char(2)     NOT NULL,
    rating      smallint    NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title       text        NOT NULL,
    body        text        NOT NULL,
    status      text        NOT NULL CHECK (status IN ('published', 'hidden')),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);
-- REPLICA IDENTITY DEFAULT: a delete event carries the key only, which is all the index needs to drop a document.

GRANT USAGE ON SCHEMA support TO debezium;     -- the role from Part 16: LOGIN, REPLICATION, SELECT for snapshots
GRANT SELECT ON ALL TABLES IN SCHEMA support TO debezium;
CREATE PUBLICATION larkspur_support FOR TABLE support.tickets, support.reviews;
