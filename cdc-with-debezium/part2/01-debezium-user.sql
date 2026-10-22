-- Part 2, step 1: a schema migration, a least-privilege user for Debezium, and a publication owned by the DBA.
-- Run: docker compose exec -T postgres psql -U dispatchly -d dispatchly < part2/01-debezium-user.sql

-- shipment-service's next migration: cash-on-delivery amount and free-text delivery notes.
ALTER TABLE shipments ADD COLUMN cod_amount numeric(10, 2);
ALTER TABLE shipments ADD COLUMN delivery_notes text;

-- Consumers order changes by version, so every UPDATE must raise it, including a DBA's hot-fix that
-- forgets to. GREATEST keeps Spring Data JDBC's own increment (it already sets OLD.version + 1).
CREATE FUNCTION shipments_bump_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.version := GREATEST(NEW.version, OLD.version + 1);
    RETURN NEW;
END $$;
CREATE TRIGGER shipments_bump_version BEFORE UPDATE ON shipments
    FOR EACH ROW EXECUTE FUNCTION shipments_bump_version();

-- LOGIN and REPLICATION only: no superuser, no CREATE on the database, no table ownership.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT CONNECT ON DATABASE dispatchly TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON shipments TO debezium;          -- the initial snapshot reads the table

-- The DBA decides what is published; the connector runs with publication.autocreate.mode=disabled.
CREATE PUBLICATION dbz_dispatchly FOR TABLE shipments;

SELECT rolname, rolsuper, rolreplication FROM pg_roles WHERE rolname = 'debezium';
SELECT pubname, schemaname, tablename FROM pg_publication_tables;
