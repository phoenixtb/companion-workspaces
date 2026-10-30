-- Part 33: the support tickets an agent may read, and the one role it reads them as (database larkspur).
-- Least privilege: the MCP server's ticket tool connects as agent_support_reader, which can read this table and
-- nothing else; the lab password is fake, like every secret in .env.example.
DROP SCHEMA IF EXISTS agent CASCADE;
CREATE SCHEMA agent;

CREATE TABLE agent.support_tickets (
    ticket_id  text        PRIMARY KEY,
    subject    text        NOT NULL,
    body       text        NOT NULL,   -- the thread, personal data redacted (Part 32's redaction)
    status     text        NOT NULL,
    created_at timestamptz NOT NULL
);

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'agent_support_reader') THEN
        CREATE ROLE agent_support_reader LOGIN PASSWORD 'agent_support_reader';
    END IF;
END $$;
REVOKE ALL ON DATABASE larkspur FROM agent_support_reader;
GRANT CONNECT ON DATABASE larkspur TO agent_support_reader;
GRANT USAGE ON SCHEMA agent TO agent_support_reader;
GRANT SELECT ON agent.support_tickets TO agent_support_reader;
