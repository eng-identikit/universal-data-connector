-- Attribute history and device commands

-- One row per attribute value change. num is set for numeric values (charts,
-- aggregation), val keeps the original value of any type.
CREATE TABLE attribute_history (
  time      timestamptz NOT NULL,
  tenant    text NOT NULL,
  entity_id text NOT NULL,
  attr      text NOT NULL,
  num       double precision,
  val       jsonb,
  quality   text
);
CREATE INDEX attribute_history_lookup_idx ON attribute_history (tenant, entity_id, attr, time DESC);

-- Hypertable when TimescaleDB is available; a plain table works too (slower on large ranges)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    PERFORM create_hypertable('attribute_history', 'time', chunk_time_interval => INTERVAL '1 day', migrate_data => true);
  END IF;
END
$$;

CREATE TABLE commands (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant       text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  entity_id    text NOT NULL,
  attr         text NOT NULL,
  value        jsonb,
  status       text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'succeeded', 'failed')),
  connector    text,
  target       jsonb,
  error        text,
  created_by   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);
CREATE INDEX commands_entity_idx ON commands (tenant, entity_id, created_at DESC);
CREATE INDEX commands_tenant_idx ON commands (tenant, created_at DESC);
