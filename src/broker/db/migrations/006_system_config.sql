-- Connector configuration shared by all instances (cluster): sources.json, mapping.json
CREATE TABLE system_config (
  key        text PRIMARY KEY,
  value      jsonb NOT NULL,
  updated_by text,
  updated_at timestamptz NOT NULL DEFAULT now()
);
