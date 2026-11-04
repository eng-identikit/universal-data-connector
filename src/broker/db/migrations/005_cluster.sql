-- Cluster membership and leases (connectors, subscription partitions)

CREATE TABLE cluster_instances (
  id           text PRIMARY KEY,
  host         text NOT NULL,
  started_at   timestamptz NOT NULL DEFAULT now(),
  heartbeat_at timestamptz NOT NULL DEFAULT now(),
  info         jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE cluster_leases (
  resource    text PRIMARY KEY,
  owner       text NOT NULL,
  acquired_at timestamptz NOT NULL DEFAULT now(),
  expires_at  timestamptz NOT NULL
);
CREATE INDEX cluster_leases_owner_idx ON cluster_leases (owner);
