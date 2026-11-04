-- Tenants, users, roles and API keys

CREATE TABLE tenants (
  id          text PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
  name        text NOT NULL,
  description text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE users (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  username       text NOT NULL UNIQUE,
  password_hash  text NOT NULL,
  display_name   text,
  is_superadmin  boolean NOT NULL DEFAULT false,
  disabled       boolean NOT NULL DEFAULT false,
  -- Incremented on password change / disable to invalidate issued tokens
  token_version  integer NOT NULL DEFAULT 0,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE user_tenants (
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  tenant  text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  role    text NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
  PRIMARY KEY (user_id, tenant)
);

CREATE TABLE api_keys (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant        text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name          text NOT NULL,
  key_prefix    text NOT NULL,
  key_hash      text NOT NULL UNIQUE,
  role          text NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
  allowed_types text[],
  created_by    uuid REFERENCES users(id) ON DELETE SET NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz,
  revoked_at    timestamptz,
  last_used_at  timestamptz
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant);

-- Entity store

CREATE TABLE entities (
  tenant     text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  id         text NOT NULL,
  type       text NOT NULL,
  attrs      jsonb NOT NULL DEFAULT '{}'::jsonb,
  location   jsonb,
  version    bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant, id)
);
CREATE INDEX entities_type_idx ON entities (tenant, type);
CREATE INDEX entities_updated_idx ON entities (tenant, updated_at);
CREATE INDEX entities_attrs_idx ON entities USING gin (attrs jsonb_path_ops);

-- Projection of 'relationship' attributes, kept in sync by EntityService.
-- target_id is not a foreign key: relationships may point to entities created later.
CREATE TABLE entity_relationships (
  tenant    text NOT NULL,
  source_id text NOT NULL,
  attr      text NOT NULL,
  target_id text NOT NULL,
  PRIMARY KEY (tenant, source_id, attr),
  FOREIGN KEY (tenant, source_id) REFERENCES entities(tenant, id) ON DELETE CASCADE
);
CREATE INDEX entity_relationships_target_idx ON entity_relationships (tenant, target_id);
