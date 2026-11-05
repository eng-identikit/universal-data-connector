-- Service API keys: not bound to a tenant, act as a superadmin (used by the
-- orchestrator, universal-data-connector-ui-api, to configure this instance)
ALTER TABLE api_keys ADD COLUMN scope text NOT NULL DEFAULT 'tenant' CHECK (scope IN ('tenant', 'service'));
ALTER TABLE api_keys ALTER COLUMN tenant DROP NOT NULL;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_scope_tenant CHECK ((scope = 'tenant') = (tenant IS NOT NULL));
