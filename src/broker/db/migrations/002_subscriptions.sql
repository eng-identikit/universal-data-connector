-- Subscriptions, delivery statistics and dead-letter queue

CREATE TABLE subscriptions (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant       text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name         text NOT NULL,
  description  text,
  enabled      boolean NOT NULL DEFAULT true,
  selector     jsonb NOT NULL DEFAULT '{}'::jsonb,
  trigger      jsonb NOT NULL DEFAULT '{}'::jsonb,
  filter       text,
  filter_mode  text NOT NULL DEFAULT 'level' CHECK (filter_mode IN ('level', 'edge')),
  throttle_ms  integer,
  periodic_ms  integer,
  notify       jsonb NOT NULL,
  delivery     jsonb NOT NULL DEFAULT '{}'::jsonb,
  expires_at   timestamptz,
  created_by   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX subscriptions_tenant_idx ON subscriptions (tenant);

-- Counters are incremented by every instance (matching and delivery may run on different nodes)
CREATE TABLE subscription_stats (
  subscription_id      uuid PRIMARY KEY REFERENCES subscriptions(id) ON DELETE CASCADE,
  matched              bigint NOT NULL DEFAULT 0,
  delivered            bigint NOT NULL DEFAULT 0,
  failed_attempts      bigint NOT NULL DEFAULT 0,
  dead                 bigint NOT NULL DEFAULT 0,
  last_notification_at timestamptz,
  last_success_at      timestamptz,
  last_failure_at      timestamptz,
  last_error           text,
  last_latency_ms      integer
);

CREATE TABLE notification_dlq (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant          text NOT NULL,
  subscription_id uuid NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  notification    jsonb NOT NULL,
  error           text,
  attempts        integer NOT NULL,
  failed_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notification_dlq_sub_idx ON notification_dlq (subscription_id, failed_at DESC);
