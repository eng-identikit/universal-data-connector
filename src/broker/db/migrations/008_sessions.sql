-- Login sessions. With BROKER_REDIS_URL they live in Redis and this table stays
-- empty; without Redis it is the session store.
CREATE TABLE auth_sessions (
  id           text PRIMARY KEY,
  user_id      uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  refresh_jti  text NOT NULL,
  ip           text,
  user_agent   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz NOT NULL
);
CREATE INDEX auth_sessions_user ON auth_sessions (user_id);

-- Access log: logins (successful and failed), refreshes reused, logouts, revocations
CREATE TABLE auth_events (
  id         bigserial PRIMARY KEY,
  at         timestamptz NOT NULL DEFAULT now(),
  kind       text NOT NULL,
  user_id    uuid,
  username   text,
  session_id text,
  ip         text,
  user_agent text,
  actor      text,
  detail     text
);
CREATE INDEX auth_events_at ON auth_events (at DESC);
CREATE INDEX auth_events_user ON auth_events (user_id, at DESC);
