-- Notifications waiting for another delivery attempt. A failed delivery is
-- moved here and acknowledged on the queue, so a failing endpoint never holds
-- queue slots that the other subscriptions need. Any instance picks up due rows
-- (FOR UPDATE SKIP LOCKED); claiming a row moves due_at forward, so a crashed
-- instance's rows are tried again once that lease passes.
CREATE TABLE notification_retries (
  id              uuid PRIMARY KEY,
  tenant          text NOT NULL,
  subscription_id uuid NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  notification    jsonb NOT NULL,
  attempts        integer NOT NULL,
  last_error      text,
  due_at          timestamptz NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notification_retries_due_idx ON notification_retries (due_at);
CREATE INDEX notification_retries_sub_idx ON notification_retries (subscription_id, due_at);
