-- Previous refresh token, accepted for a short interval after a rotation
-- (two browser tabs refreshing at the same time must not look like a stolen token)
ALTER TABLE auth_sessions ADD COLUMN prev_jti text, ADD COLUMN rotated_at timestamptz;
