const fs = require('fs');
const path = require('path');
const logger = require('../../utils/logger');

const MIGRATIONS_DIR = path.join(__dirname, 'migrations');
// Arbitrary constant: serializes migrations across broker instances sharing the DB
const MIGRATION_LOCK_ID = 74210931;

/**
 * Applies pending SQL migrations (db/migrations/NNN_name.sql) in order.
 * Each migration runs in its own transaction; an advisory lock makes it safe
 * to start several instances at once.
 */
async function migrate(pool) {
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name       text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )`);

    const { rows } = await client.query('SELECT name FROM schema_migrations');
    const applied = new Set(rows.map(r => r.name));
    const files = fs.readdirSync(MIGRATIONS_DIR).filter(f => f.endsWith('.sql')).sort();

    for (const file of files) {
      if (applied.has(file)) continue;
      const sql = fs.readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8');
      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
        await client.query('COMMIT');
        logger.info(`Broker migration applied: ${file}`);
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw new Error(`Migration ${file} failed: ${error.message}`);
      }
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID]).catch(() => {});
    client.release();
  }
}

module.exports = { migrate };
