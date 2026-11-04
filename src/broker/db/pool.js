const { Pool } = require('pg');

/**
 * Builds the PostgreSQL pool for the broker entity store.
 * BROKER_DB_URL takes precedence over the individual BROKER_DB_* variables.
 */
function createPool() {
  const config = process.env.BROKER_DB_URL
    ? { connectionString: process.env.BROKER_DB_URL }
    : {
      host: process.env.BROKER_DB_HOST || 'localhost',
      port: parseInt(process.env.BROKER_DB_PORT || '5432', 10),
      database: process.env.BROKER_DB_NAME || 'udc_broker',
      user: process.env.BROKER_DB_USER || 'postgres',
      password: process.env.BROKER_DB_PASSWORD || 'password'
    };

  return new Pool({
    ...config,
    max: parseInt(process.env.BROKER_DB_POOL_SIZE || '20', 10),
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000
  });
}

/**
 * Runs fn inside a transaction, committing on success and rolling back on error.
 */
async function withTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

module.exports = { createPool, withTransaction };
