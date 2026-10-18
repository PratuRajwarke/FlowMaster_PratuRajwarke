const { Pool } = require('pg');

const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    'postgres://flowmaster:flowmaster@localhost:5432/flowmaster',
});

async function query(sql, params) {
  return pool.query(sql, params);
}

module.exports = { query, pool };
