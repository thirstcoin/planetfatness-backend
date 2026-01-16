import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost")
    ? false
    : { rejectUnauthorized: false },
});

// 🔥 auto-migrate on startup
async function init() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      address TEXT PRIMARY KEY,
      total_calories BIGINT DEFAULT 0,
      best_seconds NUMERIC DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  console.log("✅ users table ready");
}

init().catch((err) => {
  console.error("❌ DB init failed", err);
  process.exit(1);
});

export function query(text: string, params?: any[]) {
  return pool.query(text, params);
}