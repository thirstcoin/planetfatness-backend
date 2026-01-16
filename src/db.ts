import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

export const pool = new Pool({
  connectionString: DATABASE_URL,
  // Render external DB URLs require SSL. Internal often works without, but this is safe:
  ssl: DATABASE_URL.includes("localhost") ? undefined : { rejectUnauthorized: false },
});

export async function initDb() {
  // One-time table bootstrap (no manual psql needed)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      address TEXT PRIMARY KEY,
      total_calories BIGINT DEFAULT 0,
      best_seconds NUMERIC DEFAULT 0,
      total_miles NUMERIC DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Optional helper trigger-ish behavior (simple update timestamp)
  await pool.query(`
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = NOW();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `).catch(() => {});

  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'users_set_updated_at'
      ) THEN
        CREATE TRIGGER users_set_updated_at
        BEFORE UPDATE ON users
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
      END IF;
    END$$;
  `).catch(() => {});
}

export type UserRow = {
  address: string;
  total_calories: string | number;
  best_seconds: string | number;
  total_miles: string | number;
};

export async function upsertUser(address: string) {
  await pool.query(
    `INSERT INTO users (address) VALUES ($1)
     ON CONFLICT (address) DO NOTHING`,
    [address]
  );
}

export async function getMe(address: string) {
  const r = await pool.query<UserRow>(
    `SELECT address, total_calories, best_seconds, total_miles
     FROM users WHERE address=$1`,
    [address]
  );
  return r.rows[0] || null;
}

export async function addActivity(params: {
  address: string;
  addCalories?: number;
  bestSeconds?: number;
  addMiles?: number;
}) {
  const addCalories = Math.max(0, Math.floor(params.addCalories ?? 0));
  const addMiles = Math.max(0, Number(params.addMiles ?? 0));
  const bestSeconds = Number(params.bestSeconds ?? 0);

  // Update calories/miles additively, and best_seconds as max()
  await pool.query(
    `
    INSERT INTO users (address, total_calories, best_seconds, total_miles)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (address) DO UPDATE SET
      total_calories = users.total_calories + EXCLUDED.total_calories,
      total_miles    = users.total_miles + EXCLUDED.total_miles,
      best_seconds   = GREATEST(users.best_seconds, EXCLUDED.best_seconds)
    `,
    [params.address, addCalories, bestSeconds, addMiles]
  );

  return getMe(params.address);
}

export async function getLeaderboard(limit = 30) {
  const r = await pool.query<UserRow>(
    `
    SELECT address, total_calories, best_seconds, total_miles
    FROM users
    ORDER BY total_calories DESC, best_seconds DESC
    LIMIT $1
    `,
    [limit]
  );
  return r.rows;
}