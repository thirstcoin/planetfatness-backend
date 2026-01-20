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
  // Existing users table (lifetime rollups) — unchanged
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

  // NEW: sessions table (game receipts)
  // We store raw runs so we can build weekly/monthly/per-game leaderboards safely.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      id BIGSERIAL PRIMARY KEY,
      address TEXT NOT NULL REFERENCES users(address) ON DELETE CASCADE,
      game TEXT NOT NULL,                  -- runner | snack | lift | basket (free-form for now)
      calories BIGINT DEFAULT 0,            -- can be 0 for score-only games
      miles NUMERIC DEFAULT 0,              -- can be 0 for non-runner games
      best_seconds NUMERIC DEFAULT 0,       -- optional metric (e.g., runner best time)
      score NUMERIC DEFAULT 0,              -- optional score
      duration_ms BIGINT DEFAULT 0,         -- anti-cheat / sanity checks
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_addr_created_idx ON sessions(address, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_created_idx ON sessions(created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_game_created_idx ON sessions(game, created_at DESC);`);

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

export type SessionRow = {
  id: string | number;
  address: string;
  game: string;
  calories: string | number;
  miles: string | number;
  best_seconds: string | number;
  score: string | number;
  duration_ms: string | number;
  created_at: string;
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

/**
 * Existing behavior: update lifetime totals (kept exactly compatible)
 */
export async function addActivity(params: {
  address: string;
  addCalories?: number;
  bestSeconds?: number;
  addMiles?: number;
}) {
  const addCalories = Math.max(0, Math.floor(params.addCalories ?? 0));
  const addMiles = Math.max(0, Number(params.addMiles ?? 0));
  const bestSeconds = Number(params.bestSeconds ?? 0);

  // Ensure user exists
  await upsertUser(params.address);

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

/**
 * NEW: Log a session receipt (for weekly/monthly/per-game leaderboards)
 * This does NOT replace addActivity — it complements it.
 * You can call this from /activity/add (when you’re ready) or from a new endpoint later.
 */
export async function logSession(params: {
  address: string;
  game: string;
  calories?: number;
  miles?: number;
  bestSeconds?: number;
  score?: number;
  durationMs?: number;
  // optional: allow passing createdAt in future, but default is NOW() on DB
}) {
  await upsertUser(params.address);

  const game = String(params.game || "unknown").slice(0, 32);

  const calories = Math.max(0, Math.floor(Number(params.calories ?? 0)));
  const miles = Math.max(0, Number(params.miles ?? 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds ?? 0));
  const score = Math.max(0, Number(params.score ?? 0));
  const durationMs = Math.max(0, Math.floor(Number(params.durationMs ?? 0)));

  const r = await pool.query<SessionRow>(
    `
    INSERT INTO sessions (address, game, calories, miles, best_seconds, score, duration_ms)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    RETURNING id, address, game, calories, miles, best_seconds, score, duration_ms, created_at
    `,
    [params.address, game, calories, miles, bestSeconds, score, durationMs]
  );

  return r.rows[0];
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

/**
 * NEW: Flexible leaderboard from sessions (weekly/monthly/lifetime, per-game optional)
 *
 * window:
 *  - "weekly"  = last 7 days
 *  - "monthly" = last 30 days
 *  - "lifetime" = all time (from sessions)
 *
 * metric:
 *  - "calories" (sum)
 *  - "miles"    (sum)
 *  - "score"    (max)  -> best score per player
 *  - "bestSeconds" (max) (kept consistent with your current best_seconds logic)
 */
export async function getLeaderboardV2(params: {
  limit?: number;
  window?: "weekly" | "monthly" | "lifetime";
  metric?: "calories" | "miles" | "score" | "bestSeconds";
  game?: string; // optional filter
}) {
  const limit = Math.max(1, Math.min(200, Number(params.limit ?? 30)));
  const window = params.window ?? "lifetime";
  const metric = params.metric ?? "calories";
  const game = params.game ? String(params.game).slice(0, 32) : null;

  let sinceSql = "";
  if (window === "weekly") sinceSql = "AND s.created_at >= NOW() - INTERVAL '7 days'";
  else if (window === "monthly") sinceSql = "AND s.created_at >= NOW() - INTERVAL '30 days'";

  const gameSql = game ? "AND s.game = $2" : "";
  const bind: any[] = [limit];
  if (game) bind.push(game);

  // aggregate expressions
  const aggCalories = "COALESCE(SUM(s.calories),0) AS calories";
  const aggMiles = "COALESCE(SUM(s.miles),0) AS miles";
  const aggBestSeconds = "COALESCE(MAX(s.best_seconds),0) AS best_seconds";
  const aggScoreMax = "COALESCE(MAX(s.score),0) AS score";

  let orderExpr = "calories DESC";
  if (metric === "miles") orderExpr = "miles DESC";
  if (metric === "score") orderExpr = "score DESC";
  if (metric === "bestSeconds") orderExpr = "best_seconds DESC";

  const sql = `
    SELECT
      s.address,
      ${aggCalories},
      ${aggMiles},
      ${aggBestSeconds},
      ${aggScoreMax}
    FROM sessions s
    WHERE 1=1
      ${sinceSql}
      ${gameSql}
    GROUP BY s.address
    ORDER BY ${orderExpr}
    LIMIT $1
  `;

  const r = await pool.query(sql, bind);

  // normalize field names to match your existing response style
  return r.rows.map((row: any) => ({
    address: row.address,
    totalCalories: Number(row.calories || 0),
    totalMiles: Number(row.miles || 0),
    bestSeconds: Number(row.best_seconds || 0),
    score: Number(row.score || 0),
  }));
}

/**
 * NEW: Get a user's weekly/monthly/lifetime totals from sessions (and keep users table as lifetime truth too).
 * This is useful for a profile screen: /activity/summary
 */
export async function getActivitySummary(params: {
  address: string;
}) {
  const address = params.address;
  await upsertUser(address);

  const lifetime = await getMe(address);

  const weekly = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(MAX(best_seconds),0) AS best_seconds,
      COALESCE(MAX(score),0) AS score
    FROM sessions
    WHERE address=$1 AND created_at >= NOW() - INTERVAL '7 days'
    `,
    [address]
  );

  const monthly = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(MAX(best_seconds),0) AS best_seconds,
      COALESCE(MAX(score),0) AS score
    FROM sessions
    WHERE address=$1 AND created_at >= NOW() - INTERVAL '30 days'
    `,
    [address]
  );

  return {
    address,
    lifetime: {
      totalCalories: Number(lifetime?.total_calories || 0),
      totalMiles: Number(lifetime?.total_miles || 0),
      bestSeconds: Number(lifetime?.best_seconds || 0),
    },
    weekly: {
      calories: Number(weekly.rows[0]?.calories || 0),
      miles: Number(weekly.rows[0]?.miles || 0),
      bestSeconds: Number(weekly.rows[0]?.best_seconds || 0),
      score: Number(weekly.rows[0]?.score || 0),
    },
    monthly: {
      calories: Number(monthly.rows[0]?.calories || 0),
      miles: Number(monthly.rows[0]?.miles || 0),
      bestSeconds: Number(monthly.rows[0]?.best_seconds || 0),
      score: Number(monthly.rows[0]?.score || 0),
    },
  };
}