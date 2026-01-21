import pg from "pg";
import crypto from "crypto";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

export const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost") ? undefined : { rejectUnauthorized: false },
});

/**
 * Small helpers
 */
function randId(len = 10) {
  return crypto.randomBytes(Math.ceil(len / 2)).toString("hex").slice(0, len);
}

function normalizeName(name: string) {
  // allow letters, numbers, underscore, dash. 3-18 chars.
  const cleaned = String(name || "")
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 18);
  return cleaned;
}

async function columnExists(table: string, column: string) {
  const r = await pool.query(
    `
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name=$1
      AND column_name=$2
    LIMIT 1
  `,
    [table, column]
  );
  return r.rowCount > 0;
}

async function indexExists(indexName: string) {
  const r = await pool.query(
    `SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=$1 LIMIT 1`,
    [indexName]
  );
  return r.rowCount > 0;
}

export async function initDb() {
  // USERS: lifetime rollups + profile identity
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      address TEXT PRIMARY KEY,
      total_calories BIGINT DEFAULT 0,
      best_seconds NUMERIC DEFAULT 0,
      total_miles NUMERIC DEFAULT 0,

      -- ✅ NEW (profile identity)
      gym_id TEXT,
      display_name TEXT,

      -- ✅ NEW (airdrop points / tickets)
      airdrop_points BIGINT DEFAULT 0,

      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Ensure new columns exist even if users table was created earlier
  if (!(await columnExists("users", "gym_id"))) {
    await pool.query(`ALTER TABLE users ADD COLUMN gym_id TEXT;`);
  }
  if (!(await columnExists("users", "display_name"))) {
    await pool.query(`ALTER TABLE users ADD COLUMN display_name TEXT;`);
  }
  if (!(await columnExists("users", "airdrop_points"))) {
    await pool.query(`ALTER TABLE users ADD COLUMN airdrop_points BIGINT DEFAULT 0;`);
  }

  // Unique constraints for gym_id + display_name (safe create)
  // Postgres doesn't have IF NOT EXISTS for ADD CONSTRAINT in older versions, so we use indexes.
  if (!(await indexExists("users_gym_id_uq"))) {
    await pool.query(
      `CREATE UNIQUE INDEX users_gym_id_uq ON users (gym_id) WHERE gym_id IS NOT NULL;`
    );
  }
  if (!(await indexExists("users_display_name_uq"))) {
    await pool.query(
      `CREATE UNIQUE INDEX users_display_name_uq ON users (display_name) WHERE display_name IS NOT NULL;`
    );
  }

  // SESSIONS: per-run receipts
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      id BIGSERIAL PRIMARY KEY,
      address TEXT NOT NULL REFERENCES users(address) ON DELETE CASCADE,
      game TEXT NOT NULL,                  -- runner | snack | lift | basket | etc

      calories BIGINT DEFAULT 0,            -- base calories credited (usually 0 for tickets-only games)
      miles NUMERIC DEFAULT 0,
      best_seconds NUMERIC DEFAULT 0,
      score NUMERIC DEFAULT 0,
      duration_ms BIGINT DEFAULT 0,

      -- ✅ NEW: tickets receipts (airdrop points)
      base_tickets BIGINT DEFAULT 0,
      final_tickets BIGINT DEFAULT 0,
      multiplier NUMERIC DEFAULT 1,
      phat_balance BIGINT DEFAULT 0,

      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Ensure new session columns exist
  if (!(await columnExists("sessions", "base_tickets"))) {
    await pool.query(`ALTER TABLE sessions ADD COLUMN base_tickets BIGINT DEFAULT 0;`);
  }
  if (!(await columnExists("sessions", "final_tickets"))) {
    await pool.query(`ALTER TABLE sessions ADD COLUMN final_tickets BIGINT DEFAULT 0;`);
  }
  if (!(await columnExists("sessions", "multiplier"))) {
    await pool.query(`ALTER TABLE sessions ADD COLUMN multiplier NUMERIC DEFAULT 1;`);
  }
  if (!(await columnExists("sessions", "phat_balance"))) {
    await pool.query(`ALTER TABLE sessions ADD COLUMN phat_balance BIGINT DEFAULT 0;`);
  }

  // Indexes
  await pool.query(
    `CREATE INDEX IF NOT EXISTS sessions_addr_created_idx ON sessions(address, created_at DESC);`
  );
  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_created_idx ON sessions(created_at DESC);`);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS sessions_game_created_idx ON sessions(game, created_at DESC);`
  );

  // updated_at helper
  await pool
    .query(`
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = NOW();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `)
    .catch(() => {});

  await pool
    .query(`
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
  `)
    .catch(() => {});
}

export type UserRow = {
  address: string;
  total_calories: string | number;
  best_seconds: string | number;
  total_miles: string | number;

  gym_id?: string | null;
  display_name?: string | null;
  airdrop_points?: string | number;
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

  base_tickets?: string | number;
  final_tickets?: string | number;
  multiplier?: string | number;
  phat_balance?: string | number;

  created_at: string;
};

/**
 * Ensures user exists. (Back-compat)
 */
export async function upsertUser(address: string) {
  await pool.query(
    `INSERT INTO users (address) VALUES ($1)
     ON CONFLICT (address) DO NOTHING`,
    [address]
  );
}

/**
 * ✅ NEW: Ensure user has gym_id + default display_name
 * - gym_id = short stable unique ID
 * - display_name default = "LUNK_<gymid>" unless already set
 */
export async function ensureUserProfile(address: string) {
  await upsertUser(address);

  // Try a few times to avoid rare unique collisions
  for (let i = 0; i < 6; i++) {
    const me = await pool.query<UserRow>(
      `SELECT address, gym_id, display_name, airdrop_points, total_calories, best_seconds, total_miles
       FROM users WHERE address=$1`,
      [address]
    );
    const row = me.rows[0] || null;
    if (!row) return null;

    const hasGymId = !!row.gym_id;
    const hasName = !!row.display_name;

    if (hasGymId && hasName) return row;

    const gymId = hasGymId ? String(row.gym_id) : `PF${randId(10)}`; // e.g. PFa1b2c3d4e5
    const defaultName = `LUNK_${gymId.slice(-6)}`; // e.g. LUNK_c3d4e5

    try {
      await pool.query(
        `
        UPDATE users
        SET
          gym_id = COALESCE(gym_id, $2),
          display_name = COALESCE(display_name, $3)
        WHERE address=$1
        `,
        [address, gymId, defaultName]
      );
      // loop will re-fetch and return
    } catch {
      // collision on unique indexes -> retry with a different id
    }
  }

  // Final fetch even if name couldn't be set (extremely rare)
  const r = await pool.query<UserRow>(
    `SELECT address, gym_id, display_name, airdrop_points, total_calories, best_seconds, total_miles
     FROM users WHERE address=$1`,
    [address]
  );
  return r.rows[0] || null;
}

export async function getMe(address: string) {
  const r = await pool.query<UserRow>(
    `SELECT address, total_calories, best_seconds, total_miles, gym_id, display_name, airdrop_points
     FROM users WHERE address=$1`,
    [address]
  );
  return r.rows[0] || null;
}

/**
 * ✅ NEW: Profile payload (gymId + name) + lifetime stats
 */
export async function getProfile(address: string) {
  const row = await ensureUserProfile(address);
  if (!row) return null;

  return {
    address: row.address,
    gymId: row.gym_id || null,
    displayName: row.display_name || null,
    lifetime: {
      totalCalories: Number(row.total_calories || 0),
      totalMiles: Number(row.total_miles || 0),
      bestSeconds: Number(row.best_seconds || 0),
      tickets: Number((row.airdrop_points as any) || 0),
    },
  };
}

/**
 * ✅ NEW: Change display name (unique)
 */
export async function setDisplayName(params: { address: string; displayName: string }) {
  const address = params.address;
  await ensureUserProfile(address);

  const next = normalizeName(params.displayName);
  if (next.length < 3) {
    return { ok: false, error: "Name too short (min 3 chars)." };
  }

  try {
    await pool.query(`UPDATE users SET display_name=$2 WHERE address=$1`, [address, next]);
    const me = await getMe(address);
    return { ok: true, displayName: me?.display_name || next };
  } catch (e: any) {
    // likely unique violation
    return { ok: false, error: "Name already taken. Try another." };
  }
}

/**
 * Existing behavior: update lifetime totals (kept compatible)
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

  await ensureUserProfile(params.address);

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
 * ✅ NEW: Add tickets (airdrop points) to users table (lifetime tally)
 * You will call this from the server after applying multiplier.
 */
export async function addTickets(params: { address: string; addTickets: number }) {
  const add = Math.max(0, Math.floor(Number(params.addTickets || 0)));
  await ensureUserProfile(params.address);

  await pool.query(
    `
    UPDATE users
    SET airdrop_points = COALESCE(airdrop_points,0) + $2
    WHERE address=$1
    `,
    [params.address, add]
  );

  const me = await getMe(params.address);
  return {
    address: params.address,
    tickets: Number((me?.airdrop_points as any) || 0),
    added: add,
  };
}

/**
 * Log a session receipt (kept compatible; now supports tickets fields too)
 */
export async function logSession(params: {
  address: string;
  game: string;
  calories?: number;
  miles?: number;
  bestSeconds?: number;
  score?: number;
  durationMs?: number;

  // ✅ NEW optional receipt fields
  baseTickets?: number;
  finalTickets?: number;
  multiplier?: number;
  phatBalance?: number;
}) {
  await ensureUserProfile(params.address);

  const game = String(params.game || "unknown").slice(0, 32);

  const calories = Math.max(0, Math.floor(Number(params.calories ?? 0)));
  const miles = Math.max(0, Number(params.miles ?? 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds ?? 0));
  const score = Math.max(0, Number(params.score ?? 0));
  const durationMs = Math.max(0, Math.floor(Number(params.durationMs ?? 0)));

  const baseTickets = Math.max(0, Math.floor(Number(params.baseTickets ?? 0)));
  const finalTickets = Math.max(0, Math.floor(Number(params.finalTickets ?? 0)));
  const multiplier = Math.max(0, Number(params.multiplier ?? 1));
  const phatBalance = Math.max(0, Math.floor(Number(params.phatBalance ?? 0)));

  const r = await pool.query<SessionRow>(
    `
    INSERT INTO sessions (
      address, game, calories, miles, best_seconds, score, duration_ms,
      base_tickets, final_tickets, multiplier, phat_balance
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    RETURNING
      id, address, game, calories, miles, best_seconds, score, duration_ms,
      base_tickets, final_tickets, multiplier, phat_balance,
      created_at
    `,
    [
      params.address,
      game,
      calories,
      miles,
      bestSeconds,
      score,
      durationMs,
      baseTickets,
      finalTickets,
      multiplier,
      phatBalance,
    ]
  );

  return r.rows[0];
}

export async function getLeaderboard(limit = 30) {
  const r = await pool.query<UserRow>(
    `
    SELECT address, total_calories, best_seconds, total_miles, gym_id, display_name, airdrop_points
    FROM users
    ORDER BY total_calories DESC, best_seconds DESC
    LIMIT $1
    `,
    [limit]
  );
  return r.rows;
}

/**
 * ✅ NEW: Tickets leaderboard (lifetime)
 */
export async function getTicketsLeaderboard(limit = 30) {
  const lim = Math.max(1, Math.min(200, Number(limit || 30)));
  const r = await pool.query(
    `
    SELECT address, gym_id, display_name, COALESCE(airdrop_points,0) AS tickets
    FROM users
    ORDER BY COALESCE(airdrop_points,0) DESC
    LIMIT $1
    `,
    [lim]
  );

  return r.rows.map((x: any) => ({
    address: x.address,
    gymId: x.gym_id || null,
    displayName: x.display_name || null,
    tickets: Number(x.tickets || 0),
  }));
}

/**
 * Flexible leaderboard from sessions (extended to support tickets)
 *
 * window:
 *  - "weekly"  = last 7 days
 *  - "monthly" = last 30 days
 *  - "lifetime" = all time
 *
 * metric:
 *  - "calories" (sum)
 *  - "miles"    (sum)
 *  - "score"    (max)
 *  - "bestSeconds" (max)
 *  - ✅ "tickets" (sum final_tickets)
 */
export async function getLeaderboardV2(params: {
  limit?: number;
  window?: "weekly" | "monthly" | "lifetime";
  metric?: "calories" | "miles" | "score" | "bestSeconds" | "tickets";
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

  const aggCalories = "COALESCE(SUM(s.calories),0) AS calories";
  const aggMiles = "COALESCE(SUM(s.miles),0) AS miles";
  const aggBestSeconds = "COALESCE(MAX(s.best_seconds),0) AS best_seconds";
  const aggScoreMax = "COALESCE(MAX(s.score),0) AS score";
  const aggTickets = "COALESCE(SUM(s.final_tickets),0) AS tickets";

  let orderExpr = "calories DESC";
  if (metric === "miles") orderExpr = "miles DESC";
  if (metric === "score") orderExpr = "score DESC";
  if (metric === "bestSeconds") orderExpr = "best_seconds DESC";
  if (metric === "tickets") orderExpr = "tickets DESC";

  const sql = `
    SELECT
      s.address,
      ${aggCalories},
      ${aggMiles},
      ${aggBestSeconds},
      ${aggScoreMax},
      ${aggTickets}
    FROM sessions s
    WHERE 1=1
      ${sinceSql}
      ${gameSql}
    GROUP BY s.address
    ORDER BY ${orderExpr}
    LIMIT $1
  `;

  const r = await pool.query(sql, bind);

  return r.rows.map((row: any) => ({
    address: row.address,
    totalCalories: Number(row.calories || 0),
    totalMiles: Number(row.miles || 0),
    bestSeconds: Number(row.best_seconds || 0),
    score: Number(row.score || 0),
    tickets: Number(row.tickets || 0),
  }));
}

/**
 * User summary (weekly/monthly totals) + lifetime from users table
 * Extended with tickets totals from sessions.
 */
export async function getActivitySummary(params: { address: string }) {
  const address = params.address;
  await ensureUserProfile(address);

  const lifetime = await getMe(address);

  const weekly = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(MAX(best_seconds),0) AS best_seconds,
      COALESCE(MAX(score),0) AS score,
      COALESCE(SUM(final_tickets),0) AS tickets
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
      COALESCE(MAX(score),0) AS score,
      COALESCE(SUM(final_tickets),0) AS tickets
    FROM sessions
    WHERE address=$1 AND created_at >= NOW() - INTERVAL '30 days'
    `,
    [address]
  );

  return {
    address,
    profile: {
      gymId: (lifetime as any)?.gym_id || null,
      displayName: (lifetime as any)?.display_name || null,
    },
    lifetime: {
      totalCalories: Number(lifetime?.total_calories || 0),
      totalMiles: Number(lifetime?.total_miles || 0),
      bestSeconds: Number(lifetime?.best_seconds || 0),
      tickets: Number((lifetime as any)?.airdrop_points || 0),
    },
    weekly: {
      calories: Number(weekly.rows[0]?.calories || 0),
      miles: Number(weekly.rows[0]?.miles || 0),
      bestSeconds: Number(weekly.rows[0]?.best_seconds || 0),
      score: Number(weekly.rows[0]?.score || 0),
      tickets: Number(weekly.rows[0]?.tickets || 0),
    },
    monthly: {
      calories: Number(monthly.rows[0]?.calories || 0),
      miles: Number(monthly.rows[0]?.miles || 0),
      bestSeconds: Number(monthly.rows[0]?.best_seconds || 0),
      score: Number(monthly.rows[0]?.score || 0),
      tickets: Number(monthly.rows[0]?.tickets || 0),
    },
  };
}