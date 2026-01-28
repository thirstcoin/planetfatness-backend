// src/db.ts
import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL || "";
if (!DATABASE_URL) {
  console.warn("⚠️ Missing DATABASE_URL (set it in Render env vars). DB calls will fail.");
}

export const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost") ? undefined : { rejectUnauthorized: false },
});

// -------------------------------
// Schema bootstrap
// -------------------------------
export async function initDb() {
  // Users table = lifetime rollups
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      address TEXT PRIMARY KEY,
      display_name TEXT,
      total_calories BIGINT NOT NULL DEFAULT 0,
      best_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
      total_miles DOUBLE PRECISION NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS display_name TEXT;
  `);

  // ✅ Telegram identity columns
  await pool.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS tg_id BIGINT,
    ADD COLUMN IF NOT EXISTS tg_username TEXT,
    ADD COLUMN IF NOT EXISTS tg_first_name TEXT,
    ADD COLUMN IF NOT EXISTS tg_last_name TEXT;
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_users_tg_username ON users(tg_username);`);

  // Sessions table = receipts (per-run logging)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      id BIGSERIAL PRIMARY KEY,
      address TEXT NOT NULL REFERENCES users(address) ON DELETE CASCADE,
      game TEXT NOT NULL DEFAULT 'unknown',
      calories INT NOT NULL DEFAULT 0,
      miles DOUBLE PRECISION NOT NULL DEFAULT 0,
      best_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
      score INT NOT NULL DEFAULT 0,
      duration_ms INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // ✅ NEW: streak column for games like basket (one miss ends run)
  await pool.query(`
    ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS streak INT NOT NULL DEFAULT 0;
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sessions_address_created ON sessions(address, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sessions_game_created ON sessions(game, created_at DESC);`);

  console.log("✅ DB ready");
}

// -------------------------------
// Users
// -------------------------------
export async function upsertUser(address: string) {
  const a = String(address || "").trim();
  if (!a) throw new Error("missing address");

  const r = await pool.query(
    `
    INSERT INTO users (address)
    VALUES ($1)
    ON CONFLICT (address) DO UPDATE
      SET updated_at = NOW()
    RETURNING *;
    `,
    [a]
  );

  return r.rows[0] || null;
}

export async function getMe(address: string) {
  const a = String(address || "").trim();
  const r = await pool.query(`SELECT * FROM users WHERE address=$1 LIMIT 1;`, [a]);
  return r.rows[0] || null;
}

export async function setDisplayName(params: { address: string; displayName: string }) {
  const address = String(params.address || "").trim();
  let displayName = String(params.displayName || "").trim();

  if (!address) throw new Error("missing address");

  displayName = displayName.replace(/\s+/g, " ").slice(0, 24);
  if (displayName.length < 2) throw new Error("displayName_too_short");

  await upsertUser(address);

  const r = await pool.query(
    `
    UPDATE users
    SET display_name = $2,
        updated_at = NOW()
    WHERE address = $1
    RETURNING *;
    `,
    [address, displayName]
  );

  return r.rows[0] || null;
}

export async function setTelegramIdentity(params: {
  address: string;
  tgId: number;
  tgUsername?: string | null;
  firstName?: string | null;
  lastName?: string | null;
}) {
  const address = String(params.address || "").trim();
  const tgId = Number(params.tgId || 0);

  const tgUsername = params.tgUsername ? String(params.tgUsername).trim().replace(/^@/, "") : null;
  const firstName = params.firstName ? String(params.firstName).trim() : null;
  const lastName = params.lastName ? String(params.lastName).trim() : null;

  if (!address) throw new Error("missing address");
  if (!tgId) throw new Error("missing tgId");

  const fallbackDisplayName =
    (tgUsername ? `@${tgUsername}` : null) ||
    ([firstName, lastName].filter(Boolean).join(" ").trim() || null) ||
    "Member";

  await upsertUser(address);

  const r = await pool.query(
    `
    UPDATE users
    SET
      tg_id = $2,
      tg_username = $3,
      tg_first_name = $4,
      tg_last_name = $5,
      display_name = COALESCE(NULLIF(display_name,''), $6),
      updated_at = NOW()
    WHERE address = $1
    RETURNING *;
    `,
    [address, tgId, tgUsername, firstName, lastName, fallbackDisplayName]
  );

  return r.rows[0] || null;
}

export async function addActivity(params: { address: string; addCalories: number; bestSeconds: number; addMiles: number }) {
  const address = String(params.address || "").trim();
  const addCalories = Math.max(0, Math.floor(Number(params.addCalories || 0)));
  const addMiles = Math.max(0, Number(params.addMiles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));

  if (!address) throw new Error("missing address");

  await upsertUser(address);

  const r = await pool.query(
    `
    UPDATE users
    SET
      total_calories = total_calories + $2,
      total_miles = total_miles + $3,
      best_seconds = GREATEST(best_seconds, $4),
      updated_at = NOW()
    WHERE address = $1
    RETURNING *;
    `,
    [address, addCalories, addMiles, bestSeconds]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Leaderboards (legacy)
// -------------------------------
export async function getLeaderboard(limit = 30) {
  const lim = Math.max(1, Math.min(200, Number(limit || 30)));
  const r = await pool.query(
    `
    SELECT address, display_name, total_calories, best_seconds, total_miles
    FROM users
    ORDER BY total_calories DESC, best_seconds DESC
    LIMIT $1;
    `,
    [lim]
  );
  return r.rows || [];
}

// -------------------------------
// Sessions receipts
// -------------------------------
export async function logSession(params: {
  address: string;
  game: string;
  calories: number;
  miles: number;
  bestSeconds: number;
  score: number;
  durationMs: number;
  streak?: number;
}) {
  const address = String(params.address || "").trim();
  const game = String(params.game || "unknown").trim().slice(0, 32);

  const calories = Math.max(0, Math.floor(Number(params.calories || 0)));
  const miles = Math.max(0, Number(params.miles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));
  const score = Math.max(0, Math.floor(Number(params.score || 0)));
  const durationMs = Math.max(0, Math.floor(Number(params.durationMs || 0)));
  const streak = Math.max(0, Math.floor(Number(params.streak || 0)));

  if (!address) throw new Error("missing address");

  await upsertUser(address);

  const r = await pool.query(
    `
    INSERT INTO sessions (address, game, calories, miles, best_seconds, score, duration_ms, streak)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    RETURNING *;
    `,
    [address, game, calories, miles, bestSeconds, score, durationMs, streak]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Leaderboard V2
// window: lifetime | day | week | month
// metric: calories | score | miles | duration | streak
// optional game filter
//
// ✅ Basket score should be BEST RUN (MAX(score))
// ✅ Basket streak should be BEST STREAK (MAX(streak))
// ✅ Optional grind stat: shots_made = SUM(score)
// -------------------------------
export async function getLeaderboardV2(params: {
  window: "lifetime" | "day" | "week" | "month" | string;
  metric: "calories" | "score" | "miles" | "duration" | "streak" | string;
  game?: string;
  limit?: number;
}) {
  const window = String(params.window || "lifetime");
  const metric = String(params.metric || "calories");
  const game = params.game ? String(params.game).trim().slice(0, 32) : undefined;
  const limit = Math.max(1, Math.min(200, Number(params.limit || 30)));

  let whereTime = "";
  if (window === "day") whereTime = `AND s.created_at >= date_trunc('day', NOW())`;
  if (window === "week") whereTime = `AND s.created_at >= date_trunc('week', NOW())`;
  if (window === "month") whereTime = `AND s.created_at >= date_trunc('month', NOW())`;

  const whereGame = game ? `AND s.game = $2` : "";
  const args: any[] = [limit];
  if (game) args.push(game);

  // lifetime user rollups for calories/miles remain fast + clean
  if (window === "lifetime" && (metric === "calories" || metric === "miles")) {
    const col = metric === "miles" ? "total_miles" : "total_calories";
    const r = await pool.query(
      `
      SELECT
        address,
        display_name,
        ${col} AS value,
        total_calories,
        total_miles,
        best_seconds
      FROM users
      ORDER BY ${col} DESC
      LIMIT $1;
      `,
      [limit]
    );
    return r.rows || [];
  }

  // Important: when game=basket, return "score" as MAX(score) so frontend never accidentally shows SUM(score).
  const isBasket = game === "basket";

  // value expression (metric-specific)
  let metricExpr = "SUM(s.calories)";
  if (metric === "score") metricExpr = isBasket ? "MAX(s.score)" : "SUM(s.score)";
  if (metric === "streak") metricExpr = "MAX(s.streak)";
  if (metric === "miles") metricExpr = "SUM(s.miles)";
  if (metric === "duration") metricExpr = "SUM(s.duration_ms)";

  // per-row fields
  const scoreField = isBasket ? "MAX(s.score)" : "SUM(s.score)";
  const streakField = "MAX(s.streak)";
  const shotsMadeField = "SUM(s.score)"; // grind stat (optional)

  const r = await pool.query(
    `
    SELECT
      s.address,
      u.display_name,

      (${metricExpr})::DOUBLE PRECISION AS value,

      SUM(s.calories)::DOUBLE PRECISION AS calories,
      SUM(s.miles)::DOUBLE PRECISION AS miles,
      (${scoreField})::DOUBLE PRECISION AS score,
      SUM(s.duration_ms)::DOUBLE PRECISION AS duration_ms,

      MAX(s.score)::DOUBLE PRECISION AS best_score,
      MAX(s.streak)::DOUBLE PRECISION AS best_streak,

      (${shotsMadeField})::DOUBLE PRECISION AS shots_made
    FROM sessions s
    LEFT JOIN users u ON u.address = s.address
    WHERE 1=1
      ${whereTime}
      ${whereGame}
    GROUP BY s.address, u.display_name
    ORDER BY value DESC
    LIMIT $1;
    `,
    args
  );

  return r.rows || [];
}

// -------------------------------
// Activity summary
// -------------------------------
export async function getActivitySummary(params: { address: string }) {
  const address = String(params.address || "").trim();
  if (!address) throw new Error("missing address");

  const me = await getMe(address);

  // score = total score across all sessions in window
  // best_score = best single-run score (what basket should display as “Score” if you want best-run)
  const day = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(score),0) AS score,
      COALESCE(MAX(score),0) AS best_score,
      COALESCE(MAX(streak),0) AS best_streak,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(SUM(duration_ms),0) AS duration_ms
    FROM sessions
    WHERE address=$1
      AND created_at >= date_trunc('day', NOW());
    `,
    [address]
  );

  const week = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(score),0) AS score,
      COALESCE(MAX(score),0) AS best_score,
      COALESCE(MAX(streak),0) AS best_streak,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(SUM(duration_ms),0) AS duration_ms
    FROM sessions
    WHERE address=$1
      AND created_at >= date_trunc('week', NOW());
    `,
    [address]
  );

  const month = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(score),0) AS score,
      COALESCE(MAX(score),0) AS best_score,
      COALESCE(MAX(streak),0) AS best_streak,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(SUM(duration_ms),0) AS duration_ms
    FROM sessions
    WHERE address=$1
      AND created_at >= date_trunc('month', NOW());
    `,
    [address]
  );

  const byGame = await pool.query(
    `
    SELECT
      game,
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(score),0) AS score,
      COALESCE(MAX(score),0) AS best_score,
      COALESCE(MAX(streak),0) AS best_streak,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(SUM(duration_ms),0) AS duration_ms
    FROM sessions
    WHERE address=$1
      AND created_at >= NOW() - INTERVAL '30 days'
    GROUP BY game
    ORDER BY calories DESC;
    `,
    [address]
  );

  const d = day.rows[0] || {};
  const w = week.rows[0] || {};
  const m = month.rows[0] || {};

  return {
    address,
    profile: {
      displayName: me?.display_name || null,
      tgId: me?.tg_id ?? null,
      tgUsername: me?.tg_username ?? null,
    },
    lifetime: {
      totalCalories: Number(me?.total_calories || 0),
      totalMiles: Number(me?.total_miles || 0),
      bestSeconds: Number(me?.best_seconds || 0),
    },
    today: {
      calories: Number(d.calories || 0),
      score: Number(d.score || 0),
      bestScore: Number(d.best_score || 0),
      bestStreak: Number(d.best_streak || 0),
      miles: Number(d.miles || 0),
      durationMs: Number(d.duration_ms || 0),
    },
    week: {
      calories: Number(w.calories || 0),
      score: Number(w.score || 0),
      bestScore: Number(w.best_score || 0),
      bestStreak: Number(w.best_streak || 0),
      miles: Number(w.miles || 0),
      durationMs: Number(w.duration_ms || 0),
    },
    month: {
      calories: Number(m.calories || 0),
      score: Number(m.score || 0),
      bestScore: Number(m.best_score || 0),
      bestStreak: Number(m.best_streak || 0),
      miles: Number(m.miles || 0),
      durationMs: Number(m.duration_ms || 0),
    },
    last30DaysByGame: (byGame.rows || []).map((r: any) => ({
      game: r.game,
      calories: Number(r.calories || 0),
      score: Number(r.score || 0),
      bestScore: Number(r.best_score || 0),
      bestStreak: Number(r.best_streak || 0),
      miles: Number(r.miles || 0),
      durationMs: Number(r.duration_ms || 0),
    })),
  };
}