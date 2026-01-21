import pg from "pg";
import crypto from "crypto";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");

export const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost") ? undefined : { rejectUnauthorized: false },
});

function randId(len = 10) {
  return crypto.randomBytes(Math.ceil(len / 2)).toString("hex").slice(0, len);
}

function normalizeName(name: string) {
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
  // USERS: lifetime rollups + profile identity + airdrop points (tickets)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      address TEXT PRIMARY KEY,
      total_calories BIGINT DEFAULT 0,
      best_seconds NUMERIC DEFAULT 0,
      total_miles NUMERIC DEFAULT 0,

      gym_id TEXT,
      display_name TEXT,

      airdrop_points BIGINT DEFAULT 0,

      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  if (!(await columnExists("users", "gym_id"))) {
    await pool.query(`ALTER TABLE users ADD COLUMN gym_id TEXT;`);
  }
  if (!(await columnExists("users", "display_name"))) {
    await pool.query(`ALTER TABLE users ADD COLUMN display_name TEXT;`);
  }
  if (!(await columnExists("users", "airdrop_points"))) {
    await pool.query(`ALTER TABLE users ADD COLUMN airdrop_points BIGINT DEFAULT 0;`);
  }

  // Unique indexes (safe)
  if (!(await indexExists("users_gym_id_uq"))) {
    await pool.query(`CREATE UNIQUE INDEX users_gym_id_uq ON users (gym_id) WHERE gym_id IS NOT NULL;`);
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
      game TEXT NOT NULL,                  -- runner | snack | lift | basket

      calories BIGINT DEFAULT 0,
      miles NUMERIC DEFAULT 0,
      best_seconds NUMERIC DEFAULT 0,
      score NUMERIC DEFAULT 0,
      duration_ms BIGINT DEFAULT 0,

      -- tickets receipt fields (for future PHAT multiplier)
      base_tickets BIGINT DEFAULT 0,
      final_tickets BIGINT DEFAULT 0,
      multiplier NUMERIC DEFAULT 1,
      phat_balance BIGINT DEFAULT 0,

      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Ensure session new cols
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
  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_addr_created_idx ON sessions(address, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_created_idx ON sessions(created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS sessions_game_created_idx ON sessions(game, created_at DESC);`);

  // AUTH NONCES: wallet login (message + nonce) with expiry
  await pool.query(`
    CREATE TABLE IF NOT EXISTS auth_nonces (
      address TEXT PRIMARY KEY REFERENCES users(address) ON DELETE CASCADE,
      nonce TEXT NOT NULL,
      message TEXT NOT NULL,
      expires_at BIGINT NOT NULL
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS auth_nonces_expires_idx ON auth_nonces(expires_at);`);

  // DAILY CAPS: track per-day credited calories (so server enforces cap)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_caps (
      address TEXT PRIMARY KEY REFERENCES users(address) ON DELETE CASCADE,
      day_key TEXT NOT NULL,            -- e.g., "2026-01-21"
      today BIGINT DEFAULT 0,
      reset_at BIGINT NOT NULL
    );
  `);

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
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'users_set_updated_at') THEN
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

export type DailyRow = {
  address: string;
  day_key: string;
  today: string | number;
  reset_at: string | number;
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

export async function upsertUser(address: string) {
  await pool.query(`INSERT INTO users (address) VALUES ($1) ON CONFLICT (address) DO NOTHING`, [address]);
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
 * Creates/ensures gym_id + default display_name.
 * This matches your server import: ensureProfile()
 */
export async function ensureProfile(address: string) {
  await upsertUser(address);

  for (let i = 0; i < 6; i++) {
    const me = await getMe(address);
    if (!me) return null;

    const hasGymId = !!me.gym_id;
    const hasName = !!me.display_name;
    if (hasGymId && hasName) return me;

    const gymId = hasGymId ? String(me.gym_id) : `PF${randId(10)}`;
    const defaultName = `LUNK_${gymId.slice(-6)}`;

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
    } catch {
      // collision on unique indexes -> retry
    }
  }

  return getMe(address);
}

/**
 * Unique display name setter.
 * Server expects: setDisplayName(address, displayName) -> { ok, reason?, profile? }
 */
export async function setDisplayName(address: string, displayName: string) {
  await ensureProfile(address);

  const next = normalizeName(displayName);
  if (next.length < 3) return { ok: false, reason: "NAME_TOO_SHORT" };

  try {
    await pool.query(`UPDATE users SET display_name=$2 WHERE address=$1`, [address, next]);
    const profile = await getMe(address);
    return { ok: true, profile };
  } catch {
    return { ok: false, reason: "NAME_TAKEN" };
  }
}

/**
 * AUTH NONCE storage
 */
export async function saveNonce(params: {
  address: string;
  nonce: string;
  message: string;
  expiresAt: number;
}) {
  await ensureProfile(params.address);

  await pool.query(
    `
    INSERT INTO auth_nonces (address, nonce, message, expires_at)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (address) DO UPDATE SET
      nonce=EXCLUDED.nonce,
      message=EXCLUDED.message,
      expires_at=EXCLUDED.expires_at
    `,
    [params.address, params.nonce, params.message, params.expiresAt]
  );
}

/**
 * Consume nonce (one-time). Returns { ok, message?, reason? }
 */
export async function consumeNonce(address: string) {
  const r = await pool.query(
    `SELECT nonce, message, expires_at FROM auth_nonces WHERE address=$1`,
    [address]
  );
  const row = r.rows[0];
  if (!row) return { ok: false as const, reason: "NO_NONCE" };

  const expiresAt = Number(row.expires_at || 0);
  if (Date.now() > expiresAt) {
    await pool.query(`DELETE FROM auth_nonces WHERE address=$1`, [address]);
    return { ok: false as const, reason: "NONCE_EXPIRED" };
  }

  // delete so it can't be reused
  await pool.query(`DELETE FROM auth_nonces WHERE address=$1`, [address]);

  return { ok: true as const, message: String(row.message) };
}

/**
 * DAILY CAP tracking (server enforced)
 * We keep a row per address with a day_key + reset timestamp.
 */
function dayKeyUTC(ts = Date.now()) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}
function nextResetUTC(ts = Date.now()) {
  const d = new Date(ts);
  // next UTC midnight
  const next = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, 0, 0, 0, 0);
  return next;
}

export async function getDaily(address: string) {
  await ensureProfile(address);

  const now = Date.now();
  const key = dayKeyUTC(now);
  const resetAt = nextResetUTC(now);

  const r = await pool.query<DailyRow>(
    `SELECT address, day_key, today, reset_at FROM daily_caps WHERE address=$1`,
    [address]
  );

  if (!r.rows[0]) {
    await pool.query(
      `INSERT INTO daily_caps (address, day_key, today, reset_at) VALUES ($1,$2,$3,$4)`,
      [address, key, 0, resetAt]
    );
    return { today: 0, resetAt };
  }

  const row = r.rows[0];
  if (String(row.day_key) !== key || Number(row.reset_at || 0) <= now) {
    await pool.query(
      `UPDATE daily_caps SET day_key=$2, today=$3, reset_at=$4 WHERE address=$1`,
      [address, key, 0, resetAt]
    );
    return { today: 0, resetAt };
  }

  return { today: Number(row.today || 0), resetAt: Number(row.reset_at || resetAt) };
}

export async function addDailyCalories(address: string, add: number) {
  await ensureProfile(address);
  const daily = await getDaily(address);
  const nextToday = Math.max(0, daily.today + Math.max(0, Math.floor(add || 0)));

  await pool.query(
    `UPDATE daily_caps SET today=$2 WHERE address=$1`,
    [address, nextToday]
  );

  return { today: nextToday, resetAt: daily.resetAt };
}

/**
 * Existing behavior: lifetime totals (kept compatible)
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

  await ensureProfile(params.address);

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
 * Session receipt logging (compatible with your server call)
 */
export async function logSession(params: {
  address: string;
  game: string;
  calories?: number;
  miles?: number;
  bestSeconds?: number;
  score?: number;
  durationMs?: number;

  baseTickets?: number;
  finalTickets?: number;
  multiplier?: number;
  phatBalance?: number;
}) {
  await ensureProfile(params.address);

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
    RETURNING id, address, game, calories, miles, best_seconds, score, duration_ms,
              base_tickets, final_tickets, multiplier, phat_balance, created_at
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

/**
 * Compatibility exports (you imported these but aren’t using them yet)
 * If you don't need them, you can remove from server imports later.
 */
export async function getDailyCalories(address: string) {
  return getDaily(address);
}

/**
 * You had these names in your import list (but server uses getDaily/addDailyCalories).
 * Keeping aliases avoids confusion.
 */
export const getDaily_legacy = getDaily;
export const addDailyCalories_legacy = addDailyCalories;