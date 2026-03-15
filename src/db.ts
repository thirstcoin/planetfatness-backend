import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL || "";
if (!DATABASE_URL) {
  console.warn("⚠️ Missing DATABASE_URL (set it in Render env vars). DB calls will fail.");
}

export const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost")
    ? undefined
    : { rejectUnauthorized: false },
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

  await pool.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS tg_id BIGINT,
    ADD COLUMN IF NOT EXISTS tg_username TEXT,
    ADD COLUMN IF NOT EXISTS tg_first_name TEXT,
    ADD COLUMN IF NOT EXISTS tg_last_name TEXT;
  `);

  await pool.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS lifetime_makes BIGINT NOT NULL DEFAULT 0;
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_users_tg_username ON users(tg_username);`);

  // Sessions table
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
      streak INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS streak INT NOT NULL DEFAULT 0;
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sessions_address_created ON sessions(address, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sessions_game_created ON sessions(game, created_at DESC);`);

  // Greed rounds
  await pool.query(`
    CREATE TABLE IF NOT EXISTS greed_rounds (
      id BIGSERIAL PRIMARY KEY,
      address TEXT NOT NULL REFERENCES users(address) ON DELETE CASCADE,
      wager BIGINT NOT NULL,
      net_stake BIGINT NOT NULL,
      poison_indices INTEGER[] NOT NULL,
      server_seed TEXT NOT NULL,
      commit_hash TEXT NOT NULL,
      nonce BIGINT NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'active',
      result TEXT,
      safe_clicks INT NOT NULL DEFAULT 0,
      current_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
      payout BIGINT NOT NULL DEFAULT 0,
      payout_status TEXT NOT NULL DEFAULT 'unpaid',
      jackpot_won BIGINT NOT NULL DEFAULT 0,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      is_processing BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at TIMESTAMPTZ,
      revealed_at TIMESTAMPTZ
    );
  `);

  await pool.query(`
    ALTER TABLE greed_rounds
    ADD COLUMN IF NOT EXISTS result TEXT,
    ADD COLUMN IF NOT EXISTS current_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS payout_status TEXT NOT NULL DEFAULT 'unpaid',
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS is_processing BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS revealed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS commit_hash TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS nonce BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS jackpot_won BIGINT NOT NULL DEFAULT 0;
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_greed_address ON greed_rounds(address);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_greed_status_created ON greed_rounds(status, created_at DESC);`);

  // Greed picks
  await pool.query(`
    CREATE TABLE IF NOT EXISTS greed_picks (
      id BIGSERIAL PRIMARY KEY,
      round_id BIGINT NOT NULL REFERENCES greed_rounds(id) ON DELETE CASCADE,
      donut_index INT NOT NULL,
      result TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(round_id, donut_index)
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_greed_picks_round ON greed_picks(round_id, created_at ASC);`);

  // Internal balances
  await pool.query(`
    CREATE TABLE IF NOT EXISTS balances (
      address TEXT PRIMARY KEY REFERENCES users(address) ON DELETE CASCADE,
      available_balance BIGINT NOT NULL DEFAULT 0,
      locked_balance BIGINT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Deposits
  await pool.query(`
    CREATE TABLE IF NOT EXISTS deposits (
      id BIGSERIAL PRIMARY KEY,
      address TEXT NOT NULL REFERENCES users(address) ON DELETE CASCADE,
      tx_signature TEXT NOT NULL UNIQUE,
      sender_wallet TEXT,
      token_mint TEXT,
      amount BIGINT NOT NULL,
      status TEXT NOT NULL DEFAULT 'credited',
      note TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_deposits_address_created ON deposits(address, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_deposits_status_created ON deposits(status, created_at DESC);`);

  // Withdrawals
  await pool.query(`
    CREATE TABLE IF NOT EXISTS withdrawals (
      id BIGSERIAL PRIMARY KEY,
      address TEXT NOT NULL REFERENCES users(address) ON DELETE CASCADE,
      destination_wallet TEXT NOT NULL,
      amount BIGINT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      tx_signature TEXT,
      note TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_address_created ON withdrawals(address, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_status_created ON withdrawals(status, created_at DESC);`);

  // Jackpot state
  await pool.query(`
    CREATE TABLE IF NOT EXISTS jackpot_state (
      key TEXT PRIMARY KEY,
      current_amount BIGINT NOT NULL DEFAULT 5000,
      reseed_amount BIGINT NOT NULL DEFAULT 5000,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    INSERT INTO jackpot_state (key, current_amount, reseed_amount)
    VALUES ('greed', 5000, 5000)
    ON CONFLICT (key) DO NOTHING;
  `);

  console.log("✅ DB ready (TG + games + Greed + balances + deposits + withdrawals + jackpot)");
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

  await pool.query(
    `
    INSERT INTO balances (address)
    VALUES ($1)
    ON CONFLICT (address) DO NOTHING;
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

export async function addActivity(params: {
  address: string;
  addCalories: number;
  bestSeconds: number;
  addMiles: number;
  addScore?: number;
}) {
  const address = String(params.address || "").trim();
  const addCalories = Math.max(0, Math.floor(Number(params.addCalories || 0)));
  const addMiles = Math.max(0, Number(params.addMiles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));
  const addScore = Math.max(0, Math.floor(Number(params.addScore || 0)));

  if (!address) throw new Error("missing address");

  await upsertUser(address);

  const r = await pool.query(
    `
    UPDATE users
    SET
      total_calories = total_calories + $2,
      total_miles = total_miles + $3,
      best_seconds = GREATEST(best_seconds, $4),
      lifetime_makes = lifetime_makes + $5,
      updated_at = NOW()
    WHERE address = $1
    RETURNING *;
    `,
    [address, addCalories, addMiles, bestSeconds, addScore]
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
  const args: Array<string | number> = [limit];
  if (game) args.push(game);

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

  const isBasket = game === "basket";

  let metricExpr = "SUM(s.calories)";
  if (metric === "score") metricExpr = isBasket ? "MAX(s.score)" : "SUM(s.score)";
  if (metric === "streak") metricExpr = "MAX(s.streak)";
  if (metric === "miles") metricExpr = "SUM(s.miles)";
  if (metric === "duration") metricExpr = "SUM(s.duration_ms)";

  const scoreField = isBasket ? "MAX(s.score)" : "SUM(s.score)";
  const shotsMadeField = "SUM(s.score)";

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
      lifetimeMakes: Number(me?.lifetime_makes || 0),
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

// -------------------------------
// Balance helpers
// -------------------------------
export async function getBalance(address: string) {
  const a = String(address || "").trim();
  if (!a) throw new Error("missing address");

  await upsertUser(a);

  const r = await pool.query(
    `
    SELECT *
    FROM balances
    WHERE address = $1
    LIMIT 1;
    `,
    [a]
  );

  return r.rows[0] || null;
}

export async function creditBalance(params: {
  address: string;
  amount: number;
}) {
  const address = String(params.address || "").trim();
  const amount = Math.max(0, Math.floor(Number(params.amount || 0)));
  if (!address) throw new Error("missing address");
  if (amount <= 0) throw new Error("invalid amount");

  await upsertUser(address);

  const r = await pool.query(
    `
    UPDATE balances
    SET
      available_balance = available_balance + $2,
      updated_at = NOW()
    WHERE address = $1
    RETURNING *;
    `,
    [address, amount]
  );

  return r.rows[0] || null;
}

export async function debitBalance(params: {
  address: string;
  amount: number;
}) {
  const address = String(params.address || "").trim();
  const amount = Math.max(0, Math.floor(Number(params.amount || 0)));
  if (!address) throw new Error("missing address");
  if (amount <= 0) throw new Error("invalid amount");

  await upsertUser(address);

  const r = await pool.query(
    `
    UPDATE balances
    SET
      available_balance = available_balance - $2,
      updated_at = NOW()
    WHERE address = $1
      AND available_balance >= $2
    RETURNING *;
    `,
    [address, amount]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Deposit helpers
// -------------------------------
export async function hasDepositTxSignature(txSignature: string) {
  const sig = String(txSignature || "").trim();
  if (!sig) return false;

  const r = await pool.query(
    `
    SELECT 1
    FROM deposits
    WHERE tx_signature = $1
    LIMIT 1;
    `,
    [sig]
  );

  return r.rowCount > 0;
}

export async function recordDeposit(params: {
  address: string;
  txSignature: string;
  senderWallet?: string | null;
  tokenMint?: string | null;
  amount: number;
  status?: string;
  note?: string | null;
}) {
  const address = String(params.address || "").trim();
  const txSignature = String(params.txSignature || "").trim();
  const senderWallet = params.senderWallet ? String(params.senderWallet).trim() : null;
  const tokenMint = params.tokenMint ? String(params.tokenMint).trim() : null;
  const amount = Math.max(0, Math.floor(Number(params.amount || 0)));
  const status = String(params.status || "credited").trim();
  const note = params.note ? String(params.note).trim() : null;

  if (!address) throw new Error("missing address");
  if (!txSignature) throw new Error("missing txSignature");
  if (amount <= 0) throw new Error("invalid amount");

  await upsertUser(address);

  const r = await pool.query(
    `
    INSERT INTO deposits (
      address,
      tx_signature,
      sender_wallet,
      token_mint,
      amount,
      status,
      note,
      created_at,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())
    ON CONFLICT (tx_signature) DO NOTHING
    RETURNING *;
    `,
    [address, txSignature, senderWallet, tokenMint, amount, status, note]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Withdrawal helpers
// -------------------------------
export async function createWithdrawal(params: {
  address: string;
  destinationWallet: string;
  amount: number;
  note?: string | null;
}) {
  const address = String(params.address || "").trim();
  const destinationWallet = String(params.destinationWallet || "").trim();
  const amount = Math.max(0, Math.floor(Number(params.amount || 0)));
  const note = params.note ? String(params.note).trim() : null;

  if (!address) throw new Error("missing address");
  if (!destinationWallet) throw new Error("missing destinationWallet");
  if (amount <= 0) throw new Error("invalid amount");

  await upsertUser(address);

  const r = await pool.query(
    `
    INSERT INTO withdrawals (
      address,
      destination_wallet,
      amount,
      status,
      note,
      created_at,
      updated_at
    )
    VALUES ($1,$2,$3,'pending',$4,NOW(),NOW())
    RETURNING *;
    `,
    [address, destinationWallet, amount, note]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Jackpot helpers
// -------------------------------
export async function getGreedJackpotState() {
  const r = await pool.query(
    `
    SELECT *
    FROM jackpot_state
    WHERE key = 'greed'
    LIMIT 1;
    `
  );
  return r.rows[0] || null;
}

export async function setGreedJackpotAmount(amount: number) {
  const nextAmount = Math.max(0, Math.floor(Number(amount || 0)));

  const r = await pool.query(
    `
    UPDATE jackpot_state
    SET
      current_amount = $1,
      updated_at = NOW()
    WHERE key = 'greed'
    RETURNING *;
    `,
    [nextAmount]
  );

  return r.rows[0] || null;
}

export async function addToGreedJackpot(amount: number) {
  const addAmount = Math.max(0, Math.floor(Number(amount || 0)));

  const r = await pool.query(
    `
    UPDATE jackpot_state
    SET
      current_amount = current_amount + $1,
      updated_at = NOW()
    WHERE key = 'greed'
    RETURNING *;
    `,
    [addAmount]
  );

  return r.rows[0] || null;
}

export async function reseedGreedJackpot() {
  const r = await pool.query(
    `
    UPDATE jackpot_state
    SET
      current_amount = reseed_amount,
      updated_at = NOW()
    WHERE key = 'greed'
    RETURNING *;
    `
  );

  return r.rows[0] || null;
}

// -------------------------------
// Greed logic
// -------------------------------
export async function createGreedRound(params: {
  address: string;
  wager: number;
  netStake: number;
  poisonIndices: number[];
  seed: string;
  commitHash: string;
  nonce?: number;
}) {
  await upsertUser(params.address);

  const nonce = Math.max(1, Math.floor(Number(params.nonce || 1)));

  const r = await pool.query(
    `
    INSERT INTO greed_rounds (
      address,
      wager,
      net_stake,
      poison_indices,
      server_seed,
      commit_hash,
      nonce,
      status,
      result,
      safe_clicks,
      current_multiplier,
      payout,
      payout_status,
      jackpot_won,
      is_active,
      is_processing,
      created_at,
      updated_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', NULL, 0, 1.0, 0, 'unpaid', 0, TRUE, FALSE, NOW(), NOW())
    RETURNING *;
    `,
    [
      params.address,
      Math.floor(params.wager),
      Math.floor(params.netStake),
      params.poisonIndices,
      params.seed,
      params.commitHash,
      nonce,
    ]
  );

  return r.rows[0] || null;
}

export async function getActiveGreedRound(address: string) {
  const r = await pool.query(
    `
    SELECT *
    FROM greed_rounds
    WHERE address = $1
      AND status = 'active'
      AND is_active = TRUE
    ORDER BY created_at DESC
    LIMIT 1;
    `,
    [address]
  );
  return r.rows[0] || null;
}

export async function getGreedRoundByIdForAddress(roundId: number, address: string) {
  const r = await pool.query(
    `
    SELECT *
    FROM greed_rounds
    WHERE id = $1
      AND address = $2
    LIMIT 1;
    `,
    [roundId, address]
  );
  return r.rows[0] || null;
}

export async function getGreedPickedIndices(roundId: number): Promise<number[]> {
  const r = await pool.query(
    `
    SELECT donut_index
    FROM greed_picks
    WHERE round_id = $1
    ORDER BY created_at ASC;
    `,
    [roundId]
  );

  return (r.rows || []).map((x: { donut_index: number }) => Number(x.donut_index));
}

export async function recordGreedPick(params: {
  roundId: number;
  donutIndex: number;
  result: "safe" | "poison";
}) {
  const r = await pool.query(
    `
    INSERT INTO greed_picks (round_id, donut_index, result)
    VALUES ($1, $2, $3)
    RETURNING *;
    `,
    [params.roundId, params.donutIndex, params.result]
  );
  return r.rows[0] || null;
}

// -------------------------------
// Greed action lock helpers
// -------------------------------
export async function acquireGreedRoundProcessingLock(params: {
  roundId: number;
  address: string;
}) {
  const r = await pool.query(
    `
    UPDATE greed_rounds
    SET
      is_processing = TRUE,
      updated_at = NOW()
    WHERE id = $1
      AND address = $2
      AND status = 'active'
      AND is_active = TRUE
      AND is_processing = FALSE
    RETURNING *;
    `,
    [params.roundId, params.address]
  );

  return r.rows[0] || null;
}

export async function releaseGreedRoundProcessingLock(params: {
  roundId: number;
  address: string;
}) {
  const r = await pool.query(
    `
    UPDATE greed_rounds
    SET
      is_processing = FALSE,
      updated_at = NOW()
    WHERE id = $1
      AND address = $2
    RETURNING *;
    `,
    [params.roundId, params.address]
  );

  return r.rows[0] || null;
}

export async function updateGreedRoundProgress(params: {
  roundId: number;
  address: string;
  safeClicks: number;
  currentMultiplier: number;
}) {
  const r = await pool.query(
    `
    UPDATE greed_rounds
    SET
      safe_clicks = $3,
      current_multiplier = $4,
      updated_at = NOW()
    WHERE id = $1
      AND address = $2
      AND status = 'active'
      AND is_active = TRUE
    RETURNING *;
    `,
    [params.roundId, params.address, params.safeClicks, params.currentMultiplier]
  );

  return r.rows[0] || null;
}

export async function closeGreedRoundAsPoison(params: {
  roundId: number;
  address: string;
  safeClicks: number;
  currentMultiplier: number;
}) {
  const r = await pool.query(
    `
    UPDATE greed_rounds
    SET
      safe_clicks = $3,
      current_multiplier = $4,
      status = 'closed',
      result = 'poison',
      payout = 0,
      payout_status = 'unpaid',
      jackpot_won = 0,
      is_active = FALSE,
      is_processing = FALSE,
      updated_at = NOW(),
      ended_at = NOW(),
      revealed_at = NOW()
    WHERE id = $1
      AND address = $2
      AND status = 'active'
      AND is_active = TRUE
    RETURNING *;
    `,
    [params.roundId, params.address, params.safeClicks, params.currentMultiplier]
  );

  return r.rows[0] || null;
}

export async function closeGreedRoundAsCashout(params: {
  roundId: number;
  address: string;
  safeClicks: number;
  currentMultiplier: number;
  payout: number;
  result: "cashout" | "perfect";
  jackpotWon?: number;
}) {
  const jackpotWon = Math.max(0, Math.floor(Number(params.jackpotWon || 0)));

  const r = await pool.query(
    `
    UPDATE greed_rounds
    SET
      safe_clicks = $3,
      current_multiplier = $4,
      status = 'closed',
      result = $5,
      payout = $6,
      payout_status = 'recorded',
      jackpot_won = $7,
      is_active = FALSE,
      is_processing = FALSE,
      updated_at = NOW(),
      ended_at = NOW(),
      revealed_at = NOW()
    WHERE id = $1
      AND address = $2
      AND status = 'active'
      AND is_active = TRUE
      AND payout_status = 'unpaid'
    RETURNING *;
    `,
    [
      params.roundId,
      params.address,
      params.safeClicks,
      params.currentMultiplier,
      params.result,
      Math.floor(params.payout),
      jackpotWon,
    ]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Greed leaderboards
// -------------------------------
function greedWindowClause(window: string) {
  if (window === "day") return `AND gr.created_at >= date_trunc('day', NOW())`;
  if (window === "week") return `AND gr.created_at >= date_trunc('week', NOW())`;
  if (window === "month") return `AND gr.created_at >= date_trunc('month', NOW())`;
  return "";
}

export async function getGreedLeaderboard(params: {
  board: "most_wagered" | "most_won" | "perfect_runs" | "biggest_cashout" | "top_glaze_sacrifices";
  window: "lifetime" | "day" | "week" | "month";
  limit?: number;
}) {
  const limit = Math.max(1, Math.min(200, Number(params.limit || 25)));
  const whereTime = greedWindowClause(params.window);

  let sql = "";

  if (params.board === "most_wagered") {
    sql = `
      SELECT
        gr.address,
        u.display_name,
        SUM(gr.wager)::BIGINT AS value
      FROM greed_rounds gr
      LEFT JOIN users u ON u.address = gr.address
      WHERE gr.status = 'closed'
        ${whereTime}
      GROUP BY gr.address, u.display_name
      ORDER BY value DESC
      LIMIT $1;
    `;
  } else if (params.board === "most_won") {
    sql = `
      SELECT
        gr.address,
        u.display_name,
        (COALESCE(SUM(gr.payout),0) - COALESCE(SUM(gr.wager),0))::BIGINT AS value
      FROM greed_rounds gr
      LEFT JOIN users u ON u.address = gr.address
      WHERE gr.status = 'closed'
        ${whereTime}
      GROUP BY gr.address, u.display_name
      ORDER BY value DESC
      LIMIT $1;
    `;
  } else if (params.board === "perfect_runs") {
    sql = `
      SELECT
        gr.address,
        u.display_name,
        COUNT(*)::BIGINT AS value
      FROM greed_rounds gr
      LEFT JOIN users u ON u.address = gr.address
      WHERE gr.status = 'closed'
        AND gr.result = 'perfect'
        ${whereTime}
      GROUP BY gr.address, u.display_name
      ORDER BY value DESC
      LIMIT $1;
    `;
  } else if (params.board === "biggest_cashout") {
    sql = `
      SELECT
        gr.address,
        u.display_name,
        MAX(gr.payout)::BIGINT AS value
      FROM greed_rounds gr
      LEFT JOIN users u ON u.address = gr.address
      WHERE gr.status = 'closed'
        AND gr.result IN ('cashout', 'perfect')
        ${whereTime}
      GROUP BY gr.address, u.display_name
      ORDER BY value DESC
      LIMIT $1;
    `;
  } else {
    sql = `
      SELECT
        gr.address,
        u.display_name,
        (COALESCE(SUM(gr.wager),0) - COALESCE(SUM(gr.payout),0))::BIGINT AS value
      FROM greed_rounds gr
      LEFT JOIN users u ON u.address = gr.address
      WHERE gr.status = 'closed'
        ${whereTime}
      GROUP BY gr.address, u.display_name
      HAVING (COALESCE(SUM(gr.wager),0) - COALESCE(SUM(gr.payout),0)) > 0
      ORDER BY value DESC
      LIMIT $1;
    `;
  }

  const r = await pool.query(sql, [limit]);
  return r.rows || [];
}

export async function getGreedFeed(limit = 20) {
  const lim = Math.max(1, Math.min(100, Number(limit || 20)));
  const r = await pool.query(
    `
    SELECT
      gr.id,
      gr.address,
      u.display_name,
      gr.wager,
      gr.payout,
      gr.jackpot_won,
      gr.safe_clicks,
      gr.current_multiplier,
      gr.result,
      gr.commit_hash,
      gr.server_seed,
      gr.nonce,
      gr.poison_indices,
      gr.created_at,
      gr.ended_at,
      gr.revealed_at
    FROM greed_rounds gr
    LEFT JOIN users u ON u.address = gr.address
    WHERE gr.status = 'closed'
    ORDER BY COALESCE(gr.ended_at, gr.created_at) DESC
    LIMIT $1;
    `,
    [lim]
  );

  return r.rows || [];
}