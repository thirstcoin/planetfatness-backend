// src/index.ts
// Planet Fatness Backend — Phase 2 hardened (plus profile + leaderboard windows)

import dotenv from "dotenv";
dotenv.config();

import type { Request, Response } from "express";
import { Telegraf } from "telegraf"; // ✅ Added for Telegram Game support
import jwt from "jsonwebtoken"; // ✅ Added: mint JWT for Telegram Game overlay without touching mini-app flow
import crypto from "crypto"; // ✅ Added for secure Greed seed generation

// --- Dynamic imports (so env is ready BEFORE db.ts reads DATABASE_URL) ---
const expressMod = await import("express");
const express = expressMod.default;

const corsMod = await import("cors");
const cors = corsMod.default;

const authMod = await import("./auth.js");
const authRouter = authMod.default;
const requireAuth = authMod.requireAuth as any;

const dbMod = await import("./db.js");
const {
  initDb,
  getMe,
  addActivity,
  getLeaderboard,
  logSession,
  getLeaderboardV2,
  getActivitySummary,
  setDisplayName,
  setTelegramIdentity, // ✅ Added: used by Telegram Game overlay
  upsertUser,          // ✅ Added: used by Telegram Game overlay
  pool,
  // 🍩 New Feed Your Greed Database Helpers
  createGreedRound,
  updateGreedStep,
  finishGreedRound,
  getActiveGreedRound
} = dbMod;

// -------------------------------
// App + config
// -------------------------------
const app = express();

const PORT = Number(process.env.PORT || 10000);
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

app.use(express.json({ limit: "1mb" }));
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN.split(",").map((s) => s.trim()),
    credentials: false,
  })
);

// -------------------------------
// Helpers: reward fairness + daily tracking
// -------------------------------
type GameKey = "runner" | "snack" | "lift" | "basket" | "greed";

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}

function asGameKey(x: any): GameKey {
  const g = String(x || "").toLowerCase().trim();
  if (g === "runner" || g === "snack" || g === "lift" || g === "basket" || g === "greed") return g as GameKey;
  return "snack";
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * ✅ Window aliases for frontend convenience
 * Hub can use: daily/weekly/monthly/lifetime
 * Backend internally uses: day/week/month/lifetime
 */
function normalizeWindow(w: any): "lifetime" | "day" | "week" | "month" {
  const s = String(w || "lifetime").toLowerCase().trim();
  if (s === "daily") return "day";
  if (s === "weekly") return "week";
  if (s === "monthly") return "month";
  if (s === "day" || s === "week" || s === "month" || s === "lifetime") return s;
  return "lifetime";
}

// ✅ Mint the SAME kind of JWT as auth.ts does, but ONLY for Telegram Game overlay.
// This does NOT change mini-app initData flow at all.
function signTokenForAddress(address: string) {
  const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
  if (!JWT_SECRET) throw new Error("missing_jwt_secret");
  return jwt.sign({ address }, JWT_SECRET, { expiresIn: "30d" });
}

// -------------------------------
// CONSISTENT CALORIE CAPS ACROSS ALL GAMES
// - Calories are protected by minDuration + per-minute cap + max-run cap + daily cap
// - Basket HIGH SCORE is NOT capped (no maxScorePerRun); only score-rate anti-cheat affects calories
// -------------------------------
const COMMON_RULES = {
  minDurationMs: 10_000, // must play 10s to earn cals
  maxRunCalories: 180, // max cals per run (same across games)
  dailyCapCalories: 1200, // max cals per day per game (same across games)
  cpmCap: 220, // calories-per-minute ceiling guardrail
};

const RULES: Record<
  string,
  {
    minDurationMs: number;
    maxRunCalories: number;
    dailyCapCalories: number;
    cpmCap: number;
    maxScorePerRun: number;
  }
> = {
  runner: { ...COMMON_RULES, maxScorePerRun: 0 },
  snack: { ...COMMON_RULES, maxScorePerRun: 0 },
  lift: { ...COMMON_RULES, maxScorePerRun: 0 },
  basket: { ...COMMON_RULES, maxScorePerRun: 0 }, // ✅ no score cap
};

const DAILY_GOALS: Record<string, { label: string; goal: number; metric: "score" | "miles" | "seconds" }> = {
  snack: { label: "Daily Goal", goal: 30, metric: "score" },
  runner: { label: "Daily Goal", goal: 1, metric: "miles" },
  lift: { label: "Daily Goal", goal: 50, metric: "score" },
  basket: { label: "Daily Goal", goal: 20, metric: "score" },
};

async function getTodayAgg(address: string, game: GameKey) {
  const r = await pool.query(
    `
    SELECT
      -- Only sum these if they happened TODAY
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN calories ELSE 0 END), 0) AS calories,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN miles ELSE 0 END), 0) AS miles,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN duration_ms ELSE 0 END), 0) AS duration_ms,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN score ELSE 0 END), 0) AS score,
      
      -- Look at your WHOLE history for the best score
      COALESCE(MAX(score), 0) AS best_score
    FROM sessions
    WHERE address=$1 AND game=$2
    `,
    [address, game]
  );

  const row = r.rows[0] || {};
  return {
    calories: Number(row.calories || 0),
    miles: Number(row.miles || 0),
    durationMs: Number(row.duration_ms || 0),
    score: Number(row.score || 0),
    bestScore: Number(row.best_score || 0),
  };
}

function computeEarnedCalories(params: { game: GameKey; score: number; miles: number; bestSeconds: number; durationMs: number }) {
  const { game } = params;

  // Greed game does not award calories directly through this path
  if (game === "greed") {
    return { earnedCalories: 0, reason: "ok" as const, normalized: params };
  }

  const rules = RULES[game];

  const durationMs = Math.max(0, Math.floor(Number(params.durationMs || 0)));
  const durationMin = durationMs / 60000;

  const score = Math.max(0, Number(params.score || 0));
  const miles = Math.max(0, Number(params.miles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));

  if (durationMs < rules.minDurationMs) {
    return { earnedCalories: 0, reason: "too_short" as const, normalized: { score, miles, bestSeconds, durationMs } };
  }

  // ✅ Anti-cheat: cap SCORE RATE (points per minute), NOT total score.
  if (game !== "runner") {
    const scorePerMin = durationMin > 0 ? score / durationMin : score;

    const SCORE_PER_MIN_CAP: Record<string, number> = {
      runner: 999999,
      snack: 520,
      lift: 450,
      basket: 360, // tune later from real data
    };

    if (scorePerMin > SCORE_PER_MIN_CAP[game]) {
      return {
        earnedCalories: 0,
        reason: "score_too_high" as const,
        normalized: { score, miles, bestSeconds, durationMs },
      };
    }
  }

  let base = 0;

  if (game === "snack") {
    base = score * 2.2;
  } else if (game === "basket") {
    base = score * 1.8;
  } else if (game === "lift") {
    base = score * 2.0;
  } else if (game === "runner") {
    base = miles * 110;
    if (!miles || miles <= 0) base = durationMin * 120;
  }

  const timeCap = durationMin * rules.cpmCap;
  const earnedCalories = Math.floor(clamp(base, 0, Math.min(timeCap, rules.maxRunCalories)));

  return { earnedCalories, reason: "ok" as const, normalized: { score, miles, bestSeconds, durationMs } };
}

function computeDailyGoalProgress(params: { game: GameKey; today: { score: number; miles: number; durationMs: number } }) {
  const g = DAILY_GOALS[params.game];
  if (!g) return { goal: 0, progress: 0, hit: false };

  let progress = 0;
  if (g.metric === "score") {
    progress = Math.floor(params.today.score || 0);
  }
  if (g.metric === "miles") {
    progress = Number(params.today.miles || 0);
  }
  if (g.metric === "seconds") {
    progress = Math.floor((params.today.durationMs || 0) / 1000);
  }

  const goal = g.goal;
  const hit = progress >= goal;
  return { goal, progress, hit };
}

// -------------------------------
// Routes
// -------------------------------
app.get("/", (_req: Request, res: Response) => {
  res
    .status(200)
    .type("text/plain")
    .send(
      [
        "Planet Fatness Backend ✅",
        "",
        "Endpoints:",
        "  GET  /health",
        "  POST /auth/*",
        "  GET  /profile/me           (auth)",
        "  POST /profile/name         (auth)",
        "  GET  /activity/me          (auth)",
        "  POST /activity/add         (auth)   [legacy + optional receipts]",
        "  POST /activity/submit      (auth)   [server-computed calories]",
        "  GET  /activity/summary     (auth)",
        "  GET  /daily/progress       (auth)",
        "  GET  /leaderboard",
        "  GET  /leaderboard/v2       (window=day|week|month|lifetime)",
        "  GET  /leaderboard/games    (window=day|week|month|lifetime)",
        "  --- GREED GAME ---",
        "  POST /greed/start          (auth)",
        "  POST /greed/step           (auth)",
        "  POST /greed/finish         (auth)",
        "  GET  /greed/active         (auth)",
        "",
        "tapping counts as cardio 🟣🟡",
      ].join("\n")
    );
});

app.get("/health", (_req: Request, res: Response) =>
  res.json({
    ok: true,
    service: "planetfatness-backend",
    ts: nowIso(),
  })
);

app.use("/auth", authRouter);

/**
 * GET /profile/me (auth)
 */
app.get("/profile/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;
  const me = await getMe(address);
  if (!me) return res.status(404).json({ error: "User not found" });

  res.json({
    address: me.address,
    displayName: me.display_name || null,
  });
});

/**
 * POST /profile/name (auth)
 * body: { displayName }
 */
app.post("/profile/name", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const displayName = String(req.body?.displayName || "").trim();

    const u = await setDisplayName({ address, displayName });
    if (!u) return res.status(500).json({ error: "Update failed" });

    res.json({ ok: true, address: u.address, displayName: u.display_name || null });
  } catch (e: any) {
    const msg = String(e?.message || "");
    if (msg === "displayName_too_short") return res.status(400).json({ error: "Name too short (min 2 chars)" });
    console.error(e);
    res.status(500).json({ error: "Name update failed" });
  }
});

/**
 * GET /activity/me (auth)
 */
app.get("/activity/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;
  const me = await getMe(address);
  if (!me) return res.status(404).json({ error: "User not found" });

  res.json({
    address: me.address,
    displayName: me.display_name || null,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

/**
 * GET /activity/summary (auth)
 */
app.get("/activity/summary", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const summary = await getActivitySummary({ address });
    res.json(summary);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Summary failed" });
  }
});

/**
 * GET /daily/progress (auth)
 */
app.get("/daily/progress", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;

    const games: GameKey[] = ["snack", "runner", "lift", "basket"];
    const out: any = { address, ts: nowIso(), games: {} as any };

    for (const g of games) {
      const today = await getTodayAgg(address, g);
      const rules = RULES[g];
      const goal = computeDailyGoalProgress({ game: g, today });

      out.games[g] = {
        today: {
          calories: today.calories,
          miles: today.miles,
          score: today.score,
          durationMs: today.durationMs,
          bestScore: today.bestScore,
        },
        goal,
        caps: {
          dailyCapCalories: rules.dailyCapCalories,
          remainingCalories: Math.max(0, rules.dailyCapCalories - today.calories),
        },
      };
    }

    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Daily progress failed" });
  }
});

/**
 * POST /activity/add (auth) — legacy compatible + optional receipts
 */
app.post("/activity/add", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;

  // --- Legacy fields ---
  const legacyAddCalories = Number(req.body?.addCalories ?? 0);
  const legacyBestSeconds = Number(req.body?.bestSeconds ?? 0);
  const legacyAddMiles = Number(req.body?.addMiles ?? 0);

  // --- Optional v2 receipt fields ---
  const game = req.body?.game ? String(req.body.game) : "";
  const v2Calories = req.body?.calories != null ? Number(req.body.calories) : undefined;
  const v2Miles = req.body?.miles != null ? Number(req.body.miles) : undefined;
  const v2BestSeconds = req.body?.bestSeconds != null ? Number(req.body.bestSeconds) : undefined;
  const v2Score = req.body?.score != null ? Number(req.body.score) : undefined;
  const v2DurationMs = req.body?.durationMs != null ? Number(req.body.durationMs) : undefined;
  const v2Streak = req.body?.streak != null ? Number(req.body.streak) : undefined; // ✅ NEW (optional)

  // Aliases (v2 wins if present)
  const finalAddCalories = Number.isFinite(v2Calories as any) ? Number(v2Calories) : legacyAddCalories;
  const finalAddMiles = Number.isFinite(v2Miles as any) ? Number(v2Miles) : legacyAddMiles;
  const finalBestSeconds = Number.isFinite(v2BestSeconds as any) ? Number(v2BestSeconds) : legacyBestSeconds;
  const finalScore = Number.isFinite(v2Score as any) ? Number(v2Score) : 0;
  const finalDurationMs = Number.isFinite(v2DurationMs as any) ? Math.max(0, Math.floor(Number(v2DurationMs))) : 0;
  const finalStreak = Number.isFinite(v2Streak as any) ? Math.max(0, Math.floor(Number(v2Streak))) : 0;

  // 1) Update lifetime rollups
  const me = await addActivity({
    address,
    addCalories: Number.isFinite(finalAddCalories) ? finalAddCalories : 0,
    bestSeconds: Number.isFinite(finalBestSeconds) ? finalBestSeconds : 0,
    addMiles: Number.isFinite(finalAddMiles) ? finalAddMiles : 0,
  });

  if (!me) return res.status(500).json({ error: "Update failed" });

  // 2) If it looks like a receipt, also log it
  const looksLikeReceipt =
    (!!game && game.length > 0) ||
    (Number.isFinite(finalScore as any) && finalScore > 0) ||
    (Number.isFinite(finalDurationMs as any) && finalDurationMs > 0);

  if (looksLikeReceipt) {
    const safeGame = (game || "unknown").slice(0, 32);

    try {
      await logSession({
        address,
        game: safeGame,
        calories: Math.max(0, Math.floor(finalAddCalories || 0)),
        miles: Math.max(0, Number(finalAddMiles || 0)),
        bestSeconds: Math.max(0, Number(finalBestSeconds || 0)),
        score: Math.max(0, Number(finalScore || 0)),
        streak: finalStreak, // ✅ NEW
        durationMs: Math.max(0, Math.floor(finalDurationMs || 0)),
      });
    } catch (e) {
      console.error("logSession failed:", e);
    }
  }

  res.json({
    address: me.address,
    displayName: (me as any).display_name || null,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

/**
 * POST /activity/submit (auth)
 * Server-computed calories + daily caps.
 */
app.post("/activity/submit", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;

    const game = asGameKey(req.body?.game);
    const score = Number(req.body?.score ?? 0);
    const miles = Number(req.body?.miles ?? 0);
    const bestSeconds = Number(req.body?.bestSeconds ?? 0);
    const durationMs = Number(req.body?.durationMs ?? 0);

    // ✅ NEW: optional streak (used by basket “one miss ends run”)
    const streak = Math.max(0, Math.floor(Number(req.body?.streak ?? 0) || 0));

    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      return res.status(400).json({ error: "Missing durationMs" });
    }

    // 1) compute earned calories (server side)
    const calc = computeEarnedCalories({
      game,
      score,
      miles,
      bestSeconds,
      durationMs,
    });

    // 2) apply daily per-game cap
    const todayBefore = await getTodayAgg(address, game);
    const rules = RULES[game] || COMMON_RULES;

    const remaining = Math.max(0, rules.dailyCapCalories - todayBefore.calories);
    const earnedCapped = Math.max(0, Math.min(calc.earnedCalories, remaining));

    // 3) log receipt (✅ includes streak)
    await logSession({
      address,
      game,
      calories: earnedCapped,
      miles: Math.max(0, miles || 0),
      bestSeconds: Math.max(0, bestSeconds || 0),
      score: Math.max(0, score || 0),
      streak, // ✅ NEW
      durationMs: Math.max(0, Math.floor(durationMs || 0)),
    });

    // 4) update lifetime rollups
    const me = await addActivity({
      address,
      addCalories: earnedCapped,
      bestSeconds: Math.max(0, bestSeconds || 0),
      addMiles: Math.max(0, miles || 0),
    });

    if (!me) return res.status(500).json({ error: "Update failed" });

    // 5) return daily progress snapshot
    const todayAfter = await getTodayAgg(address, game);
    const goal = computeDailyGoalProgress({ game, today: todayAfter });

    res.json({
      ok: true,
      address,
      game,
      earnedCalories: earnedCapped,
      reason: calc.reason,
      caps: {
        dailyCapCalories: rules.dailyCapCalories,
        remainingCalories: Math.max(0, rules.dailyCapCalories - todayAfter.calories),
      },
      today: {
        calories: todayAfter.calories,
        miles: todayAfter.miles,
        score: todayAfter.score,
        durationMs: todayAfter.durationMs,
        bestScore: todayAfter.bestScore,
      },
      goal,
      totals: {
        totalCalories: Number(me.total_calories || 0),
        bestSeconds: Number(me.best_seconds || 0),
        totalMiles: Number(me.total_miles || 0),
      },
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Submit failed" });
  }
});

// -------------------------------
// 🍩 FEED YOUR GREED API
// -------------------------------

/**
 * POST /greed/start
 * Starts a new round. Generates the "poison" locations server-side.
 */
app.post("/greed/start", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const { wager, netStake, mode } = req.body;

    // Check if already in a round
    const existing = await getActiveGreedRound(address);
    if (existing) {
      return res.status(400).json({ error: "Round already active", round: existing });
    }

    // Logic: 24 total slots. 
    // Classic = 1 poison. Chaos = 3 poisons.
    const poisonCount = mode === "chaos" ? 3 : 1;
    const poisonIndices: number[] = [];
    while (poisonIndices.length < poisonCount) {
      const r = Math.floor(Math.random() * 24);
      if (!poisonIndices.includes(r)) {
        poisonIndices.push(r);
      }
    }

    const seed = crypto.randomBytes(16).toString("hex");
    
    const round = await createGreedRound({
      address,
      wager: Number(wager),
      netStake: Number(netStake),
      mode,
      poisonIndices,
      seed
    });

    res.json({ ok: true, roundId: round.id, mode: round.mode });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Failed to start greed round" });
  }
});

/**
 * POST /greed/step
 * Called every time a user clicks a donut.
 */
app.post("/greed/step", requireAuth, async (req: Request, res: Response) => {
  try {
    const { roundId, safeClicks } = req.body;
    const round = await updateGreedStep(Number(roundId), Number(safeClicks));
    res.json({ ok: true, safeClicks: round.safe_clicks });
  } catch (e) {
    res.status(500).json({ error: "Greed step update failed" });
  }
});

/**
 * POST /greed/finish
 * Ends the game. Finalizes payout.
 */
app.post("/greed/finish", requireAuth, async (req: Request, res: Response) => {
  try {
    const { roundId, status, payout } = req.body;
    const round = await finishGreedRound(Number(roundId), status, Number(payout));
    
    // Log it as a session so it shows on leaderboards
    const address = (req as any).user?.address as string;
    await logSession({
      address,
      game: "greed",
      calories: 0, 
      miles: 0,
      bestSeconds: 0,
      score: status === 'won' ? Number(payout) : 0,
      durationMs: 0
    });

    res.json({ ok: true, status: round.status, payout: round.payout });
  } catch (e) {
    res.status(500).json({ error: "Greed finish failed" });
  }
});

app.get("/greed/active", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const round = await getActiveGreedRound(address);
    res.json({ active: !!round, round });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch active round" });
  }
});

/**
 * GET /leaderboard (legacy, lifetime users table)
 */
app.get("/leaderboard", async (_req: Request, res: Response) => {
  const top = await getLeaderboard(30);
  res.json(
    top.map((u: any) => ({
      address: u.address,
      displayName: u.display_name || null,
      totalCalories: Number(u.total_calories || 0),
      bestSeconds: Number(u.best_seconds || 0),
      totalMiles: Number(u.total_miles || 0),
    }))
  );
});

/**
 * GET /leaderboard/v2
 * query:
 * window = lifetime | day | week | month  (also accepts daily|weekly|monthly)
 * metric = calories | score | miles | duration | streak
 * game   = runner | snack | lift | basket | greed (optional)
 */
app.get("/leaderboard/v2", async (req: Request, res: Response) => {
  try {
    const window = normalizeWindow(req.query.window || "lifetime") as any;
    const metric = String(req.query.metric || "calories") as any;

    const gameRaw = req.query.game ? String(req.query.game) : undefined;
    const game = gameRaw ? asGameKey(gameRaw) : undefined;

    const limit = req.query.limit ? Number(req.query.limit) : 30;

    const rows = await getLeaderboardV2({ window, metric, game, limit });
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Leaderboard v2 failed" });
  }
});

/**
 * GET /leaderboard/games
 * One call returns Top N by SCORE for each game for a given window.
 */
app.get("/leaderboard/games", async (req: Request, res: Response) => {
  try {
    const window = normalizeWindow(req.query.window || "lifetime");
    const limit = req.query.limit ? Number(req.query.limit) : 3;

    const games: GameKey[] = ["runner", "snack", "lift", "basket", "greed"];

    const out: any = { ok: true, window, limit, games: {} as any };

    for (const g of games) {
      const rows = await getLeaderboardV2({
        window,
        metric: "score",
        game: g,
        limit: Math.max(1, Math.min(50, Number(limit) || 3)),
      });
      out.games[g] = rows;
    }

    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Leaderboard games failed" });
  }
});

/**
 * 🚀 FIXED ADMIN LAUNCH RESET
 * Clears EVERY stat shown on the Gym Card.
 */
app.get("/admin/launch-reset", async (req: Request, res: Response) => {
  const secret = req.query.secret;
  if (secret !== "launch2026") return res.status(403).send("Unauthorized");

  try {
    // 1. Reset ALL lifetime columns on the users table
    await pool.query(`
      UPDATE users 
      SET total_calories = 0, 
          total_miles = 0, 
          best_seconds = 0, 
          lifetime_makes = 0;
    `);

    // 2. Clear the sessions table (This wipes the Leaderboards & Daily/Weekly/Monthly)
    await pool.query(`TRUNCATE TABLE sessions CASCADE;`);

    // 3. Reset the bot-specific table
    await pool.query(`UPDATE pf_users SET total_calories = 0;`);

    res.type("text/plain").send("✅ Planet Fatness DEEP CLEANED! All stats, times, and miles are now 0.");
  } catch (err: any) {
    res.status(500).send("❌ Reset failed: " + err.message);
  }
});

// -------------------------------
// ✅ TELEGRAM BOT ENGINE
// Conflict-Proof: Uses GREED_BOT_TOKEN
// -------------------------------
const bot = new Telegraf(process.env.GREED_BOT_TOKEN || "");

// Listen for the /game command to send the official Game Banner
bot.command("game", async (ctx) => {
  // 'planetfatness' must match your BotFather Game Short Name exactly
  await ctx.replyWithGame("planetfatness");
});

// Handle the "Play" button clicks
bot.on("callback_query", async (ctx) => {
  try {
    const q = ctx.callbackQuery;
    if (!q || !("game_short_name" in q)) return;

    const user: any = ctx.from;
    const tgIdNum = Number(user?.id || 0);
    const username = user?.username ? String(user.username) : "";
    const firstName = user?.first_name ? String(user.first_name) : "";
    const lastName = user?.last_name ? String(user.last_name) : "";

    if (!tgIdNum) {
      await ctx.answerGameQuery("https://planetfatness.fit/");
      return;
    }

    const address = `tg:${tgIdNum}`;

    // ✅ Upsert + attach TG identity server-side
    try {
      await upsertUser(address);
      await setTelegramIdentity({
        address,
        tgId: tgIdNum,
        tgUsername: username || null,
        firstName: firstName || null,
        lastName: lastName || null,
      });

      const displayName =
        (username ? `@${username}` : "") ||
        ([firstName, lastName].filter(Boolean).join(" ").trim().slice(0, 24)) ||
        "Member";

      if (displayName && displayName.trim().length >= 2) {
        try {
          await setDisplayName({ address, displayName });
        } catch {
          // ignore
        }
      }
    } catch (e) {
      console.error("TG game upsert/identity failed:", e);
      await ctx.answerGameQuery("https://planetfatness.fit/");
      return;
    }

    // ✅ Mint real JWT so requireAuth routes work in overlay
    let token = "";
    try {
      token = signTokenForAddress(address);
    } catch (e) {
      console.error("TG game signToken failed:", e);
      await ctx.answerGameQuery("https://planetfatness.fit/");
      return;
    }

    // ✅ Generate Launch URL based on which game was clicked
    let launchUrl = `https://planetfatness.fit/?t=${encodeURIComponent(token)}`;
    
    // Check if the user specifically clicked the 'Greed' game card
    if (q.game_short_name === 'Greed') {
       launchUrl = `https://planetfatness.fit/greed?t=${encodeURIComponent(token)}`;
    }

    // Tells Telegram to open the URL as an overlay on top of the chat
    await ctx.answerGameQuery(launchUrl);
  } catch (e) {
    console.error("TG game callback handler error:", e);
    try {
      await ctx.answerGameQuery("https://planetfatness.fit/");
    } catch {}
  }
});

// -------------------------------
// Boot
// -------------------------------
try {
  await initDb();

  // Launch Bot Engine with Conflict Shield
  if (process.env.GREED_BOT_TOKEN) {
    bot.launch().then(() => console.log("🤖 Planet Fatness Greed Bot Engine Active"));
  } else {
    console.warn("⚠️ No GREED_BOT_TOKEN found in environment.");
  }

  app.listen(PORT, () => console.log(`✅ Planet Fatness backend on :${PORT}`));
} catch (e) {
  console.error("❌ Failed to boot:", e);
  process.exit(1);
}
