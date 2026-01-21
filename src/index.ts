// src/index.ts
// Planet Fatness Backend — Phase 2 hardened
// ✅ Loads env BEFORE importing db/auth (critical for Render + ESM)
// ✅ Fixes /activity/add aliasing (bestSeconds + v2 fields)
// ✅ Keeps your existing routes + behavior intact

import dotenv from "dotenv";
dotenv.config();

import type { Request, Response } from "express";

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
  pool,
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
type GameKey = "runner" | "snack" | "lift" | "basket";

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}

function asGameKey(x: any): GameKey {
  const g = String(x || "").toLowerCase().trim();
  if (g === "runner" || g === "snack" || g === "lift" || g === "basket") return g;
  return "snack";
}

function nowIso() {
  return new Date().toISOString();
}

// Per-game guardrails (tune later)
const RULES: Record<
  GameKey,
  {
    minDurationMs: number;
    maxRunCalories: number;
    dailyCapCalories: number;
    cpmCap: number;
    maxScorePerRun: number;
  }
> = {
  runner: { minDurationMs: 12_000, maxRunCalories: 260, dailyCapCalories: 1_600, cpmCap: 220, maxScorePerRun: 0 },
  snack:  { minDurationMs: 10_000, maxRunCalories: 190, dailyCapCalories: 1_200, cpmCap: 190, maxScorePerRun: 220 },
  lift:   { minDurationMs: 10_000, maxRunCalories: 210, dailyCapCalories: 1_250, cpmCap: 200, maxScorePerRun: 260 },
  basket: { minDurationMs: 10_000, maxRunCalories: 170, dailyCapCalories: 950,  cpmCap: 175, maxScorePerRun: 260 },
};

const DAILY_GOALS: Record<GameKey, { label: string; goal: number; metric: "score" | "miles" | "seconds" }> = {
  snack:  { label: "Daily Goal", goal: 30, metric: "score" },
  runner: { label: "Daily Goal", goal: 1,  metric: "miles" },
  lift:   { label: "Daily Goal", goal: 50, metric: "score" },
  basket: { label: "Daily Goal", goal: 20, metric: "score" },
};

async function getTodayAgg(address: string, game: GameKey) {
  const r = await pool.query(
    `
    SELECT
      COALESCE(SUM(calories),0) AS calories,
      COALESCE(SUM(miles),0) AS miles,
      COALESCE(SUM(duration_ms),0) AS duration_ms,
      COALESCE(SUM(score),0) AS score,
      COALESCE(MAX(score),0) AS best_score
    FROM sessions
    WHERE address=$1
      AND game=$2
      AND created_at >= date_trunc('day', NOW())
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

function computeEarnedCalories(params: {
  game: GameKey;
  score: number;
  miles: number;
  bestSeconds: number;
  durationMs: number;
}) {
  const { game } = params;
  const rules = RULES[game];

  const durationMs = Math.max(0, Math.floor(Number(params.durationMs || 0)));
  const durationMin = durationMs / 60000;

  const score = Math.max(0, Number(params.score || 0));
  const miles = Math.max(0, Number(params.miles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));

  if (durationMs < rules.minDurationMs) {
    return { earnedCalories: 0, reason: "too_short" as const, normalized: { score, miles, bestSeconds, durationMs } };
  }

  if (rules.maxScorePerRun > 0 && score > rules.maxScorePerRun) {
    return { earnedCalories: 0, reason: "score_too_high" as const, normalized: { score, miles, bestSeconds, durationMs } };
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

  const timeCap = durationMin * RULES[game].cpmCap;
  const earnedCalories = Math.floor(clamp(base, 0, Math.min(timeCap, RULES[game].maxRunCalories)));

  return { earnedCalories, reason: "ok" as const, normalized: { score, miles, bestSeconds, durationMs } };
}

function computeDailyGoalProgress(params: {
  game: GameKey;
  today: { score: number; miles: number; durationMs: number };
}) {
  const g = DAILY_GOALS[params.game];
  if (!g) return { goal: 0, progress: 0, hit: false };

  let progress = 0;
  if (g.metric === "score") progress = Math.floor(params.today.score || 0);
  if (g.metric === "miles") progress = Number(params.today.miles || 0);
  if (g.metric === "seconds") progress = Math.floor((params.today.durationMs || 0) / 1000);

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
        "  GET  /activity/me          (auth)",
        "  POST /activity/add         (auth)   [legacy + optional receipts]",
        "  POST /activity/submit      (auth)   [NEW: server-computed calories]",
        "  GET  /activity/summary     (auth)   [NEW]",
        "  GET  /daily/progress       (auth)   [NEW]",
        "  GET  /leaderboard",
        "  GET  /leaderboard/v2       [NEW]",
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
 * GET /activity/me (auth)
 * returns: { address, totalCalories, bestSeconds, totalMiles }
 */
app.get("/activity/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;
  const me = await getMe(address);
  if (!me) return res.status(404).json({ error: "User not found" });

  res.json({
    address: me.address,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

/**
 * NEW: GET /activity/summary (auth)
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
 * NEW: GET /daily/progress (auth)
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

  // Aliases (v2 wins if present)
  const finalAddCalories = Number.isFinite(v2Calories as any) ? Number(v2Calories) : legacyAddCalories;
  const finalAddMiles = Number.isFinite(v2Miles as any) ? Number(v2Miles) : legacyAddMiles;
  const finalBestSeconds = Number.isFinite(v2BestSeconds as any) ? Number(v2BestSeconds) : legacyBestSeconds;
  const finalScore = Number.isFinite(v2Score as any) ? Number(v2Score) : 0;
  const finalDurationMs = Number.isFinite(v2DurationMs as any) ? Math.max(0, Math.floor(Number(v2DurationMs))) : 0;

  // 1) Update lifetime rollups (same as before)
  const me = await addActivity({
    address,
    addCalories: Number.isFinite(finalAddCalories) ? finalAddCalories : 0,
    bestSeconds: Number.isFinite(finalBestSeconds) ? finalBestSeconds : 0,
    addMiles: Number.isFinite(finalAddMiles) ? finalAddMiles : 0,
  });

  if (!me) return res.status(500).json({ error: "Update failed" });

  // 2) If it looks like a receipt, also log it (additive)
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
        durationMs: Math.max(0, Math.floor(finalDurationMs || 0)),
      });
    } catch (e) {
      console.error("logSession failed:", e);
    }
  }

  res.json({
    address: me.address,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

/**
 * NEW: POST /activity/submit (auth)
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
    const rules = RULES[game];

    const remaining = Math.max(0, rules.dailyCapCalories - todayBefore.calories);
    const earnedCapped = Math.max(0, Math.min(calc.earnedCalories, remaining));

    // 3) log receipt (always for submit)
    await logSession({
      address,
      game,
      calories: earnedCapped,
      miles: Math.max(0, miles || 0),
      bestSeconds: Math.max(0, bestSeconds || 0),
      score: Math.max(0, score || 0),
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

/**
 * GET /leaderboard (legacy, lifetime users table)
 */
app.get("/leaderboard", async (_req: Request, res: Response) => {
  const top = await getLeaderboard(30);
  res.json(
    top.map((u: any) => ({
      address: u.address,
      totalCalories: Number(u.total_calories || 0),
      bestSeconds: Number(u.best_seconds || 0),
      totalMiles: Number(u.total_miles || 0),
    }))
  );
});

/**
 * NEW: GET /leaderboard/v2
 */
app.get("/leaderboard/v2", async (req: Request, res: Response) => {
  try {
    const window = String(req.query.window || "lifetime") as any;
    const metric = String(req.query.metric || "calories") as any;
    const game = req.query.game ? String(req.query.game) : undefined;
    const limit = req.query.limit ? Number(req.query.limit) : 30;

    const rows = await getLeaderboardV2({ window, metric, game, limit });
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Leaderboard v2 failed" });
  }
});

// -------------------------------
// Boot
// -------------------------------
try {
  await initDb();
  app.listen(PORT, () => console.log(`✅ Planet Fatness backend on :${PORT}`));
} catch (e) {
  console.error("❌ Failed to boot:", e);
  process.exit(1);
}