import express, { Request, Response } from "express";
import cors from "cors";
import dotenv from "dotenv";
import authRouter, { requireAuth } from "./auth.js";
import {
  initDb,
  getMe,
  addActivity,
  getLeaderboard,
  // NEW (from updated db.ts)
  logSession,
  getLeaderboardV2,
  getActivitySummary,
  pool,
} from "./db.js";

dotenv.config();

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
  // default: treat unknown as snack-ish but safer
  return "snack";
}

function nowIso() {
  return new Date().toISOString();
}

// Per-game guardrails (tune later, but these stop obvious farming immediately)
const RULES: Record<
  GameKey,
  {
    minDurationMs: number;     // must be at least this long to earn calories
    maxRunCalories: number;    // per-run hard cap
    dailyCapCalories: number;  // per-day per-game hard cap
    cpmCap: number;            // calories-per-minute cap (prevents "huge score in 5s")
    maxScorePerRun: number;    // sanity cap
  }
> = {
  // runner should mainly reward *time + distance*; cap stops micro-run spam
  runner: { minDurationMs: 12_000, maxRunCalories: 260, dailyCapCalories: 1_600, cpmCap: 220, maxScorePerRun: 0 },
  // snack is the easiest to script/spam; keep it tighter
  snack:  { minDurationMs: 10_000, maxRunCalories: 190, dailyCapCalories: 1_200, cpmCap: 190, maxScorePerRun: 220 },
  // lift can be “tap spam”; require time and cap CPM
  lift:   { minDurationMs: 10_000, maxRunCalories: 210, dailyCapCalories: 1_250, cpmCap: 200, maxScorePerRun: 260 },
  // basket is score-y; cap it a bit lower so it doesn’t dominate
  basket: { minDurationMs: 10_000, maxRunCalories: 170, dailyCapCalories: 950,  cpmCap: 175, maxScorePerRun: 260 },
};

// Daily “goal” hooks (what your UI shows; goal is NOT a cap)
const DAILY_GOALS: Record<GameKey, { label: string; goal: number; metric: "score" | "miles" | "seconds" }> = {
  snack:  { label: "Daily Goal", goal: 30, metric: "score" },     // your /30 in snack
  runner: { label: "Daily Goal", goal: 1,  metric: "miles" },     // example: 1 mile/day
  lift:   { label: "Daily Goal", goal: 50, metric: "score" },     // example: 50 reps/points
  basket: { label: "Daily Goal", goal: 20, metric: "score" },     // example: 20 points
};

async function getTodayAgg(address: string, game: GameKey) {
  // Using sessions as receipts. We keep it simple: "today" = current_date in DB timezone.
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
  score: number;       // score-like metric (snack correct count, lift reps/points, basket points)
  miles: number;       // runner distance
  bestSeconds: number; // runner best time metric if you want it later
  durationMs: number;
}) {
  const { game } = params;
  const rules = RULES[game];

  const durationMs = Math.max(0, Math.floor(Number(params.durationMs || 0)));
  const durationMin = durationMs / 60000;

  const score = Math.max(0, Number(params.score || 0));
  const miles = Math.max(0, Number(params.miles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));

  // Basic sanity
  if (durationMs < rules.minDurationMs) {
    return { earnedCalories: 0, reason: "too_short" as const, normalized: { score, miles, bestSeconds, durationMs } };
  }

  // Score caps (for score-based games)
  if (rules.maxScorePerRun > 0 && score > rules.maxScorePerRun) {
    return { earnedCalories: 0, reason: "score_too_high" as const, normalized: { score, miles, bestSeconds, durationMs } };
  }

  // Base earn (game-specific)
  let base = 0;

  if (game === "snack") {
    // snack: correct count, but bounded by time so you can’t “teleport” scores
    // ~2.2 calories per correct, tuned to feel rewarding but not dominant
    base = score * 2.2;
  } else if (game === "basket") {
    // basket: points-based
    base = score * 1.8;
  } else if (game === "lift") {
    // lift: reps/points, slightly higher than basket but still time-capped
    base = score * 2.0;
  } else if (game === "runner") {
    // runner: distance is the honest signal; duration still matters
    // ~110 calories per mile baseline (feel free to tune)
    base = miles * 110;
    // if miles missing, allow some time-based earning (lower)
    if (!miles || miles <= 0) base = durationMin * 120;
  }

  // Calories-per-minute cap (prevents short-run inflation)
  const cpmCap = rules.cpmCap;
  const timeCap = durationMin * cpmCap;

  // Apply caps: time cap + per-run cap
  const earnedCalories = Math.floor(
    clamp(base, 0, Math.min(timeCap, rules.maxRunCalories))
  );

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
// Routes (existing + new)
// -------------------------------

// ✅ Root route so the base Render URL works (fixes "Cannot GET /")
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
 * returns lifetime + weekly + monthly totals computed from sessions (and lifetime from users table)
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
 * returns per-game today progress + caps remaining (so UI can show /30, etc.)
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
 * POST /activity/add (auth)
 * Backwards compatible body (existing):
 *   { addCalories?: number, bestSeconds?: number, addMiles?: number }
 *
 * Optional "session receipt" fields (NEW, additive):
 *   {
 *     game?: "runner"|"snack"|"lift"|"basket"|string,
 *     calories?: number,   // alias for addCalories
 *     miles?: number,      // alias for addMiles
 *     bestSeconds?: number,
 *     score?: number,
 *     durationMs?: number
 *   }
 *
 * Behavior:
 * - Always updates lifetime rollups via addActivity (same as today)
 * - If body includes `game` OR `durationMs` OR `score`, it also logs a session to sessions table
 */
app.post("/activity/add", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;

  // --- Backwards-compatible fields ---
  const addCalories = Number(req.body?.addCalories ?? 0);
  const bestSeconds = Number(req.body?.bestSeconds ?? 0);
  const addMiles = Number(req.body?.addMiles ?? 0);

  // --- Optional v2 receipt fields ---
  const game = req.body?.game ? String(req.body.game) : "";
  const calories = req.body?.calories != null ? Number(req.body.calories) : undefined;
  const miles = req.body?.miles != null ? Number(req.body.miles) : undefined;
  const score = req.body?.score != null ? Number(req.body.score) : undefined;
  const durationMs = req.body?.durationMs != null ? Number(req.body.durationMs) : undefined;

  // If caller used v2 field names, use those as aliases (but don’t break old clients)
  const finalAddCalories = Number.isFinite(calories as any) ? Number(calories) : addCalories;
  const finalAddMiles = Number.isFinite(miles as any) ? Number(miles) : addMiles;
  const finalBestSeconds = Number.isFinite(bestSeconds as any) ? Number(bestSeconds) : 0;

  // 1) Update lifetime rollups (same core behavior as before)
  const me = await addActivity({
    address,
    addCalories: finalAddCalories,
    bestSeconds: finalBestSeconds,
    addMiles: finalAddMiles,
  });

  if (!me) return res.status(500).json({ error: "Update failed" });

  // 2) If this looks like a session receipt, also log it (additive)
  const looksLikeReceipt =
    (!!game && game.length > 0) ||
    (Number.isFinite(score as any) && Number(score) > 0) ||
    (Number.isFinite(durationMs as any) && Number(durationMs) > 0);

  if (looksLikeReceipt) {
    // very light sanity clamps (we’ll tighten later if needed)
    const safeGame = (game || "unknown").slice(0, 32);
    const safeDuration = Number.isFinite(durationMs as any) ? Math.max(0, Math.floor(Number(durationMs))) : 0;
    const safeScore = Number.isFinite(score as any) ? Math.max(0, Number(score)) : 0;

    try {
      await logSession({
        address,
        game: safeGame,
        calories: finalAddCalories,
        miles: finalAddMiles,
        bestSeconds: finalBestSeconds,
        score: safeScore,
        durationMs: safeDuration,
      });
    } catch (e) {
      // IMPORTANT: do NOT fail the request if session logging fails;
      // lifetime rollups are still the truth for v1 clients.
      console.error("logSession failed:", e);
    }
  }

  // Response stays the same shape as your current API (so the site won’t break)
  res.json({
    address: me.address,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

/**
 * NEW: POST /activity/submit (auth)
 * Server-computed calories + anti-farm caps.
 *
 * Body (one shape for all games):
 * {
 *   game: "snack"|"runner"|"lift"|"basket",
 *   score?: number,        // snack correct count / basket points / lift reps
 *   miles?: number,        // runner distance (if available)
 *   bestSeconds?: number,  // optional (runner)
 *   durationMs: number     // required for fairness + anti-spam
 * }
 *
 * Returns:
 *  - earnedCalories (server decided)
 *  - today progress + caps
 *  - updated lifetime totals
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

    // 2) apply daily per-game cap (hard cap)
    const todayBefore = await getTodayAgg(address, game);
    const rules = RULES[game];

    const remaining = Math.max(0, rules.dailyCapCalories - todayBefore.calories);
    const earnedCapped = Math.max(0, Math.min(calc.earnedCalories, remaining));

    // 3) log session receipt (always, because this is the “secure” endpoint)
    // NOTE: we log the final calories awarded (after caps), not the raw attempt
    await logSession({
      address,
      game,
      calories: earnedCapped,
      miles: Math.max(0, miles || 0),
      bestSeconds: Math.max(0, bestSeconds || 0),
      score: Math.max(0, score || 0),
      durationMs: Math.max(0, Math.floor(durationMs || 0)),
    });

    // 4) update lifetime rollups (users table)
    const me = await addActivity({
      address,
      addCalories: earnedCapped,
      bestSeconds: Math.max(0, bestSeconds || 0),
      addMiles: Math.max(0, miles || 0),
    });

    if (!me) return res.status(500).json({ error: "Update failed" });

    // 5) return daily progress snapshot (useful for UI /30 etc.)
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
 * GET /leaderboard  (legacy, lifetime users table)
 */
app.get("/leaderboard", async (_req: Request, res: Response) => {
  const top = await getLeaderboard(30);
  res.json(
    top.map((u) => ({
      address: u.address,
      totalCalories: Number(u.total_calories || 0),
      bestSeconds: Number(u.best_seconds || 0),
      totalMiles: Number(u.total_miles || 0),
    }))
  );
});

/**
 * NEW: GET /leaderboard/v2
 * Query:
 *   ?window=weekly|monthly|lifetime
 *   ?metric=calories|miles|score|bestSeconds
 *   ?game=runner|snack|lift|basket  (optional)
 *   ?limit=30 (max 200)
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

// Boot
(async () => {
  try {
    await initDb();
    app.listen(PORT, () => console.log(`✅ Planet Fatness backend on :${PORT}`));
  } catch (e) {
    console.error("❌ Failed to boot:", e);
    process.exit(1);
  }
})();