import dotenv from "dotenv";
dotenv.config();

import type { Request, Response } from "express";
import { Telegraf } from "telegraf";
import jwt from "jsonwebtoken";
import crypto from "crypto";

// --- Dynamic imports (env ready before db.ts reads DATABASE_URL) ---
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
  setTelegramIdentity,
  upsertUser,
  pool,
  createGreedRound,
  getActiveGreedRound,
  getGreedRoundByIdForAddress,
  getGreedPickedIndices,
  recordGreedPick,
  closeGreedRoundAsPoison,
  closeGreedRoundAsCashout,
  getGreedLeaderboard,
  getGreedFeed,
} = dbMod;

// -------------------------------
// App + config
// -------------------------------
const app = express();

const PORT = Number(process.env.PORT || 10000);
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const TG_GAME_SHORT_NAME = String(process.env.TG_GAME_SHORT_NAME || "planetfatness").trim();

app.use(express.json({ limit: "1mb" }));
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN.split(",").map((s) => s.trim()),
    credentials: false,
  })
);

// -------------------------------
// Helpers
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

function normalizeWindow(w: any): "lifetime" | "day" | "week" | "month" {
  const s = String(w || "lifetime").toLowerCase().trim();
  if (s === "daily") return "day";
  if (s === "weekly") return "week";
  if (s === "monthly") return "month";
  if (s === "day" || s === "week" || s === "month" || s === "lifetime") return s;
  return "lifetime";
}

function signTokenForAddress(address: string) {
  const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
  if (!JWT_SECRET) throw new Error("missing_jwt_secret");
  return jwt.sign({ address }, JWT_SECRET, { expiresIn: "30d" });
}

// -------------------------------
// Other game rules
// -------------------------------
const COMMON_RULES = {
  minDurationMs: 10_000,
  maxRunCalories: 180,
  dailyCapCalories: 1200,
  cpmCap: 220,
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
  basket: { ...COMMON_RULES, maxScorePerRun: 0 },
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
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN calories ELSE 0 END), 0) AS calories,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN miles ELSE 0 END), 0) AS miles,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN duration_ms ELSE 0 END), 0) AS duration_ms,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN score ELSE 0 END), 0) AS score,
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

  if (game !== "runner") {
    const scorePerMin = durationMin > 0 ? score / durationMin : score;

    const SCORE_PER_MIN_CAP: Record<string, number> = {
      runner: 999999,
      snack: 520,
      lift: 450,
      basket: 360,
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

  if (game === "snack") base = score * 2.2;
  else if (game === "basket") base = score * 1.8;
  else if (game === "lift") base = score * 2.0;
  else if (game === "runner") {
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
  if (g.metric === "score") progress = Math.floor(params.today.score || 0);
  if (g.metric === "miles") progress = Number(params.today.miles || 0);
  if (g.metric === "seconds") progress = Math.floor((params.today.durationMs || 0) / 1000);

  const goal = g.goal;
  const hit = progress >= goal;
  return { goal, progress, hit };
}

// -------------------------------
// Greed config
// -------------------------------
const GREED_TAX = 0.03;
const GREED_MIN_WAGER = 1000;
const GREED_MAX_WAGER = 100000;
const GREED_TOTAL_DONUTS = 12;
const GREED_POISON_COUNT = 2;
const GREED_MULTIPLIERS = [1.02, 1.07, 1.15, 1.30, 1.48, 1.70, 1.98, 2.28, 2.70, 3.50];

function sanitizeWager(raw: any) {
  const wager = Math.floor(Number(raw));
  if (!Number.isFinite(wager)) return null;
  if (wager < GREED_MIN_WAGER || wager > GREED_MAX_WAGER) return null;
  return wager;
}

function getGreedMultiplierForSafeClicks(safeClicks: number) {
  if (safeClicks <= 0) return 1.0;
  return GREED_MULTIPLIERS[safeClicks - 1] || GREED_MULTIPLIERS[GREED_MULTIPLIERS.length - 1];
}

function sha256Hex(input: string) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

function derivePoisonIndicesFromSeed(seed: string, total: number, poisonCount: number) {
  const scores: { index: number; score: string }[] = [];
  for (let i = 0; i < total; i++) {
    scores.push({
      index: i,
      score: sha256Hex(`${seed}:${i}`),
    });
  }

  scores.sort((a, b) => a.score.localeCompare(b.score));
  return scores.slice(0, poisonCount).map((x) => x.index).sort((a, b) => a - b);
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
        "  GET  /profile/me",
        "  POST /profile/name",
        "  GET  /activity/me",
        "  POST /activity/add",
        "  POST /activity/submit",
        "  GET  /activity/summary",
        "  GET  /daily/progress",
        "  GET  /leaderboard",
        "  GET  /leaderboard/v2",
        "  GET  /leaderboard/games",
        "  --- GREED GAME ---",
        "  POST /greed/start",
        "  POST /greed/pick",
        "  POST /greed/cashout",
        "  GET  /greed/active",
        "  GET  /greed/leaderboards",
        "  GET  /greed/feed",
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

app.get("/profile/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;
  const me = await getMe(address);
  if (!me) return res.status(404).json({ error: "User not found" });

  res.json({
    address: me.address,
    displayName: me.display_name || null,
  });
});

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

app.post("/activity/add", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;

  const legacyAddCalories = Number(req.body?.addCalories ?? 0);
  const legacyBestSeconds = Number(req.body?.bestSeconds ?? 0);
  const legacyAddMiles = Number(req.body?.addMiles ?? 0);

  const game = req.body?.game ? String(req.body.game) : "";
  const v2Calories = req.body?.calories != null ? Number(req.body.calories) : undefined;
  const v2Miles = req.body?.miles != null ? Number(req.body.miles) : undefined;
  const v2BestSeconds = req.body?.bestSeconds != null ? Number(req.body.bestSeconds) : undefined;
  const v2Score = req.body?.score != null ? Number(req.body.score) : undefined;
  const v2DurationMs = req.body?.durationMs != null ? Number(req.body.durationMs) : undefined;
  const v2Streak = req.body?.streak != null ? Number(req.body.streak) : undefined;

  const finalAddCalories = Number.isFinite(v2Calories as any) ? Number(v2Calories) : legacyAddCalories;
  const finalAddMiles = Number.isFinite(v2Miles as any) ? Number(v2Miles) : legacyAddMiles;
  const finalBestSeconds = Number.isFinite(v2BestSeconds as any) ? Number(v2BestSeconds) : legacyBestSeconds;
  const finalScore = Number.isFinite(v2Score as any) ? Number(v2Score) : 0;
  const finalDurationMs = Number.isFinite(v2DurationMs as any) ? Math.max(0, Math.floor(Number(v2DurationMs))) : 0;
  const finalStreak = Number.isFinite(v2Streak as any) ? Math.max(0, Math.floor(Number(v2Streak))) : 0;

  const me = await addActivity({
    address,
    addCalories: Number.isFinite(finalAddCalories) ? finalAddCalories : 0,
    bestSeconds: Number.isFinite(finalBestSeconds) ? finalBestSeconds : 0,
    addMiles: Number.isFinite(finalAddMiles) ? finalAddMiles : 0,
  });

  if (!me) return res.status(500).json({ error: "Update failed" });

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
        streak: finalStreak,
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

app.post("/activity/submit", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;

    const game = asGameKey(req.body?.game);
    const score = Number(req.body?.score ?? 0);
    const miles = Number(req.body?.miles ?? 0);
    const bestSeconds = Number(req.body?.bestSeconds ?? 0);
    const durationMs = Number(req.body?.durationMs ?? 0);
    const streak = Math.max(0, Math.floor(Number(req.body?.streak ?? 0) || 0));

    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      return res.status(400).json({ error: "Missing durationMs" });
    }

    const calc = computeEarnedCalories({
      game,
      score,
      miles,
      bestSeconds,
      durationMs,
    });

    const todayBefore = await getTodayAgg(address, game);
    const rules = RULES[game] || COMMON_RULES;

    const remaining = Math.max(0, rules.dailyCapCalories - todayBefore.calories);
    const earnedCapped = Math.max(0, Math.min(calc.earnedCalories, remaining));

    await logSession({
      address,
      game,
      calories: earnedCapped,
      miles: Math.max(0, miles || 0),
      bestSeconds: Math.max(0, bestSeconds || 0),
      score: Math.max(0, score || 0),
      streak,
      durationMs: Math.max(0, Math.floor(durationMs || 0)),
    });

    const me = await addActivity({
      address,
      addCalories: earnedCapped,
      bestSeconds: Math.max(0, bestSeconds || 0),
      addMiles: Math.max(0, miles || 0),
    });

    if (!me) return res.status(500).json({ error: "Update failed" });

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
// Greed API (provably fair)
// -------------------------------
app.post("/greed/start", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const wager = sanitizeWager(req.body?.wager);

    if (wager == null) {
      return res.status(400).json({ error: `Wager must be between ${GREED_MIN_WAGER} and ${GREED_MAX_WAGER}` });
    }

    const existing = await getActiveGreedRound(address);
    if (existing) {
      return res.status(400).json({
        error: "Round already active",
        roundId: existing.id,
      });
    }

    const netStake = Math.floor(wager * (1 - GREED_TAX));
    const serverSeed = crypto.randomBytes(32).toString("hex");
    const commitHash = sha256Hex(serverSeed);
    const poisonIndices = derivePoisonIndicesFromSeed(serverSeed, GREED_TOTAL_DONUTS, GREED_POISON_COUNT);

    const round = await createGreedRound({
      address,
      wager,
      netStake,
      poisonIndices,
      seed: serverSeed,
      commitHash,
    });

    return res.json({
      ok: true,
      roundId: round.id,
      wager,
      netStake,
      totalDonuts: GREED_TOTAL_DONUTS,
      poisonCount: GREED_POISON_COUNT,
      currentMultiplier: 1.0,
      cashoutAvailable: false,
      provablyFair: {
        commitHash,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to start greed round" });
  }
});

app.post("/greed/pick", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const roundId = Math.floor(Number(req.body?.roundId));
    const pickedIndex = Math.floor(Number(req.body?.pickedIndex));

    if (!Number.isFinite(roundId) || roundId <= 0) {
      return res.status(400).json({ error: "Invalid roundId" });
    }

    if (!Number.isFinite(pickedIndex) || pickedIndex < 0 || pickedIndex >= GREED_TOTAL_DONUTS) {
      return res.status(400).json({ error: "Invalid pickedIndex" });
    }

    const round = await getGreedRoundByIdForAddress(roundId, address);
    if (!round) {
      return res.status(404).json({ error: "Round not found" });
    }

    if (round.status !== "active" || round.is_active !== true) {
      return res.status(400).json({ error: "Round is not active" });
    }

    const pickedAlready = await getGreedPickedIndices(roundId);
    if (pickedAlready.includes(pickedIndex)) {
      return res.status(400).json({ error: "Donut already picked" });
    }

    const poisonIndices = Array.isArray(round.poison_indices) ? round.poison_indices.map((n: any) => Number(n)) : [];
    const isPoison = poisonIndices.includes(pickedIndex);

    if (isPoison) {
      await recordGreedPick({
        roundId,
        donutIndex: pickedIndex,
        result: "poison",
      });

      const currentMultiplier = Number(round.current_multiplier || 1.0);

      const closed = await closeGreedRoundAsPoison({
        roundId,
        safeClicks: Number(round.safe_clicks || 0),
        currentMultiplier,
      });

      return res.json({
        ok: true,
        result: "poison",
        roundEnded: true,
        safeClicks: Number(round.safe_clicks || 0),
        currentMultiplier,
        payout: 0,
        provablyFair: {
          commitHash: closed.commit_hash,
          serverSeed: closed.server_seed,
          poisonIndices: closed.poison_indices,
        },
      });
    }

    const newSafeClicks = Number(round.safe_clicks || 0) + 1;
    const newMultiplier = getGreedMultiplierForSafeClicks(newSafeClicks);

    await recordGreedPick({
      roundId,
      donutIndex: pickedIndex,
      result: "safe",
    });

    if (newSafeClicks >= 10) {
      const payout = Math.floor(Number(round.net_stake) * newMultiplier);

      const closed = await closeGreedRoundAsCashout({
        roundId,
        safeClicks: newSafeClicks,
        currentMultiplier: newMultiplier,
        payout,
        result: "perfect",
      });

      await logSession({
        address,
        game: "greed",
        calories: 0,
        miles: 0,
        bestSeconds: 0,
        score: payout,
        durationMs: 0,
      });

      return res.json({
        ok: true,
        result: "perfect",
        roundEnded: true,
        safeClicks: newSafeClicks,
        currentMultiplier: newMultiplier,
        payout,
        cashoutAvailable: false,
        provablyFair: {
          commitHash: closed.commit_hash,
          serverSeed: closed.server_seed,
          poisonIndices: closed.poison_indices,
        },
      });
    }

    await pool.query(
      `
      UPDATE greed_rounds
      SET
        safe_clicks = $2,
        current_multiplier = $3,
        updated_at = NOW()
      WHERE id = $1;
      `,
      [roundId, newSafeClicks, newMultiplier]
    );

    return res.json({
      ok: true,
      result: "safe",
      roundEnded: false,
      safeClicks: newSafeClicks,
      currentMultiplier: newMultiplier,
      payout: 0,
      cashoutAvailable: newSafeClicks >= 1,
      finalDonutLive: newSafeClicks === 9,
      provablyFair: {
        commitHash: round.commit_hash,
      },
    });
  } catch (e: any) {
    if (String(e?.message || "").includes("duplicate key")) {
      return res.status(400).json({ error: "Donut already picked" });
    }
    console.error(e);
    return res.status(500).json({ error: "Greed pick failed" });
  }
});

app.post("/greed/cashout", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const roundId = Math.floor(Number(req.body?.roundId));

    if (!Number.isFinite(roundId) || roundId <= 0) {
      return res.status(400).json({ error: "Invalid roundId" });
    }

    const round = await getGreedRoundByIdForAddress(roundId, address);
    if (!round) {
      return res.status(404).json({ error: "Round not found" });
    }

    if (round.status !== "active" || round.is_active !== true) {
      return res.status(400).json({ error: "Round is not active" });
    }

    const safeClicks = Number(round.safe_clicks || 0);
    if (safeClicks < 1) {
      return res.status(400).json({ error: "Cashout not available yet" });
    }

    const currentMultiplier = Number(round.current_multiplier || 1.0);
    const payout = Math.floor(Number(round.net_stake) * currentMultiplier);

    const closed = await closeGreedRoundAsCashout({
      roundId,
      safeClicks,
      currentMultiplier,
      payout,
      result: "cashout",
    });

    await logSession({
      address,
      game: "greed",
      calories: 0,
      miles: 0,
      bestSeconds: 0,
      score: payout,
      durationMs: 0,
    });

    return res.json({
      ok: true,
      result: "cashout",
      roundEnded: true,
      safeClicks,
      currentMultiplier,
      payout,
      provablyFair: {
        commitHash: closed.commit_hash,
        serverSeed: closed.server_seed,
        poisonIndices: closed.poison_indices,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Greed cashout failed" });
  }
});

app.get("/greed/active", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const round = await getActiveGreedRound(address);

    if (!round) {
      return res.json({ active: false, round: null });
    }

    const pickedIndices = await getGreedPickedIndices(Number(round.id));

    return res.json({
      active: true,
      round: {
        id: Number(round.id),
        wager: Number(round.wager),
        netStake: Number(round.net_stake),
        safeClicks: Number(round.safe_clicks || 0),
        currentMultiplier: Number(round.current_multiplier || 1.0),
        pickedIndices,
        cashoutAvailable: Number(round.safe_clicks || 0) >= 1,
        provablyFair: {
          commitHash: round.commit_hash,
        },
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch active round" });
  }
});

app.get("/greed/leaderboards", async (req: Request, res: Response) => {
  try {
    const window = normalizeWindow(req.query.window || "lifetime");
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 10)));

    const [mostWagered, mostWon, perfectRuns, biggestCashout, topGlazeSacrifices] = await Promise.all([
      getGreedLeaderboard({ board: "most_wagered", window, limit }),
      getGreedLeaderboard({ board: "most_won", window, limit }),
      getGreedLeaderboard({ board: "perfect_runs", window, limit }),
      getGreedLeaderboard({ board: "biggest_cashout", window, limit }),
      getGreedLeaderboard({ board: "top_glaze_sacrifices", window, limit }),
    ]);

    return res.json({
      ok: true,
      window,
      boards: {
        mostWagered,
        mostWon,
        perfectRuns,
        biggestCashout,
        topGlazeSacrifices,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Greed leaderboards failed" });
  }
});

app.get("/greed/feed", async (req: Request, res: Response) => {
  try {
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 20)));
    const rows = await getGreedFeed(limit);
    return res.json({ ok: true, rows });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Greed feed failed" });
  }
});

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

app.get("/admin/launch-reset", async (req: Request, res: Response) => {
  const secret = req.query.secret;
  if (secret !== "launch2026") return res.status(403).send("Unauthorized");

  try {
    await pool.query(`
      UPDATE users
      SET total_calories = 0,
          total_miles = 0,
          best_seconds = 0,
          lifetime_makes = 0;
    `);

    await pool.query(`TRUNCATE TABLE sessions CASCADE;`);
    await pool.query(`TRUNCATE TABLE greed_picks CASCADE;`);
    await pool.query(`TRUNCATE TABLE greed_rounds CASCADE;`);

    try {
      await pool.query(`UPDATE pf_users SET total_calories = 0;`);
    } catch {}

    res.type("text/plain").send("✅ Planet Fatness deep cleaned.");
  } catch (err: any) {
    res.status(500).send("❌ Reset failed: " + err.message);
  }
});

// -------------------------------
// Telegram game launch
// -------------------------------
const bot = new Telegraf(process.env.GREED_BOT_TOKEN || "");

bot.command("game", async (ctx) => {
  await ctx.replyWithGame(TG_GAME_SHORT_NAME);
});

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
        } catch {}
      }
    } catch (e) {
      console.error("TG game upsert/identity failed:", e);
      await ctx.answerGameQuery("https://planetfatness.fit/");
      return;
    }

    let token = "";
    try {
      token = signTokenForAddress(address);
    } catch (e) {
      console.error("TG game signToken failed:", e);
      await ctx.answerGameQuery("https://planetfatness.fit/");
      return;
    }

    const launchUrl = `https://planetfatness.fit/greed?t=${encodeURIComponent(token)}`;
    await ctx.answerGameQuery(launchUrl);
  } catch (e) {
    console.error("TG game callback handler error:", e);
    try {
      await ctx.answerGameQuery("https://planetfatness.fit/");
    } catch {}
  }
});

// Main gym bot
const gymBot = new Telegraf(process.env.TG_BOT_TOKEN || "");

gymBot.start((ctx) => {
  ctx.reply("🏋️ Welcome back to Planet Fatness Gym! Use the button below to start your workout.");
});

gymBot.on("callback_query", async (ctx) => {
  try {
    const address = `tg:${ctx.from.id}`;
    const token = signTokenForAddress(address);
    await ctx.answerGameQuery(`https://planetfatness.fit/?t=${encodeURIComponent(token)}`);
  } catch (e) {
    await ctx.answerGameQuery("https://planetfatness.fit/");
  }
});

// -------------------------------
// Boot
// -------------------------------
try {
  await initDb();

  if (process.env.GREED_BOT_TOKEN) {
    bot.launch().then(() => console.log("🤖 Planet Fatness Greed Bot Engine Active"));
  } else {
    console.warn("⚠️ No GREED_BOT_TOKEN found in environment.");
  }

  if (process.env.TG_BOT_TOKEN) {
    gymBot.launch().then(() => console.log("🤖 Planet Fatness Gym Bot Engine Active"));
  } else {
    console.warn("⚠️ No TG_BOT_TOKEN found in environment.");
  }

  app.listen(PORT, () => console.log(`✅ Planet Fatness backend on :${PORT}`));
} catch (e) {
  console.error("❌ Failed to boot:", e);
  process.exit(1);
}