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
        "  GET  /activity/me        (auth)",
        "  POST /activity/add       (auth)",
        "  GET  /activity/summary   (auth)   [NEW]",
        "  GET  /leaderboard",
        "  GET  /leaderboard/v2     [NEW]",
        "",
        "tapping counts as cardio 🟣🟡",
      ].join("\n")
    );
});

app.get("/health", (_req: Request, res: Response) =>
  res.json({
    ok: true,
    service: "planetfatness-backend",
    ts: new Date().toISOString(),
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