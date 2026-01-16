import express, { Request, Response } from "express";
import cors from "cors";
import dotenv from "dotenv";
import authRouter, { requireAuth } from "./auth.js";
import { initDb, getMe, addActivity, getLeaderboard } from "./db.js";

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

app.get("/health", (_req: Request, res: Response) => res.json({ ok: true }));

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
 * POST /activity/add (auth)
 * body: { addCalories?: number, bestSeconds?: number, addMiles?: number }
 */
app.post("/activity/add", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;

  const addCalories = Number(req.body?.addCalories ?? 0);
  const bestSeconds = Number(req.body?.bestSeconds ?? 0);
  const addMiles = Number(req.body?.addMiles ?? 0);

  const me = await addActivity({ address, addCalories, bestSeconds, addMiles });
  if (!me) return res.status(500).json({ error: "Update failed" });

  res.json({
    address: me.address,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

/**
 * GET /leaderboard
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