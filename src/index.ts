import express from "express";
import cors from "cors";
import authRouter, { requireAuth } from "./auth";
import { query } from "./db";

const app = express();
const PORT = process.env.PORT || 3000;

// --- middleware ---
app.use(cors());
app.use(express.json());

// --- health ---
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "planet-fatness-backend" });
});

// --- auth ---
app.use("/auth", authRouter);

// --- activity: current user ---
app.get("/activity/me", requireAuth, async (req, res) => {
  const address = (req as any).user.address;

  try {
    const { rows } = await query(
      `
      SELECT
        address,
        total_calories,
        best_seconds
      FROM users
      WHERE address = $1
      `,
      [address]
    );

    if (rows.length === 0) {
      return res.json({
        address,
        totalCalories: 0,
        bestSeconds: 0,
      });
    }

    const u = rows[0];

    res.json({
      address: u.address,
      totalCalories: Number(u.total_calories || 0),
      bestSeconds: Number(u.best_seconds || 0),
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to load activity" });
  }
});

// --- leaderboard ---
app.get("/leaderboard", async (_req, res) => {
  try {
    const { rows } = await query(
      `
      SELECT
        address,
        total_calories,
        best_seconds
      FROM users
      ORDER BY total_calories DESC
      LIMIT 50
      `
    );

    res.json(
      rows.map((u) => ({
        address: u.address,
        totalCalories: Number(u.total_calories || 0),
        bestSeconds: Number(u.best_seconds || 0),
      }))
    );

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Leaderboard unavailable" });
  }
});

// --- ingest calories (called by games later) ---
app.post("/activity/add", requireAuth, async (req, res) => {
  const address = (req as any).user.address;
  const { calories, bestSeconds } = req.body;

  try {
    await query(
      `
      INSERT INTO users (address, total_calories, best_seconds)
      VALUES ($1, $2, $3)
      ON CONFLICT (address)
      DO UPDATE SET
        total_calories = users.total_calories + EXCLUDED.total_calories,
        best_seconds = GREATEST(users.best_seconds, EXCLUDED.best_seconds)
      `,
      [
        address,
        Number(calories || 0),
        Number(bestSeconds || 0),
      ]
    );

    res.json({ ok: true });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to record activity" });
  }
});

// --- start ---
app.listen(PORT, () => {
  console.log(`Planet Fatness backend running on ${PORT}`);
});