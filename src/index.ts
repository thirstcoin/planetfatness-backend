import express from "express";
import cors from "cors";
import { pool, initDb } from "./db";
import { signToken, authMiddleware } from "./auth";

const app = express();
app.use(cors());
app.use(express.json());

initDb().then(() => {
  console.log("Planet Fatness DB ready 🟣");
});

// health check
app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// fake login (wallet verified on frontend)
app.post("/auth/login", async (req, res) => {
  const { address } = req.body;
  if (!address) return res.status(400).json({ error: "No address" });

  await pool.query(
    `INSERT INTO users (address)
     VALUES ($1)
     ON CONFLICT (address) DO NOTHING`,
    [address]
  );

  const token = signToken(address);
  res.json({ token });
});

// get my stats
app.get("/activity/me", authMiddleware, async (req, res) => {
  const address = (req as any).user;

  const { rows } = await pool.query(
    "SELECT * FROM users WHERE address=$1",
    [address]
  );

  res.json(rows[0] || {});
});

// leaderboard
app.get("/leaderboard", async (_req, res) => {
  const { rows } = await pool.query(`
    SELECT address, total_calories, best_seconds
    FROM users
    ORDER BY total_calories DESC
    LIMIT 50
  `);

  res.json(rows);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Planet Fatness backend running on ${PORT}`);
});