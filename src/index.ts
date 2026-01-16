import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import nacl from "tweetnacl";
import bs58 from "bs58";
import { pool } from "./db.js";
import { signToken, verifyToken } from "./auth.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

/* -------------------- helpers -------------------- */

function nowIso() {
  return new Date().toISOString();
}

function shortAddr(a: string) {
  if (!a) return "";
  return a.slice(0, 4) + "…" + a.slice(-4);
}

async function ensureUser(address: string) {
  await pool.query(
    `INSERT INTO users(address, total_calories, best_seconds, total_miles, created_at, updated_at)
     VALUES ($1, 0, 0, 0, NOW(), NOW())
     ON CONFLICT (address) DO NOTHING`,
    [address]
  );
}

/* -------------------- auth -------------------- */

app.post("/auth/challenge", async (req, res) => {
  const address = String(req.body?.address || "").trim();
  if (!address) return res.status(400).json({ error: "Missing address" });

  // nonce per address
  const nonce = Math.random().toString(36).slice(2) + "-" + Date.now().toString(36);

  const message = `Planet Fatness Gym Login

Wallet: ${address}
Nonce: ${nonce}
Time: ${nowIso()}`;

  await pool.query(
    `INSERT INTO auth_nonces(address, nonce, created_at)
     VALUES($1,$2,NOW())
     ON CONFLICT (address) DO UPDATE SET nonce=$2, created_at=NOW()`,
    [address, nonce]
  );

  res.json({ message, nonce });
});

app.post("/auth/verify", async (req, res) => {
  const address = String(req.body?.address || "").trim();
  const nonce = String(req.body?.nonce || "").trim();
  const signatureB64 = String(req.body?.signature || "").trim();

  if (!address || !nonce || !signatureB64) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const row = await pool.query(`SELECT nonce FROM auth_nonces WHERE address=$1`, [address]);
  if (!row.rows.length || row.rows[0].nonce !== nonce) {
    return res.status(401).json({ error: "Invalid nonce" });
  }

  const message = `Planet Fatness Gym Login

Wallet: ${address}
Nonce: ${nonce}
Time: ${nowIso()}`;

  // NOTE: your frontend signs the exact challenge.message returned from /auth/challenge.
  // So we must verify the SAME message. We'll re-fetch it by rebuilding isn't safe due to Time.
  // Fix: store the message in DB too and verify that exact string.
  // To keep it simple: store message now.
  // (We’ll do that by reading it from DB — see SQL + code below.)

  // If you already deployed without message storage, this will reject.
  // We'll instead store and verify exact message.

  const msgRow = await pool.query(`SELECT message FROM auth_nonces WHERE address=$1`, [address]);
  const storedMessage = msgRow.rows?.[0]?.message as string | undefined;
  if (!storedMessage) return res.status(500).json({ error: "Challenge message missing in DB" });

  let pubkey: Uint8Array;
  let sig: Uint8Array;

  try {
    pubkey = bs58.decode(address);
  } catch {
    return res.status(400).json({ error: "Bad address format" });
  }

  try {
    sig = Uint8Array.from(Buffer.from(signatureB64, "base64"));
  } catch {
    return res.status(400).json({ error: "Bad signature format" });
  }

  const ok = nacl.sign.detached.verify(
    new TextEncoder().encode(storedMessage),
    sig,
    pubkey
  );

  if (!ok) return res.status(401).json({ error: "Bad signature" });

  await ensureUser(address);
  const token = signToken(address);

  res.json({ token });
});

/* -------------------- auth middleware -------------------- */

function auth(req: any, res: any, next: any) {
  const h = String(req.headers?.authorization || "");
  if (!h.startsWith("Bearer ")) return res.status(401).json({ error: "Missing auth" });
  try {
    req.user = verifyToken(h.slice("Bearer ".length));
    next();
  } catch {
    res.status(401).json({ error: "Invalid token" });
  }
}

/* -------------------- activity -------------------- */

app.get("/activity/me", auth, async (req: any, res) => {
  const address = String(req.user?.address || "");
  if (!address) return res.status(401).json({ error: "No address" });

  await ensureUser(address);

  const r = await pool.query(
    `SELECT address, total_calories, best_seconds, total_miles
     FROM users WHERE address=$1`,
    [address]
  );

  const u = r.rows[0];
  res.json({
    address: u.address,
    totalCalories: Number(u.total_calories || 0),
    bestSeconds: Number(u.best_seconds || 0),
    totalMiles: Number(u.total_miles || 0)
  });
});

/**
 * POST /activity/add
 * Body: { caloriesDelta?: number, bestSeconds?: number, milesDelta?: number }
 * - caloriesDelta adds to lifetime calories
 * - milesDelta adds to lifetime miles (optional)
 * - bestSeconds updates if greater than existing best (runner)
 */
app.post("/activity/add", auth, async (req: any, res) => {
  const address = String(req.user?.address || "");
  if (!address) return res.status(401).json({ error: "No address" });

  const caloriesDelta = Number(req.body?.caloriesDelta || 0);
  const milesDelta = Number(req.body?.milesDelta || 0);
  const bestSeconds = Number(req.body?.bestSeconds || 0);

  if (
    !Number.isFinite(caloriesDelta) ||
    !Number.isFinite(milesDelta) ||
    !Number.isFinite(bestSeconds)
  ) {
    return res.status(400).json({ error: "Bad numbers" });
  }

  await ensureUser(address);

  // clamp to prevent goofy spam
  const cals = Math.max(0, Math.min(50000, caloriesDelta));
  const miles = Math.max(0, Math.min(1000, milesDelta));
  const best = Math.max(0, Math.min(24 * 60 * 60, bestSeconds)); // cap 24h

  await pool.query(
    `UPDATE users
     SET total_calories = total_calories + $2,
         total_miles = total_miles + $3,
         best_seconds = GREATEST(best_seconds, $4),
         updated_at = NOW()
     WHERE address = $1`,
    [address, cals, miles, best]
  );

  const r = await pool.query(
    `SELECT address, total_calories, best_seconds, total_miles
     FROM users WHERE address=$1`,
    [address]
  );

  const u = r.rows[0];
  res.json({
    ok: true,
    address: shortAddr(u.address),
    totalCalories: Number(u.total_calories || 0),
    bestSeconds: Number(u.best_seconds || 0),
    totalMiles: Number(u.total_miles || 0)
  });
});

app.get("/leaderboard", async (_req, res) => {
  const r = await pool.query(
    `SELECT address, total_calories, best_seconds
     FROM users
     ORDER BY total_calories DESC
     LIMIT 30`
  );

  res.json(
    r.rows.map((u) => ({
      address: u.address,
      totalCalories: Number(u.total_calories || 0),
      bestSeconds: Number(u.best_seconds || 0)
    }))
  );
});

/* -------------------- health -------------------- */

app.get("/health", (_req, res) => res.json({ ok: true }));

/* -------------------- start -------------------- */

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`Planet Fatness backend listening on :${PORT}`);
});