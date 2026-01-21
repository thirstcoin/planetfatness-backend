import express from "express";
import cors from "cors";
import nacl from "tweetnacl";
import bs58 from "bs58";
import jwt from "jsonwebtoken";
import crypto from "crypto";

import {
  initDb,
  upsertUser,
  ensureProfile,
  setDisplayName,
  saveNonce,
  consumeNonce,
  getMe,
  getDaily,
  addDailyCalories,
  addActivity,
  logSession,
} from "./db";

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const JWT_SECRET = process.env.JWT_SECRET || "";
if (!JWT_SECRET || JWT_SECRET.length < 20) {
  console.warn("⚠️ Set a strong JWT_SECRET in env (min ~20 chars).");
}

// ---- config ----
const DAILY_CAP = 1000;

// ---- utils ----
function norm(s: any) {
  return String(s || "").trim();
}

function makeNonce() {
  return crypto.randomBytes(16).toString("hex");
}

function makeMessage(address: string, nonce: string) {
  return `Planet Fatness Gym Login

Wallet: ${address}
Nonce: ${nonce}

Sign this message to prove you own the wallet.
No gas. No transaction.`;
}

function signToken(address: string) {
  return jwt.sign({ sub: address, address }, JWT_SECRET || "DEV_ONLY_CHANGE_ME", { expiresIn: "30d" });
}

function auth(req: any, res: any, next: any) {
  const h = norm(req.headers.authorization);
  const token = h.startsWith("Bearer ") ? h.slice(7).trim() : "";
  if (!token) return res.status(401).json({ ok: false, error: "NO_TOKEN" });

  try {
    const decoded: any = jwt.verify(token, JWT_SECRET || "DEV_ONLY_CHANGE_ME");
    req.user = { address: decoded.address || decoded.sub };
    if (!req.user.address) throw new Error("no address");
    next();
  } catch {
    return res.status(401).json({ ok: false, error: "BAD_TOKEN" });
  }
}

// ---- health ----
app.get("/health", (_req, res) => res.json({ ok: true }));

// =====================================================
// AUTH (Solana-only)
// =====================================================

// 1) Get a nonce + message to sign
app.post("/auth/nonce", async (req, res) => {
  const address = norm(req.body?.address);
  if (!address) return res.status(400).json({ ok: false, error: "MISSING_ADDRESS" });

  if (address.length < 32 || address.length > 64) {
    return res.status(400).json({ ok: false, error: "BAD_ADDRESS" });
  }

  const nonce = makeNonce();
  const message = makeMessage(address, nonce);
  const expiresAt = Date.now() + 5 * 60 * 1000;

  await saveNonce({ address, nonce, message, expiresAt });
  await upsertUser(address);
  await ensureProfile(address);

  res.json({ ok: true, address, nonce, message, expiresAt });
});

// 2) Verify signature and return JWT + profile
app.post("/auth/verify", async (req, res) => {
  const address = norm(req.body?.address);
  const signature = norm(req.body?.signature);

  if (!address || !signature) {
    return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });
  }

  const consumed = await consumeNonce(address);
  if (!consumed.ok) {
    return res.status(400).json({ ok: false, error: consumed.reason });
  }

  try {
    const msgBytes = new TextEncoder().encode(consumed.message);
    const sigBytes = bs58.decode(signature);
    const pubKeyBytes = bs58.decode(address);

    const ok = nacl.sign.detached.verify(msgBytes, sigBytes, pubKeyBytes);
    if (!ok) return res.status(401).json({ ok: false, error: "BAD_SIGNATURE" });
  } catch {
    return res.status(401).json({ ok: false, error: "VERIFY_FAILED" });
  }

  const token = signToken(address);
  const profile = await ensureProfile(address);
  const me = await getMe(address);
  const daily = await getDaily(address);

  res.json({
    ok: true,
    token,
    profile: {
      address: profile?.address,
      gymId: profile?.gym_id,
      displayName: profile?.display_name,
    },
    stats: {
      totalCalories: Number(me?.total_calories || 0),
      totalMiles: Number(me?.total_miles || 0),
      bestSeconds: Number(me?.best_seconds || 0),
      today: daily.today,
      cap: DAILY_CAP,
      resetAt: daily.resetAt,
    },
  });
});

// =====================================================
// PROFILE
// =====================================================

app.get("/me", auth, async (req: any, res) => {
  const address = req.user.address;
  const profile = await ensureProfile(address);
  const me = await getMe(address);
  const daily = await getDaily(address);

  res.json({
    ok: true,
    profile: {
      address,
      gymId: profile?.gym_id,
      displayName: profile?.display_name,
    },
    stats: {
      totalCalories: Number(me?.total_calories || 0),
      totalMiles: Number(me?.total_miles || 0),
      bestSeconds: Number(me?.best_seconds ||