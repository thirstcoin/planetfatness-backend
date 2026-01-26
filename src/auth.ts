import { Router, Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import nacl from "tweetnacl";
import bs58 from "bs58";
import crypto from "crypto";
import { upsertUser, setDisplayName } from "./db.js";

const router = Router();

const JWT_SECRET = process.env.JWT_SECRET || "";
if (!JWT_SECRET) {
  console.warn("⚠️ Missing JWT_SECRET (set it in Render env vars). Auth verify will fail.");
}

// Telegram Bot Token (from BotFather) for verifying WebApp initData
const TG_BOT_TOKEN = process.env.TG_BOT_TOKEN || "";
if (!TG_BOT_TOKEN) {
  console.warn("⚠️ Missing TG_BOT_TOKEN. /auth/telegram will fail until you set it.");
}

// Optional: how old initData can be (seconds). Default 1 day.
const TG_MAX_AGE_SEC = Number(process.env.TG_MAX_AGE_SEC || 86400);

type Challenge = { nonce: string; message: string; exp: number };
const challenges = new Map<string, Challenge>();

function randNonce(len = 24) {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let out = "";
  for (let i = 0; i < len; i++) out += chars[(Math.random() * chars.length) | 0];
  return out;
}

function signToken(address: string) {
  return jwt.sign({ address }, JWT_SECRET, { expiresIn: "30d" });
}

function safeAddress(a: string) {
  return String(a || "").trim();
}

/**
 * Supports signatures sent as:
 * - base64 string (recommended)
 * - base58 string (fallback)
 */
function decodeSignature(sig: string): Uint8Array {
  const s = String(sig || "").trim();
  if (!s) throw new Error("empty signature");

  // Heuristic: base64 often has + / = and is longer; base58 won't.
  const looksBase64 = /[+/=]/.test(s) || s.length > 70;

  if (looksBase64) {
    const buf = Buffer.from(s, "base64");
    return new Uint8Array(buf);
  }

  // fallback: base58
  return bs58.decode(s);
}

export function requireAuth(req: Request, res: Response, next: NextFunction) {
  const h = req.headers.authorization || "";
  const token = h.startsWith("Bearer ") ? h.slice(7) : "";
  if (!token) return res.status(401).json({ error: "Missing token" });

  try {
    if (!JWT_SECRET) return res.status(500).json({ error: "Server missing JWT_SECRET" });

    const payload = jwt.verify(token, JWT_SECRET) as { address: string };
    (req as any).user = { address: payload.address };
    return next();
  } catch {
    return res.status(401).json({ error: "Invalid token" });
  }
}

/**
 * GET /auth/me
 * headers: Authorization: Bearer <token>
 */
router.get("/me", requireAuth, (req: Request, res: Response) => {
  return res.json({ address: (req as any).user?.address || null });
});

/**
 * POST /auth/challenge
 * body: { address }
 */
router.post("/challenge", async (req: Request, res: Response) => {
  const address = safeAddress(req.body?.address);
  if (!address) return res.status(400).json({ error: "Missing address" });

  const nonce = randNonce();
  const nowIso = new Date().toISOString();

  // Lore-friendly but still clear "sign-in only"
  const message =
    `PLANET FATNESS — GYM CHECK-IN\n` +
    `Wallet: ${address}\n` +
    `Nonce: ${nonce}\n` +
    `Time: ${nowIso}\n` +
    `\n` +
    `Sign to prove you own this wallet.\n` +
    `No transactions. No approvals. No spending.`;

  challenges.set(address, { nonce, message, exp: Date.now() + 5 * 60 * 1000 });

  return res.json({ nonce, message, exp: Date.now() + 5 * 60 * 1000 });
});

/**
 * POST /auth/verify
 * body: { address, nonce, signature }
 * signature: base64 (recommended) OR base58 (fallback)
 */
router.post("/verify", async (req: Request, res: Response) => {
  if (!JWT_SECRET) return res.status(500).json({ error: "Server missing JWT_SECRET" });

  const address = safeAddress(req.body?.address);
  const nonce = safeAddress(req.body?.nonce);
  const signature = safeAddress(req.body?.signature);

  if (!address || !nonce || !signature) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const ch = challenges.get(address);
  if (!ch) return res.status(400).json({ error: "No challenge found. Reconnect." });

  if (Date.now() > ch.exp) {
    challenges.delete(address);
    return res.status(400).json({ error: "Challenge expired. Reconnect." });
  }

  if (nonce !== ch.nonce) {
    return res.status(400).json({ error: "Bad nonce. Reconnect." });
  }

  try {
    // Solana pubkey base58 -> 32 bytes
    const pubkeyBytes = bs58.decode(address);

    // Signature decode (base64 or base58)
    const sigBytes = decodeSignature(signature);

    // Message bytes
    const msgBytes = new TextEncoder().encode(ch.message);

    const ok = nacl.sign.detached.verify(msgBytes, sigBytes, pubkeyBytes);
    if (!ok) return res.status(401).json({ error: "Signature failed" });

    // Ensure user exists
    await upsertUser(address);

    challenges.delete(address);

    const token = signToken(address);
    return res.json({ token, address });
  } catch (e) {
    console.error("verify error:", e);
    return res.status(400).json({ error: "Verify error" });
  }
});

/* =========================================================
   TELEGRAM WEB APP AUTH
   POST /auth/telegram
   body: { initData }  // Telegram.WebApp.initData (raw querystring)
   Returns: { token, address, telegram: {...}, displayName }
   ========================================================= */

function parseInitData(initData: string): Record<string, string> {
  const out: Record<string, string> = {};
  const s = String(initData || "").trim();
  if (!s) return out;

  // initData is querystring-like: key=value&key2=value2...
  const parts = s.split("&");
  for (const p of parts) {
    const eq = p.indexOf("=");
    if (eq === -1) continue;
    const k = decodeURIComponent(p.slice(0, eq));
    const v = decodeURIComponent(p.slice(eq + 1));
    out[k] = v;
  }
  return out;
}

function verifyTelegramInitData(initData: string, botToken: string): { ok: true; data: any } | { ok: false; error: string } {
  const data = parseInitData(initData);
  const hash = data.hash;
  if (!hash) return { ok: false, error: "missing_hash" };

  // Build data_check_string: sorted "key=value" lines excluding hash
  const keys = Object.keys(data).filter((k) => k !== "hash").sort();
  const dataCheckString = keys.map((k) => `${k}=${data[k]}`).join("\n");

  // secret_key = sha256(botToken)
  const secretKey = crypto.createHash("sha256").update(botToken).digest();

  // HMAC_SHA256(data_check_string, secret_key) => hex
  const computed = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex");

  if (computed !== hash) return { ok: false, error: "bad_hash" };

  // auth_date freshness check (optional but recommended)
  const authDate = Number(data.auth_date || 0);
  if (!authDate) return { ok: false, error: "missing_auth_date" };

  const ageSec = Math.floor(Date.now() / 1000) - authDate;
  if (TG_MAX_AGE_SEC > 0 && ageSec > TG_MAX_AGE_SEC) return { ok: false, error: "initdata_too_old" };

  // user field is JSON
  let user: any = null;
  try {
    user = data.user ? JSON.parse(data.user) : null;
  } catch {
    user = null;
  }
  if (!user || !user.id) return { ok: false, error: "missing_user" };

  return { ok: true, data: { ...data, user } };
}

router.post("/telegram", async (req: Request, res: Response) => {
  try {
    if (!JWT_SECRET) return res.status(500).json({ error: "Server missing JWT_SECRET" });
    if (!TG_BOT_TOKEN) return res.status(500).json({ error: "Server missing TG_BOT_TOKEN" });

    const initData = String(req.body?.initData || "").trim();
    if (!initData) return res.status(400).json({ error: "Missing initData" });

    const v = verifyTelegramInitData(initData, TG_BOT_TOKEN);
    if (!v.ok) return res.status(401).json({ error: `tg_${v.error}` });

    const tgUser = v.data.user as any;
    const tgId = String(tgUser.id);
    const username = tgUser.username ? String(tgUser.username) : "";
    const firstName = tgUser.first_name ? String(tgUser.first_name) : "";
    const lastName = tgUser.last_name ? String(tgUser.last_name) : "";

    // Telegram-first identity "address" (fits your existing DB + JWT model)
    const address = `tg:${tgId}`;

    await upsertUser(address);

    // Prefer @username if available, otherwise fallback to first/last
    const displayName =
      username ? `@${username}` : [firstName, lastName].filter(Boolean).join(" ").slice(0, 24);

    // Only set if we have something usable
    if (displayName && displayName.trim().length >= 2) {
      try {
        await setDisplayName({ address, displayName });
      } catch {
        // ignore name failures
      }
    }

    const token = signToken(address);

    return res.json({
      ok: true,
      token,
      address,
      displayName: displayName || null,
      telegram: {
        id: tgUser.id,
        username: username || null,
        first_name: firstName || null,
        last_name: lastName || null,
        language_code: tgUser.language_code || null,
      },
    });
  } catch (e) {
    console.error("telegram auth error:", e);
    return res.status(500).json({ error: "Telegram auth failed" });
  }
});

export default router;