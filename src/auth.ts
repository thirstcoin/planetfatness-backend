// src/auth.ts
import { Router, Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import nacl from "tweetnacl";
import bs58 from "bs58";
import crypto from "crypto";
import { upsertUser, getMe, setDisplayName, setTelegramIdentity } from "./db.js";

const router = Router();

const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
if (!JWT_SECRET) {
  console.warn("⚠️ Missing JWT_SECRET (set it in Render env vars). Auth verify will fail.");
}

/**
 * ✅ Telegram Bot Token (from BotFather)
 * Most common tg_bad_hash cause: token has trailing newline/space or accidental quotes in env.
 * We hard-clean it here.
 */
const TG_BOT_TOKEN = String(process.env.TG_BOT_TOKEN || "")
  .trim()
  .replace(/^\s+|\s+$/g, "")
  .replace(/^"+|"+$/g, "") // strip accidental quotes
  .replace(/^'+|'+$/g, ""); // strip accidental quotes

if (!TG_BOT_TOKEN) {
  console.warn("⚠️ Missing TG_BOT_TOKEN. /auth/telegram will fail until you set it.");
} else {
  // Safe diagnostics (does NOT print the token)
  const hasNewline = TG_BOT_TOKEN.includes("\n") || TG_BOT_TOKEN.includes("\r");
  const hasSpace = TG_BOT_TOKEN.includes(" ");
  console.log(
    "[TG] TG_BOT_TOKEN len:",
    TG_BOT_TOKEN.length,
    "has_newline:",
    hasNewline,
    "has_space:",
    hasSpace
  );
}

// Optional: how old initData can be (seconds). Default 1 day.
const TG_MAX_AGE_SEC = Number(process.env.TG_MAX_AGE_SEC || 86400);

// Optional debug logs (set TG_DEBUG=1 in Render to see TG verify details)
const TG_DEBUG = String(process.env.TG_DEBUG || "").trim() === "1";

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

function safeStr(a: any) {
  return String(a || "").trim();
}

/**
 * Supports signatures sent as:
 * - base64 string (recommended)
 * - base58 string (fallback)
 */
function decodeSignature(sig: string): Uint8Array {
  const s = safeStr(sig);
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
 * ✅ returns profile info (display name + tg fields)
 */
router.get("/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address || null;
  if (!address) return res.json({ address: null });

  try {
    const me = await getMe(address);

    return res.json({
      ok: true,
      address,
      profile: {
        displayName: me?.display_name || null,
        tgId: me?.tg_id ?? null,
        tgUsername: me?.tg_username ?? null,
        tgFirstName: me?.tg_first_name ?? null,
        tgLastName: me?.tg_last_name ?? null,
      },
    });
  } catch (e) {
    console.error("me error:", e);
    return res.json({ ok: true, address, profile: { displayName: null } });
  }
});

/**
 * POST /auth/challenge
 * body: { address }
 */
router.post("/challenge", async (req: Request, res: Response) => {
  const address = safeStr(req.body?.address);
  if (!address) return res.status(400).json({ error: "Missing address" });

  const nonce = randNonce();
  const nowIso = new Date().toISOString();

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
 */
router.post("/verify", async (req: Request, res: Response) => {
  if (!JWT_SECRET) return res.status(500).json({ error: "Server missing JWT_SECRET" });

  const address = safeStr(req.body?.address);
  const nonce = safeStr(req.body?.nonce);
  const signature = safeStr(req.body?.signature);

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
    const pubkeyBytes = bs58.decode(address);
    const sigBytes = decodeSignature(signature);
    const msgBytes = new TextEncoder().encode(ch.message);

    const ok = nacl.sign.detached.verify(msgBytes, sigBytes, pubkeyBytes);
    if (!ok) return res.status(401).json({ error: "Signature failed" });

    await upsertUser(address);
    challenges.delete(address);

    const token = signToken(address);
    return res.json({ ok: true, token, address });
  } catch (e) {
    console.error("verify error:", e);
    return res.status(400).json({ error: "Verify error" });
  }
});

/* =========================================================
   TELEGRAM WEB APP AUTH
   POST /auth/telegram
   body: { initData }  // Telegram.WebApp.initData (raw querystring)
   Returns: { token, address, profile, telegram }
   ========================================================= */

function parseInitData(initData: string): Record<string, string> {
  const out: Record<string, string> = {};
  let s = safeStr(initData);
  if (!s) return out;

  // Sometimes people accidentally pass "?a=b&c=d"
  if (s.startsWith("?")) s = s.slice(1);

  // ✅ URLSearchParams handles + and % decoding the way Telegram expects
  const usp = new URLSearchParams(s);
  usp.forEach((v, k) => {
    out[k] = v;
  });
  return out;
}

function buildDataCheckString(data: Record<string, string>) {
  const keys = Object.keys(data).filter((k) => k !== "hash").sort();
  return { keys, dataCheckString: keys.map((k) => `${k}=${data[k]}`).join("\n") };
}

function verifyTelegramInitData(
  initData: string,
  botToken: string
): { ok: true; data: any } | { ok: false; error: string } {
  const data = parseInitData(initData);
  const hash = data.hash;
  if (!hash) return { ok: false, error: "missing_hash" };

  const { keys, dataCheckString } = buildDataCheckString(data);

  // ✅ Per Telegram docs: secret_key = sha256(bot_token)
  const secretKey = crypto.createHash("sha256").update(botToken).digest();
  const computed = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex");

  if (TG_DEBUG) {
    console.log("[TG_VERIFY] initDataLen:", safeStr(initData).length);
    console.log("[TG_VERIFY] keys:", keys.join(","));
    console.log("[TG_VERIFY] dataCheckString:\n" + dataCheckString);
    console.log("[TG_VERIFY] providedHash:", hash);
    console.log("[TG_VERIFY] computedHash:", computed);
    console.log(
      "[TG_VERIFY] botTokenLen:",
      botToken.length,
      "botTokenHasNewline:",
      botToken.includes("\n") || botToken.includes("\r"),
      "botTokenHasSpace:",
      botToken.includes(" ")
    );
  }

  // ✅ Return actionable error that can surface on frontend toast
  if (computed !== hash) {
    return {
      ok: false,
      error: `bad_hash|provided=${String(hash).slice(0, 12)}|computed=${String(computed).slice(
        0,
        12
      )}|keys=${keys.length}|initLen=${safeStr(initData).length}`,
    };
  }

  const authDate = Number(data.auth_date || 0);
  if (!authDate) return { ok: false, error: "missing_auth_date" };

  const ageSec = Math.floor(Date.now() / 1000) - authDate;
  if (TG_MAX_AGE_SEC > 0 && ageSec > TG_MAX_AGE_SEC) return { ok: false, error: "initdata_too_old" };

  let user: any = null;
  try {
    user = data.user ? JSON.parse(data.user) : null;
  } catch {
    user = null;
  }
  if (!user || !user.id) return { ok: false, error: "missing_user" };

  return { ok: true, data: { ...data, user } };
}

/**
 * POST /auth/telegram
 * body: { initData }
 */
router.post("/telegram", async (req: Request, res: Response) => {
  try {
    if (!JWT_SECRET) return res.status(500).json({ error: "Server missing JWT_SECRET" });
    if (!TG_BOT_TOKEN) return res.status(500).json({ error: "Server missing TG_BOT_TOKEN" });

    const initData = safeStr(req.body?.initData);
    if (!initData) return res.status(400).json({ error: "Missing initData" });

    // ✅ Always verify with CLEAN token
    const v = verifyTelegramInitData(initData, TG_BOT_TOKEN);
    if (!v.ok) return res.status(401).json({ error: `tg_${v.error}` });

    const tgUser = v.data.user as any;

    const tgIdNum = Number(tgUser.id);
    const username = tgUser.username ? String(tgUser.username) : "";
    const firstName = tgUser.first_name ? String(tgUser.first_name) : "";
    const lastName = tgUser.last_name ? String(tgUser.last_name) : "";

    // Telegram-first identity
    const address = `tg:${tgIdNum}`;

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
      } catch {
        // ignore
      }
    }

    const token = signToken(address);
    const me = await getMe(address);

    return res.json({
      ok: true,
      token,
      address,
      profile: {
        displayName: me?.display_name || displayName || null,
        tgId: me?.tg_id ?? tgIdNum,
        tgUsername: me?.tg_username ?? (username || null),
        tgFirstName: me?.tg_first_name ?? (firstName || null),
        tgLastName: me?.tg_last_name ?? (lastName || null),
      },
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