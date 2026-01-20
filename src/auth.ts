import { Router, Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import nacl from "tweetnacl";
import bs58 from "bs58";
import { upsertUser } from "./db.js";

const router = Router();

const JWT_SECRET = process.env.JWT_SECRET || "";
if (!JWT_SECRET) {
  console.warn("⚠️ Missing JWT_SECRET (set it in Render env vars). Auth verify will fail.");
}

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

export default router;