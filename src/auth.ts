import { Router, Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import nacl from "tweetnacl";
import bs58 from "bs58";
import { upsertUser } from "./db.js";

const router = Router();

const JWT_SECRET = process.env.JWT_SECRET || "";
if (!JWT_SECRET) {
  // Fail loudly so you don't think auth is working when it's not
  console.warn("⚠️ Missing JWT_SECRET (set it in Render env vars).");
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

export function requireAuth(req: Request, res: Response, next: NextFunction) {
  const h = req.headers.authorization || "";
  const token = h.startsWith("Bearer ") ? h.slice(7) : "";
  if (!token) return res.status(401).json({ error: "Missing token" });

  try {
    const payload = jwt.verify(token, JWT_SECRET) as { address: string };
    (req as any).user = { address: payload.address };
    return next();
  } catch {
    return res.status(401).json({ error: "Invalid token" });
  }
}

/**
 * POST /auth/challenge
 * body: { address }
 */
router.post("/challenge", async (req: Request, res: Response) => {
  const address = String(req.body?.address || "").trim();
  if (!address) return res.status(400).json({ error: "Missing address" });

  const nonce = randNonce();
  const message =
    `Planet Fatness Gym Login\n` +
    `Wallet: ${address}\n` +
    `Nonce: ${nonce}\n` +
    `Only sign this message to prove you own this wallet.`;

  challenges.set(address, { nonce, message, exp: Date.now() + 5 * 60 * 1000 });

  res.json({ nonce, message });
});

/**
 * POST /auth/verify
 * body: { address, nonce, signature }  signature is base64 string from frontend
 */
router.post("/verify", async (req: Request, res: Response) => {
  const address = String(req.body?.address || "").trim();
  const nonce = String(req.body?.nonce || "").trim();
  const signatureB64 = String(req.body?.signature || "").trim();

  if (!address || !nonce || !signatureB64) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const ch = challenges.get(address);
  if (!ch) return res.status(400).json({ error: "No challenge found. Reconnect." });
  if (Date.now() > ch.exp) {
    challenges.delete(address);
    return res.status(400).json({ error: "Challenge expired. Reconnect." });
  }
  if (nonce !== ch.nonce) return res.status(400).json({ error: "Bad nonce. Reconnect." });

  try {
    const msgBytes = new TextEncoder().encode(ch.message);

    const sigBytes = Uint8Array.from(atob(signatureB64), (c) => c.charCodeAt(0));
    const pubkeyBytes = bs58.decode(address); // Solana pubkey base58 -> 32 bytes

    const ok = nacl.sign.detached.verify(msgBytes, sigBytes, pubkeyBytes);
    if (!ok) return res.status(401).json({ error: "Signature failed" });

    // Ensure user exists
    await upsertUser(address);

    challenges.delete(address);

    if (!JWT_SECRET) return res.status(500).json({ error: "Server missing JWT_SECRET" });
    const token = signToken(address);
    return res.json({ token });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ error: "Verify error" });
  }
});

export default router;