import { Router, Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import nacl from "tweetnacl";
import bs58 from "bs58";
import { query } from "./db";

const router = Router();

const JWT_SECRET = process.env.JWT_SECRET!;
if (!JWT_SECRET) {
  throw new Error("JWT_SECRET not set");
}

// In-memory nonce store (simple + safe for now)
const nonces = new Map<string, string>();

/**
 * POST /auth/challenge
 * Body: { address }
 */
router.post("/challenge", async (req: Request, res: Response) => {
  const { address } = req.body;
  if (!address) {
    return res.status(400).json({ error: "Missing address" });
  }

  const nonce = Math.random().toString(36).slice(2);
  const message = `Planet Fatness Gym Login\n\nWallet:\n${address}\n\nNonce:\n${nonce}`;

  nonces.set(address, nonce);

  res.json({
    message,
    nonce,
  });
});

/**
 * POST /auth/verify
 * Body: { address, nonce, signature }
 */
router.post("/verify", async (req: Request, res: Response) => {
  const { address, nonce, signature } = req.body;

  if (!address || !nonce || !signature) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const expected = nonces.get(address);
  if (!expected || expected !== nonce) {
    return res.status(401).json({ error: "Invalid or expired nonce" });
  }

  try {
    const message = `Planet Fatness Gym Login\n\nWallet:\n${address}\n\nNonce:\n${nonce}`;
    const msgBytes = new TextEncoder().encode(message);

    const sigBytes = Uint8Array.from(atob(signature), c => c.charCodeAt(0));
    const pubKey = bs58.decode(address);

    const valid = nacl.sign.detached.verify(
      msgBytes,
      sigBytes,
      pubKey
    );

    if (!valid) {
      return res.status(401).json({ error: "Signature invalid" });
    }

    // Ensure user exists
    await query(
      `
      INSERT INTO users (address)
      VALUES ($1)
      ON CONFLICT (address) DO NOTHING
      `,
      [address]
    );

    const token = jwt.sign(
      { address },
      JWT_SECRET,
      { expiresIn: "7d" }
    );

    nonces.delete(address);

    res.json({ token });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Auth failed" });
  }
});

/**
 * Middleware: requireAuth
 */
export function requireAuth(req: Request, res: Response, next: NextFunction) {
  const header = req.headers.authorization;
  if (!header?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Missing token" });
  }

  try {
    const token = header.replace("Bearer ", "");
    const decoded = jwt.verify(token, JWT_SECRET) as { address: string };
    (req as any).user = decoded;
    next();
  } catch {
    res.status(401).json({ error: "Invalid token" });
  }
}

export default router;