import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "dev_secret";

export function signToken(address: string) {
  return jwt.sign({ address }, JWT_SECRET, { expiresIn: "30d" });
}

export function authMiddleware(
  req: Request,
  res: Response,
  next: NextFunction
) {
  const auth = req.headers.authorization;
  if (!auth) return res.status(401).json({ error: "No token" });

  const token = auth.replace("Bearer ", "");
  try {
    const decoded = jwt.verify(token, JWT_SECRET) as { address: string };
    (req as any).user = decoded.address;
    next();
  } catch {
    return res.status(401).json({ error: "Invalid token" });
  }
}