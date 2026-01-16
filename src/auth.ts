import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "dev-secret-change-me";

export function signToken(address: string) {
  return jwt.sign({ address }, JWT_SECRET, { expiresIn: "7d" });
}

export function verifyToken(token: string): { address: string } {
  return jwt.verify(token, JWT_SECRET) as any;
}