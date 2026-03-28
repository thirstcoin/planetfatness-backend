import dotenv from "dotenv";
dotenv.config();

import type { Request, Response } from "express";
import { Telegraf, Markup } from "telegraf";
import jwt from "jsonwebtoken";
import crypto from "crypto";

// --- Dynamic imports (env ready before db.ts reads DATABASE_URL) ---
const expressMod = await import("express");
const express = expressMod.default;

const corsMod = await import("cors");
const cors = corsMod.default;

const solanaWeb3Mod = await import("@solana/web3.js");
const { Connection, PublicKey } = solanaWeb3Mod;

const authMod = await import("./auth.js");
const authRouter = authMod.default;
const requireAuth = authMod.requireAuth as any;

const dbMod = await import("./db.js");
const {
  initDb,
  getMe,
  addActivity,
  getLeaderboard,
  logSession,
  getLeaderboardV2,
  getActivitySummary,
  setDisplayName,
  setTelegramIdentity,
  upsertUser,
  pool,
  getBalance,
  creditBalance,
  debitBalance,
  hasDepositTxSignature,
  recordDeposit,
  createWithdrawal,
  recordGreedTaxLedger,
  getGreedJackpotState,
  setGreedJackpotAmount,
  addToGreedJackpot,
  reseedGreedJackpot,
  createGreedRound,
  getActiveGreedRound,
  getGreedRoundByIdForAddress,
  getGreedPickedIndices,
  recordGreedPick,
  acquireGreedRoundProcessingLock,
  releaseGreedRoundProcessingLock,
  updateGreedRoundProgress,
  closeGreedRoundAsPoison,
  closeGreedRoundAsCashout,
  getGreedLeaderboard,
  getGreedFeed,
  expireStaleGreedDepositIntents,
  getOpenGreedDepositIntentByAddress,
  getGreedDepositIntentByIdForAddress,
  createGreedDepositIntent,
  cancelGreedDepositIntent,
  consumeFundedGreedDepositIntent,
  markGreedDepositIntentFunded,
  findGreedDepositIntentByExactAmount,
} = dbMod;

// -------------------------------
// App + config
// -------------------------------
const app = express();

const PORT = Number(process.env.PORT || 10000);
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const TG_GAME_SHORT_NAME = String(process.env.TG_GAME_SHORT_NAME || "planetfatness").trim();
const ADMIN_SECRET = String(process.env.ADMIN_SECRET || "launch2026").trim();
const GREED_WEBAPP_URL = String(process.env.GREED_WEBAPP_URL || "https://planetfatness.fit/greed").trim();
const HUB_WEBAPP_URL = String(process.env.HUB_WEBAPP_URL || "https://planetfatness.fit/").trim();

const DEPOSIT_WALLET = String(process.env.DEPOSIT_WALLET || "").trim();
const PHAT_TOKEN_ACCOUNT = String(process.env.PHAT_TOKEN_ACCOUNT || "").trim();
const PHAT_TOKEN_MINT = String(process.env.PHAT_TOKEN_MINT || "").trim() || "PHAT";
const GREED_INTENT_EXPIRES_MINUTES = Math.max(
  1,
  Math.min(60, Number(process.env.GREED_INTENT_EXPIRES_MINUTES || 10))
);

// Solana watcher config
const SOLANA_RPC_URL = String(
  process.env.SOLANA_RPC_URL || process.env.RPC_URL || "https://api.mainnet-beta.solana.com"
).trim();

const SOLANA_WATCH_ENABLED =
  String(process.env.SOLANA_WATCH_ENABLED || "true").trim().toLowerCase() !== "false";

const SOLANA_WATCH_INTERVAL_MS = Math.max(
  2500,
  Math.min(20000, Number(process.env.SOLANA_WATCH_INTERVAL_MS || 4000))
);

const SOLANA_WATCH_SIGNATURE_LIMIT = Math.max(
  5,
  Math.min(50, Number(process.env.SOLANA_WATCH_SIGNATURE_LIMIT || 25))
);

// Spectator / live loop config
const GREED_SPECTATOR_ENABLED =
  String(process.env.GREED_SPECTATOR_ENABLED || "true").trim().toLowerCase() !== "false";

const GREED_SPECTATOR_CHAT_ID = String(process.env.GREED_SPECTATOR_CHAT_ID || "").trim();

const GREED_SHOUT_INTERVAL_MS = Math.max(
  15000,
  Math.min(180000, Number(process.env.GREED_SHOUT_INTERVAL_MS || 45000))
);

const GREED_MIN_IDLE_FOR_SHOUT_MS = Math.max(
  8000,
  Math.min(120000, Number(process.env.GREED_MIN_IDLE_FOR_SHOUT_MS || 20000))
);

app.use(express.json({ limit: "1mb" }));
app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? true : CORS_ORIGIN.split(",").map((s) => s.trim()),
    credentials: false,
  })
);

// -------------------------------
// Helpers
// -------------------------------
type GameKey = "runner" | "snack" | "lift" | "basket" | "greed";

type SpectatorRoundState = {
  roundId: number;
  address: string;
  displayName: string;
  wager: number;
  fundedExactAmount: number;
  chatId: string;
  startedAt: number;
  updatedAt: number;
  safeClicks: number;
  isActive: boolean;
};

const liveSpectatorRounds = new Map<number, SpectatorRoundState>();
let greedWatcherTimer: NodeJS.Timeout | null = null;
let greedShoutTimer: NodeJS.Timeout | null = null;
let greedWatcherBusy = false;
let greedWatcherLastSeenSignature = "";

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}

function asGameKey(x: unknown): GameKey {
  const g = String(x || "").toLowerCase().trim();
  if (g === "runner" || g === "snack" || g === "lift" || g === "basket" || g === "greed") return g as GameKey;
  return "snack";
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeWindow(w: unknown): "lifetime" | "day" | "week" | "month" {
  const s = String(w || "lifetime").toLowerCase().trim();
  if (s === "daily") return "day";
  if (s === "weekly") return "week";
  if (s === "monthly") return "month";
  if (s === "day" || s === "week" || s === "month" || s === "lifetime") return s;
  return "lifetime";
}

function signTokenForAddress(address: string) {
  const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
  if (!JWT_SECRET) throw new Error("missing_jwt_secret");
  return jwt.sign({ address }, JWT_SECRET, { expiresIn: "30d" });
}

function sha256Hex(input: string) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

function requireAdmin(req: Request, res: Response): boolean {
  const headerSecret = String(req.headers["x-admin-secret"] || "").trim();
  const querySecret = String(req.query.secret || "").trim();
  const bodySecret = String((req.body as { secret?: string } | undefined)?.secret || "").trim();
  const provided = headerSecret || querySecret || bodySecret;

  if (!ADMIN_SECRET || provided !== ADMIN_SECRET) {
    res.status(403).json({ error: "Unauthorized" });
    return false;
  }
  return true;
}

function round3(n: number) {
  return Number(Number(n || 0).toFixed(3));
}

function formatAmount3(n: number | string) {
  const value = Number(n || 0);
  if (!Number.isFinite(value)) return "0.000";
  return value.toFixed(3);
}

function parseAmount3(raw: unknown) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  return round3(value);
}

function sanitizeWager(raw: unknown) {
  const wager = Math.floor(Number(raw));
  if (!Number.isFinite(wager)) return null;
  if (wager < GREED_MIN_WAGER || wager > GREED_MAX_WAGER) return null;
  return wager;
}

function serializeGreedIntent(row: any) {
  if (!row) return null;
  return {
    id: Number(row.id),
    address: row.address,
    status: String(row.status || "pending"),
    requestedWager: Number(row.requested_wager || 0),
    exactAmount: Number(row.exact_amount || 0),
    depositWallet: row.deposit_wallet || null,
    senderWallet: row.sender_wallet || null,
    tokenMint: row.token_mint || null,
    txSignature: row.tx_signature || null,
    expiresAt: row.expires_at || null,
    fundedAt: row.funded_at || null,
    startedAt: row.started_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };
}

function maskAddress(address: string) {
  const a = String(address || "").trim();
  if (!a) return "Unknown";
  if (a.startsWith("tg:")) return a;
  if (a.length <= 10) return a;
  return `${a.slice(0, 4)}...${a.slice(-4)}`;
}

function displayNameFromUserRow(userRow: any, fallbackAddress: string) {
  const direct = String(userRow?.display_name || "").trim();
  if (direct) return direct;

  const tgUsername = String(userRow?.tg_username || "").trim();
  if (tgUsername) return `@${tgUsername.replace(/^@/, "")}`;

  const first = String(userRow?.tg_first_name || "").trim();
  const last = String(userRow?.tg_last_name || "").trim();
  const combined = [first, last].filter(Boolean).join(" ").trim();
  if (combined) return combined;

  return maskAddress(fallbackAddress);
}

async function getDisplayNameForAddress(address: string) {
  try {
    const me = await getMe(address);
    return displayNameFromUserRow(me, address);
  } catch {
    return maskAddress(address);
  }
}

async function generateUniqueExactAmount(requestedWager: number) {
  const base = Math.floor(requestedWager);

  for (let i = 0; i < 60; i++) {
    const decimalPart = Math.floor(Math.random() * 999) + 1;
    const exact = `${base}.${String(decimalPart).padStart(3, "0")}`;

    const existing = await pool.query(
      `
      SELECT 1
      FROM greed_deposit_intents
      WHERE exact_amount = $1::numeric
        AND status IN ('pending', 'funded')
        AND expires_at > NOW()
      LIMIT 1;
      `,
      [exact]
    );

    if ((existing.rowCount || 0) === 0) {
      return Number(exact);
    }
  }

  for (let decimalPart = 1; decimalPart <= 999; decimalPart++) {
    const exact = `${base}.${String(decimalPart).padStart(3, "0")}`;

    const existing = await pool.query(
      `
      SELECT 1
      FROM greed_deposit_intents
      WHERE exact_amount = $1::numeric
        AND status IN ('pending', 'funded')
        AND expires_at > NOW()
      LIMIT 1;
      `,
      [exact]
    );

    if ((existing.rowCount || 0) === 0) {
      return Number(exact);
    }
  }

  throw new Error("Could not generate unique exact funding amount");
}

function getSpectatorChatIdFromReq(req: Request) {
  const bodyChatId = String((req.body as any)?.spectatorChatId || "").trim();
  const queryChatId = String(req.query?.spectatorChatId || "").trim();
  return bodyChatId || queryChatId || GREED_SPECTATOR_CHAT_ID || "";
}

async function sendGymSpectatorMessage(text: string, extra?: Record<string, any>) {
  if (!GREED_SPECTATOR_ENABLED) return;
  if (!GREED_SPECTATOR_CHAT_ID) return;
  if (!process.env.TG_BOT_TOKEN) return;

  try {
    await gymBot.telegram.sendMessage(GREED_SPECTATOR_CHAT_ID, text, {
      ...(extra || {}),
    });
  } catch (e) {
    console.error("Spectator message failed:", e);
  }
}

async function sendGymSpectatorMessageToChat(chatId: string, text: string, extra?: Record<string, any>) {
  if (!GREED_SPECTATOR_ENABLED) return;
  if (!chatId) return;
  if (!process.env.TG_BOT_TOKEN) return;

  try {
    await gymBot.telegram.sendMessage(chatId, text, {
      ...(extra || {}),
    });
  } catch (e) {
    console.error("Spectator message to chat failed:", e);
  }
}

function pickCardEmojis() {
  return ["🍩", "🍩", "🍩", "🍩", "🍩", "🍩", "🍩", "🍩", "🍩", "🍩", "🍩", "🍩"];
}

function formatDonutBoardLine() {
  const donuts = pickCardEmojis();
  return donuts.map((d, i) => `${d}${i + 1}`).join("  ");
}

function safeMultiplierLabel(safeClicks: number) {
  if (safeClicks <= 0) return "x1.00";
  return `x${GREED_MULTIPLIERS[Math.min(safeClicks - 1, GREED_MULTIPLIERS.length - 1)].toFixed(2)}`;
}

const shoutTemplates = [
  "Chat is screaming for donut #{pick}.",
  "The room says #{pick} is blessed.",
  "A degen in the crowd is demanding donut #{pick}.",
  "Phil is staring hard at donut #{pick}.",
  "The glaze committee is leaning toward #{pick}.",
  "Crowd temperature says donut #{pick} looks lucky.",
  "A couch-certified analyst likes #{pick}.",
  "The gym floor is yelling for #{pick}.",
  "Suspiciously many people are calling #{pick}.",
  "Greed radar says #{pick} might print.",
];

function randomInt(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function buildRandomShout(state: SpectatorRoundState) {
  const pick = randomInt(1, 12);
  const template = shoutTemplates[randomInt(0, shoutTemplates.length - 1)];
  return `🍩 LIVE GREED • ${state.displayName}\n${template.replace("#{pick}", String(pick))}\nCurrent safe picks: ${state.safeClicks} • Current target: ${safeMultiplierLabel(state.safeClicks)}`;
}

function registerLiveRound(state: SpectatorRoundState) {
  liveSpectatorRounds.set(state.roundId, state);
}

function updateLiveRound(roundId: number, patch: Partial<SpectatorRoundState>) {
  const existing = liveSpectatorRounds.get(roundId);
  if (!existing) return;
  liveSpectatorRounds.set(roundId, {
    ...existing,
    ...patch,
    updatedAt: Date.now(),
  });
}

function closeLiveRound(roundId: number) {
  const existing = liveSpectatorRounds.get(roundId);
  if (!existing) return;
  liveSpectatorRounds.set(roundId, {
    ...existing,
    isActive: false,
    updatedAt: Date.now(),
  });
  liveSpectatorRounds.delete(roundId);
}

function startGreedShoutLoop() {
  if (!GREED_SPECTATOR_ENABLED) return;
  if (!GREED_SPECTATOR_CHAT_ID) return;
  if (!process.env.TG_BOT_TOKEN) return;

  if (greedShoutTimer) {
    clearInterval(greedShoutTimer);
  }

  greedShoutTimer = setInterval(async () => {
    try {
      const now = Date.now();

      for (const state of liveSpectatorRounds.values()) {
        if (!state.isActive) continue;
        if (now - state.updatedAt < GREED_MIN_IDLE_FOR_SHOUT_MS) continue;

        const msg = buildRandomShout(state);
        await sendGymSpectatorMessageToChat(state.chatId, msg);
        updateLiveRound(state.roundId, {});
      }
    } catch (e) {
      console.error("Greed shout loop failed:", e);
    }
  }, GREED_SHOUT_INTERVAL_MS);
}

// -------------------------------
// Solana watcher helpers
// -------------------------------
const solanaConnection = SOLANA_WATCH_ENABLED ? new Connection(SOLANA_RPC_URL, "confirmed") : null;

function extractAccountKeyStrings(tx: any): string[] {
  const keys = tx?.transaction?.message?.accountKeys || [];
  return keys.map((k: any) => {
    if (typeof k === "string") return k;
    if (k?.pubkey?.toBase58) return k.pubkey.toBase58();
    if (typeof k?.pubkey === "string") return k.pubkey;
    return "";
  });
}

function getRawAmountFromTokenBalanceEntry(entry: any): bigint {
  try {
    const raw = String(entry?.uiTokenAmount?.amount || "0");
    return BigInt(raw);
  } catch {
    return 0n;
  }
}

function getDecimalsFromTokenBalanceEntry(entry: any): number {
  const decimals = Number(entry?.uiTokenAmount?.decimals ?? 0);
  return Number.isFinite(decimals) ? Math.max(0, decimals) : 0;
}

function bigintAmountToDecimal(raw: bigint, decimals: number): number {
  const sign = raw < 0n ? -1 : 1;
  const abs = raw < 0n ? -raw : raw;
  const divisor = 10 ** decimals;
  const whole = Number(abs / BigInt(divisor));
  const fraction = Number(abs % BigInt(divisor)) / divisor;
  return sign * (whole + fraction);
}

function extractObservedDepositFromParsedTx(parsedTx: any): {
  exactAmount: number | null;
  senderWallet: string | null;
  tokenMint: string | null;
} {
  const meta = parsedTx?.meta;
  const tx = parsedTx?.transaction;
  if (!meta || !tx) {
    return { exactAmount: null, senderWallet: null, tokenMint: null };
  }

  const accountKeys = extractAccountKeyStrings(parsedTx);
  const preTokenBalances = Array.isArray(meta.preTokenBalances) ? meta.preTokenBalances : [];
  const postTokenBalances = Array.isArray(meta.postTokenBalances) ? meta.postTokenBalances : [];

  const balanceByIndexPre = new Map<number, any>();
  const balanceByIndexPost = new Map<number, any>();

  for (const entry of preTokenBalances) {
    const idx = Number(entry?.accountIndex);
    if (Number.isFinite(idx)) balanceByIndexPre.set(idx, entry);
  }

  for (const entry of postTokenBalances) {
    const idx = Number(entry?.accountIndex);
    if (Number.isFinite(idx)) balanceByIndexPost.set(idx, entry);
  }

  let bestMatch: { exactAmount: number; tokenMint: string | null } | null = null;

  const watcherTarget = PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET;

  const candidateIndexes = new Set<number>([
    ...Array.from(balanceByIndexPre.keys()),
    ...Array.from(balanceByIndexPost.keys()),
  ]);

  for (const idx of candidateIndexes) {
    const accountAddress = String(accountKeys[idx] || "");
    if (!accountAddress || accountAddress !== watcherTarget) continue;

    const pre = balanceByIndexPre.get(idx) || null;
    const post = balanceByIndexPost.get(idx) || null;

    const mint = String(post?.mint || pre?.mint || "").trim() || null;
    if (PHAT_TOKEN_MINT && mint && mint !== PHAT_TOKEN_MINT) continue;

    const decimals = getDecimalsFromTokenBalanceEntry(post || pre);
    const preRaw = getRawAmountFromTokenBalanceEntry(pre);
    const postRaw = getRawAmountFromTokenBalanceEntry(post);
    const deltaRaw = postRaw - preRaw;

    if (deltaRaw <= 0n) continue;

    const delta = round3(bigintAmountToDecimal(deltaRaw, decimals));
    if (delta <= 0) continue;

    bestMatch = {
      exactAmount: delta,
      tokenMint: mint,
    };
    break;
  }

  const senderWallet =
    accountKeys.find((k) => !!k && k !== DEPOSIT_WALLET && k !== watcherTarget) || null;

  return {
    exactAmount: bestMatch?.exactAmount ?? null,
    senderWallet,
    tokenMint: bestMatch?.tokenMint ?? PHAT_TOKEN_MINT ?? null,
  };
}

async function processSolanaDepositSignature(signature: string) {
  if (!signature) return;

  const alreadyProcessed = await hasDepositTxSignature(signature);
  if (alreadyProcessed) return;

  if (!solanaConnection) return;

  const parsedTx = await solanaConnection.getParsedTransaction(signature, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });

  if (!parsedTx || parsedTx.meta?.err) return;

  const observed = extractObservedDepositFromParsedTx(parsedTx);
  if (!observed.exactAmount || observed.exactAmount <= 0) return;

  const exactAmount = Number(formatAmount3(observed.exactAmount));
  const matchingIntent = await findGreedDepositIntentByExactAmount({
    exactAmount,
    status: "pending",
  });

  if (!matchingIntent) return;

  const funded = await markGreedDepositIntentFunded({
    id: Number(matchingIntent.id),
    address: String(matchingIntent.address),
    txSignature: signature,
    senderWallet: observed.senderWallet,
    tokenMint: observed.tokenMint || PHAT_TOKEN_MINT,
  });

  if (!funded) return;

  await recordDeposit({
    address: String(funded.address),
    txSignature: signature,
    senderWallet: observed.senderWallet,
    tokenMint: observed.tokenMint || PHAT_TOKEN_MINT,
    amount: Number(funded.exact_amount || exactAmount),
    status: "credited",
    note: "greed intent funded by watcher",
  });

  console.log(
    `✅ Greed watcher funded intent #${funded.id} for ${funded.address} with ${formatAmount3(
      funded.exact_amount || exactAmount
    )} ${observed.tokenMint || PHAT_TOKEN_MINT}`
  );
}

async function tickGreedSolanaWatcher() {
  const watcherTarget = PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET;
  if (!SOLANA_WATCH_ENABLED || !solanaConnection || !watcherTarget) return;
  if (greedWatcherBusy) return;

  greedWatcherBusy = true;

  try {
    const watcherPk = new PublicKey(watcherTarget);
    const signatures = await solanaConnection.getSignaturesForAddress(
      watcherPk,
      { limit: SOLANA_WATCH_SIGNATURE_LIMIT },
      "confirmed"
    );

    if (!signatures.length) return;

    const newestSignature = signatures[0]?.signature || "";
    const ordered = [...signatures].reverse();

    for (const entry of ordered) {
      if (!entry?.signature) continue;
      if (greedWatcherLastSeenSignature && entry.signature === greedWatcherLastSeenSignature) continue;
      await processSolanaDepositSignature(entry.signature);
    }

    greedWatcherLastSeenSignature = newestSignature;
  } catch (e) {
    console.error("❌ Greed Solana watcher tick failed:", e);
  } finally {
    greedWatcherBusy = false;
  }
}

function startGreedSolanaWatcher() {
  const watcherTarget = PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET;

  if (!SOLANA_WATCH_ENABLED) {
    console.warn("⚠️ SOLANA_WATCH_ENABLED=false. Greed watcher disabled.");
    return;
  }

  if (!DEPOSIT_WALLET) {
    console.warn("⚠️ No DEPOSIT_WALLET set. Greed watcher disabled.");
    return;
  }

  if (!watcherTarget) {
    console.warn("⚠️ No watcher target set. Greed watcher disabled.");
    return;
  }

  if (!PHAT_TOKEN_MINT) {
    console.warn("⚠️ No PHAT_TOKEN_MINT set. Greed watcher disabled.");
    return;
  }

  console.log(`💸 User deposit wallet: ${DEPOSIT_WALLET}`);
  console.log(`👀 Greed watcher target: ${watcherTarget}`);
  console.log(`🔗 RPC: ${SOLANA_RPC_URL}`);
  console.log(`⏱️ Watch interval: ${SOLANA_WATCH_INTERVAL_MS}ms`);

  tickGreedSolanaWatcher().catch((e) => console.error("Initial watcher tick failed:", e));

  greedWatcherTimer = setInterval(() => {
    tickGreedSolanaWatcher().catch((e) => console.error("Watcher interval failed:", e));
  }, SOLANA_WATCH_INTERVAL_MS);
}

// -------------------------------
// Other game rules
// -------------------------------
const COMMON_RULES = {
  minDurationMs: 10_000,
  maxRunCalories: 180,
  dailyCapCalories: 1200,
  cpmCap: 220,
};

const RULES: Record<
  string,
  {
    minDurationMs: number;
    maxRunCalories: number;
    dailyCapCalories: number;
    cpmCap: number;
    maxScorePerRun: number;
  }
> = {
  runner: { ...COMMON_RULES, maxScorePerRun: 0 },
  snack: { ...COMMON_RULES, maxScorePerRun: 0 },
  lift: { ...COMMON_RULES, maxScorePerRun: 0 },
  basket: { ...COMMON_RULES, maxScorePerRun: 0 },
};

const DAILY_GOALS: Record<string, { label: string; goal: number; metric: "score" | "miles" | "seconds" }> = {
  snack: { label: "Daily Goal", goal: 30, metric: "score" },
  runner: { label: "Daily Goal", goal: 1, metric: "miles" },
  lift: { label: "Daily Goal", goal: 50, metric: "score" },
  basket: { label: "Daily Goal", goal: 20, metric: "score" },
};

async function getTodayAgg(address: string, game: GameKey) {
  const r = await pool.query(
    `
    SELECT
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN calories ELSE 0 END), 0) AS calories,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN miles ELSE 0 END), 0) AS miles,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN duration_ms ELSE 0 END), 0) AS duration_ms,
      COALESCE(SUM(CASE WHEN created_at >= date_trunc('day', NOW()) THEN score ELSE 0 END), 0) AS score,
      COALESCE(MAX(score), 0) AS best_score
    FROM sessions
    WHERE address=$1 AND game=$2
    `,
    [address, game]
  );

  const row = r.rows[0] || {};
  return {
    calories: Number(row.calories || 0),
    miles: Number(row.miles || 0),
    durationMs: Number(row.duration_ms || 0),
    score: Number(row.score || 0),
    bestScore: Number(row.best_score || 0),
  };
}

function computeEarnedCalories(params: { game: GameKey; score: number; miles: number; bestSeconds: number; durationMs: number }) {
  const { game } = params;

  if (game === "greed") {
    return { earnedCalories: 0, reason: "ok" as const, normalized: params };
  }

  const rules = RULES[game];
  const durationMs = Math.max(0, Math.floor(Number(params.durationMs || 0)));
  const durationMin = durationMs / 60000;

  const score = Math.max(0, Number(params.score || 0));
  const miles = Math.max(0, Number(params.miles || 0));
  const bestSeconds = Math.max(0, Number(params.bestSeconds || 0));

  if (durationMs < rules.minDurationMs) {
    return { earnedCalories: 0, reason: "too_short" as const, normalized: { score, miles, bestSeconds, durationMs } };
  }

  if (game !== "runner") {
    const scorePerMin = durationMin > 0 ? score / durationMin : score;

    const SCORE_PER_MIN_CAP: Record<string, number> = {
      runner: 999999,
      snack: 520,
      lift: 450,
      basket: 360,
    };

    if (scorePerMin > SCORE_PER_MIN_CAP[game]) {
      return {
        earnedCalories: 0,
        reason: "score_too_high" as const,
        normalized: { score, miles, bestSeconds, durationMs },
      };
    }
  }

  let base = 0;

  if (game === "snack") base = score * 2.2;
  else if (game === "basket") base = score * 1.8;
  else if (game === "lift") base = score * 2.0;
  else if (game === "runner") {
    base = miles * 110;
    if (!miles || miles <= 0) base = durationMin * 120;
  }

  const timeCap = durationMin * rules.cpmCap;
  const earnedCalories = Math.floor(clamp(base, 0, Math.min(timeCap, rules.maxRunCalories)));

  return { earnedCalories, reason: "ok" as const, normalized: { score, miles, bestSeconds, durationMs } };
}

function computeDailyGoalProgress(params: { game: GameKey; today: { score: number; miles: number; durationMs: number } }) {
  const g = DAILY_GOALS[params.game];
  if (!g) return { goal: 0, progress: 0, hit: false };

  let progress = 0;
  if (g.metric === "score") progress = Math.floor(params.today.score || 0);
  if (g.metric === "miles") progress = Number(params.today.miles || 0);
  if (g.metric === "seconds") progress = Math.floor((params.today.durationMs || 0) / 1000);

  const goal = g.goal;
  const hit = progress >= goal;
  return { goal, progress, hit };
}

// -------------------------------
// Greed config
// -------------------------------
const GREED_TAX = 0.05;
const GREED_JACKPOT_FEED_RATE = 0.00625;
const GREED_MIN_WAGER = 1000;
const GREED_MAX_WAGER = 50000;
const GREED_TOTAL_DONUTS = 12;
const GREED_POISON_COUNT = 2;
const GREED_JACKPOT_RESEED = 5000;
const GREED_MULTIPLIERS = [1.02, 1.07, 1.15, 1.30, 1.48, 1.70, 1.98, 2.28, 2.70, 3.50];

function getGreedMultiplierForSafeClicks(safeClicks: number) {
  if (safeClicks <= 0) return 1.0;
  return GREED_MULTIPLIERS[safeClicks - 1] || GREED_MULTIPLIERS[GREED_MULTIPLIERS.length - 1];
}

function derivePoisonIndicesFromSeed(seed: string, nonce: number, total: number, poisonCount: number) {
  const scores: Array<{ index: number; score: string }> = [];
  for (let i = 0; i < total; i++) {
    scores.push({
      index: i,
      score: sha256Hex(`${seed}:${nonce}:${i}`),
    });
  }

  scores.sort((a, b) => a.score.localeCompare(b.score));
  return scores.slice(0, poisonCount).map((x) => x.index).sort((a, b) => a - b);
}

function getGreedTaxBreakdown(lockedWager: number) {
  const totalTax = round3(lockedWager * GREED_TAX);
  const devCut = round3(totalTax * 0.4);
  const treasuryCut = round3(totalTax * 0.4);
  const jackpotCut = round3(totalTax - devCut - treasuryCut);
  const netStake = round3(lockedWager - totalTax);
  return { totalTax, devCut, treasuryCut, jackpotCut, netStake };
}

// -------------------------------
// Routes
// -------------------------------
app.get("/", (_req: Request, res: Response) => {
  res
    .status(200)
    .type("text/plain")
    .send(
      [
        "Planet Fatness Backend ✅",
        "",
        "Endpoints:",
        "  GET  /health",
        "  POST /auth/*",
        "  GET  /profile/me",
        "  POST /profile/name",
        "  GET  /activity/me",
        "  POST /activity/add",
        "  POST /activity/submit",
        "  GET  /activity/summary",
        "  GET  /daily/progress",
        "  GET  /leaderboard",
        "  GET  /leaderboard/v2",
        "  GET  /leaderboard/games",
        "  GET  /wallet/balance",
        "  GET  /wallet/deposit-info",
        "  POST /wallet/withdraw",
        "  GET  /greed/jackpot",
        "  GET  /greed/deposit-intent",
        "  GET  /greed/deposit-intent/:id",
        "  POST /greed/deposit-intent",
        "  POST /greed/deposit-intent/:id/cancel",
        "  POST /greed/start",
        "  POST /greed/pick",
        "  POST /greed/cashout",
        "  GET  /greed/active",
        "  GET  /greed/leaderboards",
        "  GET  /greed/feed",
        "",
        "tapping counts as cardio 🟣🟡",
      ].join("\n")
    );
});

app.get("/health", (_req: Request, res: Response) =>
  res.json({
    ok: true,
    service: "planetfatness-backend",
    ts: nowIso(),
    watcher: {
      enabled: SOLANA_WATCH_ENABLED,
      depositWallet: DEPOSIT_WALLET || null,
      watcherTarget: PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET || null,
      tokenAccount: PHAT_TOKEN_ACCOUNT || null,
      tokenMint: PHAT_TOKEN_MINT || null,
      rpc: SOLANA_RPC_URL || null,
      intervalMs: SOLANA_WATCH_INTERVAL_MS,
      lastSeenSignature: greedWatcherLastSeenSignature || null,
    },
    spectator: {
      enabled: GREED_SPECTATOR_ENABLED,
      chatId: GREED_SPECTATOR_CHAT_ID || null,
      shoutIntervalMs: GREED_SHOUT_INTERVAL_MS,
      activeRounds: liveSpectatorRounds.size,
    },
  })
);

app.use("/auth", authRouter);

// -------------------------------
// Wallet / balance
// -------------------------------
app.get("/wallet/balance", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const balance = await getBalance(address);

    return res.json({
      ok: true,
      address,
      balance,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch balance" });
  }
});

app.get("/wallet/deposit-info", requireAuth, async (_req: Request, res: Response) => {
  return res.json({
    ok: true,
    depositWallet: DEPOSIT_WALLET || null,
    bankrollWallet: String(process.env.BANKROLL_WALLET || "").trim() || null,
    jackpotWallet: String(process.env.JACKPOT_WALLET || "").trim() || null,
    treasuryWallet: String(process.env.TREASURY_WALLET || "").trim() || null,
    acceptedToken: PHAT_TOKEN_MINT,
    mode: "intent-funding",
    watcher: {
      enabled: SOLANA_WATCH_ENABLED,
      intervalMs: SOLANA_WATCH_INTERVAL_MS,
      watcherTarget: PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET || null,
    },
  });
});

app.post("/wallet/withdraw", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const amount = parseAmount3(req.body?.amount);
    const destinationWallet = String(req.body?.destinationWallet || address).trim();

    if (amount == null || amount <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    const bal = await getBalance(address);
    const available = Number(bal?.available_balance || 0);
    if (available < amount) {
      return res.status(400).json({ error: "Insufficient balance" });
    }

    const debited = await debitBalance({ address, amount });
    if (!debited) {
      return res.status(400).json({ error: "Insufficient balance" });
    }

    const row = await createWithdrawal({
      address,
      destinationWallet,
      amount,
      note: "user withdrawal request",
    });

    return res.json({
      ok: true,
      withdrawal: row,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Withdrawal request failed" });
  }
});

// -------------------------------
// Profile / activity
// -------------------------------
app.get("/profile/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;
  const me = await getMe(address);
  if (!me) return res.status(404).json({ error: "User not found" });

  const balance = await getBalance(address);

  res.json({
    address: me.address,
    displayName: me.display_name || null,
    balance,
  });
});

app.post("/profile/name", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const displayName = String(req.body?.displayName || "").trim();

    const u = await setDisplayName({ address, displayName });
    if (!u) return res.status(500).json({ error: "Update failed" });

    res.json({ ok: true, address: u.address, displayName: u.display_name || null });
  } catch (e: any) {
    const msg = String(e?.message || "");
    if (msg === "displayName_too_short") return res.status(400).json({ error: "Name too short (min 2 chars)" });
    console.error(e);
    res.status(500).json({ error: "Name update failed" });
  }
});

app.get("/activity/me", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;
  const me = await getMe(address);
  if (!me) return res.status(404).json({ error: "User not found" });

  res.json({
    address: me.address,
    displayName: me.display_name || null,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

app.get("/activity/summary", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const summary = await getActivitySummary({ address });
    res.json(summary);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Summary failed" });
  }
});

app.get("/daily/progress", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;

    const games: GameKey[] = ["snack", "runner", "lift", "basket"];
    const out: any = { address, ts: nowIso(), games: {} as any };

    for (const g of games) {
      const today = await getTodayAgg(address, g);
      const rules = RULES[g];
      const goal = computeDailyGoalProgress({ game: g, today });

      out.games[g] = {
        today: {
          calories: today.calories,
          miles: today.miles,
          score: today.score,
          durationMs: today.durationMs,
          bestScore: today.bestScore,
        },
        goal,
        caps: {
          dailyCapCalories: rules.dailyCapCalories,
          remainingCalories: Math.max(0, rules.dailyCapCalories - today.calories),
        },
      };
    }

    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Daily progress failed" });
  }
});

app.post("/activity/add", requireAuth, async (req: Request, res: Response) => {
  const address = (req as any).user?.address as string;

  const legacyAddCalories = Number(req.body?.addCalories ?? 0);
  const legacyBestSeconds = Number(req.body?.bestSeconds ?? 0);
  const legacyAddMiles = Number(req.body?.addMiles ?? 0);

  const game = req.body?.game ? String(req.body.game) : "";
  const v2Calories = req.body?.calories != null ? Number(req.body.calories) : undefined;
  const v2Miles = req.body?.miles != null ? Number(req.body.miles) : undefined;
  const v2BestSeconds = req.body?.bestSeconds != null ? Number(req.body.bestSeconds) : undefined;
  const v2Score = req.body?.score != null ? Number(req.body.score) : undefined;
  const v2DurationMs = req.body?.durationMs != null ? Number(req.body.durationMs) : undefined;
  const v2Streak = req.body?.streak != null ? Number(req.body.streak) : undefined;

  const finalAddCalories = Number.isFinite(v2Calories as any) ? Number(v2Calories) : legacyAddCalories;
  const finalAddMiles = Number.isFinite(v2Miles as any) ? Number(v2Miles) : legacyAddMiles;
  const finalBestSeconds = Number.isFinite(v2BestSeconds as any) ? Number(v2BestSeconds) : legacyBestSeconds;
  const finalScore = Number.isFinite(v2Score as any) ? Number(v2Score) : 0;
  const finalDurationMs = Number.isFinite(v2DurationMs as any) ? Math.max(0, Math.floor(Number(v2DurationMs))) : 0;
  const finalStreak = Number.isFinite(v2Streak as any) ? Math.max(0, Math.floor(Number(v2Streak))) : 0;

  const me = await addActivity({
    address,
    addCalories: Number.isFinite(finalAddCalories) ? finalAddCalories : 0,
    bestSeconds: Number.isFinite(finalBestSeconds) ? finalBestSeconds : 0,
    addMiles: Number.isFinite(finalAddMiles) ? finalAddMiles : 0,
  });

  if (!me) return res.status(500).json({ error: "Update failed" });

  const looksLikeReceipt =
    (!!game && game.length > 0) ||
    (Number.isFinite(finalScore as any) && finalScore > 0) ||
    (Number.isFinite(finalDurationMs as any) && finalDurationMs > 0);

  if (looksLikeReceipt) {
    const safeGame = (game || "unknown").slice(0, 32);

    try {
      await logSession({
        address,
        game: safeGame,
        calories: Math.max(0, Math.floor(finalAddCalories || 0)),
        miles: Math.max(0, Number(finalAddMiles || 0)),
        bestSeconds: Math.max(0, Number(finalBestSeconds || 0)),
        score: Math.max(0, Number(finalScore || 0)),
        streak: finalStreak,
        durationMs: Math.max(0, Math.floor(finalDurationMs || 0)),
      });
    } catch (e) {
      console.error("logSession failed:", e);
    }
  }

  res.json({
    address: me.address,
    displayName: (me as any).display_name || null,
    totalCalories: Number(me.total_calories || 0),
    bestSeconds: Number(me.best_seconds || 0),
    totalMiles: Number(me.total_miles || 0),
  });
});

app.post("/activity/submit", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;

    const game = asGameKey(req.body?.game);
    const score = Number(req.body?.score ?? 0);
    const miles = Number(req.body?.miles ?? 0);
    const bestSeconds = Number(req.body?.bestSeconds ?? 0);
    const durationMs = Number(req.body?.durationMs ?? 0);
    const streak = Math.max(0, Math.floor(Number(req.body?.streak ?? 0) || 0));

    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      return res.status(400).json({ error: "Missing durationMs" });
    }

    const calc = computeEarnedCalories({
      game,
      score,
      miles,
      bestSeconds,
      durationMs,
    });

    const todayBefore = await getTodayAgg(address, game);
    const rules = RULES[game] || COMMON_RULES;

    const remaining = Math.max(0, rules.dailyCapCalories - todayBefore.calories);
    const earnedCapped = Math.max(0, Math.min(calc.earnedCalories, remaining));

    await logSession({
      address,
      game,
      calories: earnedCapped,
      miles: Math.max(0, miles || 0),
      bestSeconds: Math.max(0, bestSeconds || 0),
      score: Math.max(0, score || 0),
      streak,
      durationMs: Math.max(0, Math.floor(durationMs || 0)),
    });

    const me = await addActivity({
      address,
      addCalories: earnedCapped,
      bestSeconds: Math.max(0, bestSeconds || 0),
      addMiles: Math.max(0, miles || 0),
    });

    if (!me) return res.status(500).json({ error: "Update failed" });

    const todayAfter = await getTodayAgg(address, game);
    const goal = computeDailyGoalProgress({ game, today: todayAfter });

    res.json({
      ok: true,
      address,
      game,
      earnedCalories: earnedCapped,
      reason: calc.reason,
      caps: {
        dailyCapCalories: rules.dailyCapCalories,
        remainingCalories: Math.max(0, rules.dailyCapCalories - todayAfter.calories),
      },
      today: {
        calories: todayAfter.calories,
        miles: todayAfter.miles,
        score: todayAfter.score,
        durationMs: todayAfter.durationMs,
        bestScore: todayAfter.bestScore,
      },
      goal,
      totals: {
        totalCalories: Number(me.total_calories || 0),
        bestSeconds: Number(me.best_seconds || 0),
        totalMiles: Number(me.total_miles || 0),
      },
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Submit failed" });
  }
});

// -------------------------------
// Greed deposit intent API
// -------------------------------
app.get("/greed/deposit-intent", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    await expireStaleGreedDepositIntents(address);

    const intent = await getOpenGreedDepositIntentByAddress(address);

    return res.json({
      ok: true,
      intent: serializeGreedIntent(intent),
      funding: {
        minWager: GREED_MIN_WAGER,
        maxWager: GREED_MAX_WAGER,
        quickWagers: [1000, 5000, 10000, 25000, 50000],
        acceptedToken: PHAT_TOKEN_MINT,
        depositWallet: DEPOSIT_WALLET || null,
        expiresInMinutes: GREED_INTENT_EXPIRES_MINUTES,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch deposit intent" });
  }
});

app.get("/greed/deposit-intent/:id", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const id = Math.floor(Number(req.params.id));

    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ error: "Invalid id" });
    }

    const intent = await getGreedDepositIntentByIdForAddress(id, address);
    if (!intent) {
      return res.status(404).json({ error: "Intent not found" });
    }

    return res.json({
      ok: true,
      intent: serializeGreedIntent(intent),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch deposit intent" });
  }
});

app.post("/greed/deposit-intent", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const wager = sanitizeWager(req.body?.wager);

    if (wager == null) {
      return res.status(400).json({ error: `Wager must be between ${GREED_MIN_WAGER} and ${GREED_MAX_WAGER}` });
    }

    if (!DEPOSIT_WALLET) {
      return res.status(500).json({ error: "Missing DEPOSIT_WALLET configuration" });
    }

    await expireStaleGreedDepositIntents(address);

    const existing = await getOpenGreedDepositIntentByAddress(address);
    if (existing) {
      return res.json({
        ok: true,
        reused: true,
        intent: serializeGreedIntent(existing),
      });
    }

    const exactAmount = await generateUniqueExactAmount(wager);

    const intent = await createGreedDepositIntent({
      address,
      requestedWager: wager,
      exactAmount,
      depositWallet: DEPOSIT_WALLET,
      tokenMint: PHAT_TOKEN_MINT,
      expiresInMinutes: GREED_INTENT_EXPIRES_MINUTES,
    });

    return res.json({
      ok: true,
      reused: false,
      intent: serializeGreedIntent(intent),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to create deposit intent" });
  }
});

app.post("/greed/deposit-intent/:id/cancel", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const id = Math.floor(Number(req.params.id));

    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ error: "Invalid id" });
    }

    const intent = await cancelGreedDepositIntent({ id, address });
    if (!intent) {
      return res.status(404).json({ error: "Intent not found" });
    }

    return res.json({
      ok: true,
      intent: serializeGreedIntent(intent),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to cancel deposit intent" });
  }
});

// -------------------------------
// Greed API
// -------------------------------
app.get("/greed/jackpot", async (_req: Request, res: Response) => {
  try {
    const row = await getGreedJackpotState();
    return res.json({ ok: true, jackpot: row });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch jackpot" });
  }
});

app.post("/greed/start", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const requestedWager = sanitizeWager(req.body?.wager);
    const spectatorChatId = getSpectatorChatIdFromReq(req);

    if (requestedWager == null) {
      return res.status(400).json({ error: `Wager must be between ${GREED_MIN_WAGER} and ${GREED_MAX_WAGER}` });
    }

    const existing = await getActiveGreedRound(address);
    if (existing) {
      return res.status(400).json({
        error: "Round already active",
        roundId: existing.id,
      });
    }

    await expireStaleGreedDepositIntents(address);

    let lockedWager = 0;
    let fundedIntentId: number | null = null;
    let fundingSource: "balance" | "intent" = "balance";

    const bal = await getBalance(address);
    const available = round3(Number(bal?.available_balance || 0));

    if (available >= requestedWager) {
      const debited = await debitBalance({ address, amount: requestedWager });
      if (!debited) {
        return res.status(409).json({
          error: "Failed to lock balance wager",
          code: "BALANCE_WAGER_LOCK_FAILED",
        });
      }

      lockedWager = round3(requestedWager);
      fundingSource = "balance";
    } else {
      const openIntent = await getOpenGreedDepositIntentByAddress(address);
      if (!openIntent) {
        return res.status(400).json({
          error: "Funding required",
          code: "FUNDING_REQUIRED",
        });
      }

      if (String(openIntent.status) !== "funded") {
        return res.status(400).json({
          error: "Deposit intent not funded yet",
          code: "INTENT_NOT_FUNDED",
          intent: serializeGreedIntent(openIntent),
        });
      }

      const intentWager = Number(openIntent.requested_wager || 0);
      const exactAmount = round3(Number(openIntent.exact_amount || 0));

      if (intentWager !== requestedWager) {
        return res.status(400).json({
          error: "Wager does not match funded intent",
          code: "INTENT_WAGER_MISMATCH",
          intent: serializeGreedIntent(openIntent),
        });
      }

      const consumedIntent = await consumeFundedGreedDepositIntent({
        id: Number(openIntent.id),
        address,
      });

      if (!consumedIntent) {
        return res.status(409).json({
          error: "Funded intent could not be consumed",
          code: "INTENT_CONSUME_FAILED",
        });
      }

      await creditBalance({ address, amount: exactAmount });

      const debited = await debitBalance({ address, amount: exactAmount });
      if (!debited) {
        return res.status(409).json({
          error: "Failed to lock funded wager",
          code: "FUNDED_WAGER_LOCK_FAILED",
        });
      }

      lockedWager = round3(exactAmount);
      fundedIntentId = Number(consumedIntent.id);
      fundingSource = "intent";
    }

    const { totalTax, devCut, treasuryCut, jackpotCut, netStake } = getGreedTaxBreakdown(lockedWager);

    const jackpotFeed = round3(lockedWager * GREED_JACKPOT_FEED_RATE);
    await addToGreedJackpot(jackpotFeed);

    const serverSeed = crypto.randomBytes(32).toString("hex");
    const commitHash = sha256Hex(serverSeed);
    const nonce = Date.now();
    const poisonIndices = derivePoisonIndicesFromSeed(serverSeed, nonce, GREED_TOTAL_DONUTS, GREED_POISON_COUNT);

    const round = await createGreedRound({
      address,
      wager: lockedWager,
      netStake,
      poisonIndices,
      seed: serverSeed,
      commitHash,
      nonce,
    });

    await recordGreedTaxLedger({
      address,
      roundId: Number(round.id),
      source: fundingSource === "balance" ? "greed_start_balance" : "greed_start_intent",
      grossWager: lockedWager,
      totalTax,
      devCut,
      treasuryCut,
      jackpotCut,
      note:
        fundingSource === "balance"
          ? "greed round started from internal balance"
          : `greed round started from funded intent${fundedIntentId ? ` #${fundedIntentId}` : ""}`,
    });

    const displayName = await getDisplayNameForAddress(address);

    if (GREED_SPECTATOR_ENABLED && spectatorChatId) {
      registerLiveRound({
        roundId: Number(round.id),
        address,
        displayName,
        wager: requestedWager,
        fundedExactAmount: lockedWager,
        chatId: spectatorChatId,
        startedAt: Date.now(),
        updatedAt: Date.now(),
        safeClicks: 0,
        isActive: true,
      });

      await sendGymSpectatorMessageToChat(
        spectatorChatId,
        [
          `🍩 FEED YOUR GREED LIVE`,
          ``,
          `${displayName} just locked a round.`,
          `Requested wager: ${formatAmount3(requestedWager)} PHAT`,
          `Locked amount: ${formatAmount3(lockedWager)} PHAT`,
          `Round ID: #${Number(round.id)}`,
          ``,
          `Pick your donut in chat before they do 👇`,
          `${formatDonutBoardLine()}`,
        ].join("\n"),
        {
          reply_markup: Markup.inlineKeyboard([
            [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
          ]).reply_markup,
        }
      );
    }

    return res.json({
      ok: true,
      roundId: round.id,
      requestedWager,
      fundedExactAmount: lockedWager,
      wager: lockedWager,
      netStake,
      totalTax,
      devCut,
      treasuryCut,
      jackpotCut,
      jackpotFeed,
      fundingSource,
      totalDonuts: GREED_TOTAL_DONUTS,
      poisonCount: GREED_POISON_COUNT,
      currentMultiplier: 1.0,
      cashoutAvailable: false,
      fundedIntentId,
      provablyFair: {
        commitHash,
        nonce,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to start greed round" });
  }
});

app.post("/greed/pick", requireAuth, async (req: Request, res: Response) => {
  let lockAcquired = false;

  try {
    const address = (req as any).user?.address as string;
    const roundId = Math.floor(Number(req.body?.roundId));
    const pickedIndex = Math.floor(Number(req.body?.pickedIndex));

    if (!Number.isFinite(roundId) || roundId <= 0) {
      return res.status(400).json({ error: "Invalid roundId" });
    }

    if (!Number.isFinite(pickedIndex) || pickedIndex < 0 || pickedIndex >= GREED_TOTAL_DONUTS) {
      return res.status(400).json({ error: "Invalid pickedIndex" });
    }

    const round = await getGreedRoundByIdForAddress(roundId, address);
    if (!round) {
      return res.status(404).json({ error: "Round not found" });
    }

    if (round.status !== "active" || round.is_active !== true) {
      return res.status(400).json({ error: "Round is not active" });
    }

    const lockedRound = await acquireGreedRoundProcessingLock({
      roundId,
      address,
    });

    if (!lockedRound) {
      return res.status(409).json({ error: "Round is busy processing another action" });
    }

    lockAcquired = true;

    const pickedAlready = await getGreedPickedIndices(roundId);
    if (pickedAlready.includes(pickedIndex)) {
      await releaseGreedRoundProcessingLock({ roundId, address });
      lockAcquired = false;
      return res.status(400).json({ error: "Donut already picked" });
    }

    const poisonIndices = Array.isArray(lockedRound.poison_indices)
      ? lockedRound.poison_indices.map((n: unknown) => Number(n))
      : [];

    const isPoison = poisonIndices.includes(pickedIndex);
    const liveState = liveSpectatorRounds.get(roundId);

    if (isPoison) {
      await recordGreedPick({
        roundId,
        donutIndex: pickedIndex,
        result: "poison",
      });

      const currentMultiplier = Number(lockedRound.current_multiplier || 1.0);

      const closed = await closeGreedRoundAsPoison({
        roundId,
        address,
        safeClicks: Number(lockedRound.safe_clicks || 0),
        currentMultiplier,
      });

      lockAcquired = false;

      if (!closed) {
        return res.status(409).json({ error: "Round could not be closed" });
      }

      if (liveState) {
        await sendGymSpectatorMessageToChat(
          liveState.chatId,
          [
            `☠️ BUST`,
            `${liveState.displayName} picked donut #${pickedIndex + 1}`,
            `Result: POISON`,
            `Round over at ${safeMultiplierLabel(Number(closed.safe_clicks || 0))}`,
          ].join("\n")
        );
        closeLiveRound(roundId);
      }

      return res.json({
        ok: true,
        result: "poison",
        roundEnded: true,
        safeClicks: Number(closed.safe_clicks || 0),
        currentMultiplier: Number(closed.current_multiplier || currentMultiplier),
        payout: 0,
        jackpotWon: 0,
        provablyFair: {
          commitHash: closed.commit_hash,
          serverSeed: closed.server_seed,
          nonce: Number(closed.nonce || 1),
          poisonIndices: closed.poison_indices,
        },
      });
    }

    await recordGreedPick({
      roundId,
      donutIndex: pickedIndex,
      result: "safe",
    });

    const newSafeClicks = Number(lockedRound.safe_clicks || 0) + 1;
    const newMultiplier = getGreedMultiplierForSafeClicks(newSafeClicks);

    if (newSafeClicks >= 10) {
      const jackpotState = await getGreedJackpotState();
      const jackpotWon = round3(Number(jackpotState?.current_amount || 0));
      const basePayout = round3(Number(lockedRound.net_stake || 0) * newMultiplier);
      const totalPayout = round3(basePayout + jackpotWon);

      const closed = await closeGreedRoundAsCashout({
        roundId,
        address,
        safeClicks: newSafeClicks,
        currentMultiplier: newMultiplier,
        payout: totalPayout,
        result: "perfect",
        jackpotWon,
      });

      lockAcquired = false;

      if (!closed) {
        return res.status(409).json({ error: "Perfect run close failed" });
      }

      await creditBalance({ address, amount: totalPayout });
      await reseedGreedJackpot();

      await logSession({
        address,
        game: "greed",
        calories: 0,
        miles: 0,
        bestSeconds: 0,
        score: Math.floor(totalPayout),
        durationMs: 0,
      });

      if (liveState) {
        await sendGymSpectatorMessageToChat(
          liveState.chatId,
          [
            `👑 PERFECT RUN`,
            `${liveState.displayName} survived all 10 safe donuts.`,
            `Final multiplier: x${newMultiplier.toFixed(2)}`,
            `Jackpot won: ${formatAmount3(jackpotWon)} PHAT`,
            `Total payout: ${formatAmount3(totalPayout)} PHAT`,
          ].join("\n")
        );
        closeLiveRound(roundId);
      }

      return res.json({
        ok: true,
        result: "perfect",
        roundEnded: true,
        safeClicks: newSafeClicks,
        currentMultiplier: newMultiplier,
        payout: totalPayout,
        jackpotWon,
        cashoutAvailable: false,
        provablyFair: {
          commitHash: closed.commit_hash,
          serverSeed: closed.server_seed,
          nonce: Number(closed.nonce || 1),
          poisonIndices: closed.poison_indices,
        },
      });
    }

    const updated = await updateGreedRoundProgress({
      roundId,
      address,
      safeClicks: newSafeClicks,
      currentMultiplier: newMultiplier,
    });

    await releaseGreedRoundProcessingLock({ roundId, address });
    lockAcquired = false;

    if (!updated) {
      return res.status(409).json({ error: "Round progress update failed" });
    }

    if (liveState) {
      updateLiveRound(roundId, {
        safeClicks: newSafeClicks,
      });

      let spectatorText = [
        `✅ SAFE PICK`,
        `${liveState.displayName} picked donut #${pickedIndex + 1}`,
        `Safe clicks: ${newSafeClicks}`,
        `Current multiplier: x${newMultiplier.toFixed(2)}`,
      ].join("\n");

      if (newSafeClicks === 9) {
        spectatorText += `\n🔥 FINAL DONUT LIVE`;
      }

      await sendGymSpectatorMessageToChat(liveState.chatId, spectatorText);
    }

    return res.json({
      ok: true,
      result: "safe",
      roundEnded: false,
      safeClicks: newSafeClicks,
      currentMultiplier: newMultiplier,
      payout: 0,
      cashoutAvailable: newSafeClicks >= 1,
      finalDonutLive: newSafeClicks === 9,
      provablyFair: {
        commitHash: updated.commit_hash,
        nonce: Number(updated.nonce || 1),
      },
    });
  } catch (e: any) {
    if (String(e?.message || "").includes("duplicate key")) {
      return res.status(400).json({ error: "Donut already picked" });
    }
    console.error(e);
    return res.status(500).json({ error: "Greed pick failed" });
  } finally {
    if (lockAcquired) {
      try {
        const address = (req as any).user?.address as string;
        const roundId = Math.floor(Number(req.body?.roundId));
        if (Number.isFinite(roundId) && roundId > 0 && address) {
          await releaseGreedRoundProcessingLock({ roundId, address });
        }
      } catch {}
    }
  }
});

app.post("/greed/cashout", requireAuth, async (req: Request, res: Response) => {
  let lockAcquired = false;

  try {
    const address = (req as any).user?.address as string;
    const roundId = Math.floor(Number(req.body?.roundId));

    if (!Number.isFinite(roundId) || roundId <= 0) {
      return res.status(400).json({ error: "Invalid roundId" });
    }

    const round = await getGreedRoundByIdForAddress(roundId, address);
    if (!round) {
      return res.status(404).json({ error: "Round not found" });
    }

    if (round.status !== "active" || round.is_active !== true) {
      return res.status(400).json({ error: "Round is not active" });
    }

    const lockedRound = await acquireGreedRoundProcessingLock({
      roundId,
      address,
    });

    if (!lockedRound) {
      return res.status(409).json({ error: "Round is busy processing another action" });
    }

    lockAcquired = true;

    const safeClicks = Number(lockedRound.safe_clicks || 0);
    if (safeClicks < 1) {
      await releaseGreedRoundProcessingLock({ roundId, address });
      lockAcquired = false;
      return res.status(400).json({ error: "Cashout not available yet" });
    }

    if (String(lockedRound.payout_status || "unpaid") !== "unpaid") {
      await releaseGreedRoundProcessingLock({ roundId, address });
      lockAcquired = false;
      return res.status(409).json({ error: "Payout already recorded for this round" });
    }

    const currentMultiplier = Number(lockedRound.current_multiplier || 1.0);
    const payout = round3(Number(lockedRound.net_stake || 0) * currentMultiplier);

    const closed = await closeGreedRoundAsCashout({
      roundId,
      address,
      safeClicks,
      currentMultiplier,
      payout,
      result: "cashout",
      jackpotWon: 0,
    });

    lockAcquired = false;

    if (!closed) {
      return res.status(409).json({ error: "Cashout close failed" });
    }

    await creditBalance({ address, amount: payout });

    await logSession({
      address,
      game: "greed",
      calories: 0,
      miles: 0,
      bestSeconds: 0,
      score: Math.floor(payout),
      durationMs: 0,
    });

    const liveState = liveSpectatorRounds.get(roundId);
    if (liveState) {
      await sendGymSpectatorMessageToChat(
        liveState.chatId,
        [
          `💸 CASH OUT`,
          `${liveState.displayName} bailed out safely.`,
          `Safe clicks: ${safeClicks}`,
          `Multiplier: x${currentMultiplier.toFixed(2)}`,
          `Payout: ${formatAmount3(payout)} PHAT`,
        ].join("\n")
      );
      closeLiveRound(roundId);
    }

    return res.json({
      ok: true,
      result: "cashout",
      roundEnded: true,
      safeClicks,
      currentMultiplier,
      payout,
      jackpotWon: 0,
      provablyFair: {
        commitHash: closed.commit_hash,
        serverSeed: closed.server_seed,
        nonce: Number(closed.nonce || 1),
        poisonIndices: closed.poison_indices,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Greed cashout failed" });
  } finally {
    if (lockAcquired) {
      try {
        const address = (req as any).user?.address as string;
        const roundId = Math.floor(Number(req.body?.roundId));
        if (Number.isFinite(roundId) && roundId > 0 && address) {
          await releaseGreedRoundProcessingLock({ roundId, address });
        }
      } catch {}
    }
  }
});

app.get("/greed/active", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const round = await getActiveGreedRound(address);

    if (!round) {
      return res.json({ active: false, round: null });
    }

    const pickedIndices = await getGreedPickedIndices(Number(round.id));

    return res.json({
      active: true,
      round: {
        id: Number(round.id),
        wager: Number(round.wager),
        netStake: Number(round.net_stake),
        safeClicks: Number(round.safe_clicks || 0),
        currentMultiplier: Number(round.current_multiplier || 1.0),
        pickedIndices,
        cashoutAvailable: Number(round.safe_clicks || 0) >= 1,
        provablyFair: {
          commitHash: round.commit_hash,
          nonce: Number(round.nonce || 1),
        },
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch active round" });
  }
});

app.get("/greed/leaderboards", async (req: Request, res: Response) => {
  try {
    const window = normalizeWindow(req.query.window || "lifetime");
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 10)));

    const [mostWagered, mostWon, perfectRuns, biggestCashout, topGlazeSacrifices] = await Promise.all([
      getGreedLeaderboard({ board: "most_wagered", window, limit }),
      getGreedLeaderboard({ board: "most_won", window, limit }),
      getGreedLeaderboard({ board: "perfect_runs", window, limit }),
      getGreedLeaderboard({ board: "biggest_cashout", window, limit }),
      getGreedLeaderboard({ board: "top_glaze_sacrifices", window, limit }),
    ]);

    return res.json({
      ok: true,
      window,
      boards: {
        mostWagered,
        mostWon,
        perfectRuns,
        biggestCashout,
        topGlazeSacrifices,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Greed leaderboards failed" });
  }
});

app.get("/greed/feed", async (req: Request, res: Response) => {
  try {
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 20)));
    const rows = await getGreedFeed(limit);
    return res.json({ ok: true, rows });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Greed feed failed" });
  }
});

// -------------------------------
// Standard leaderboards
// -------------------------------
app.get("/leaderboard", async (_req: Request, res: Response) => {
  const top = await getLeaderboard(30);
  res.json(
    top.map((u: any) => ({
      address: u.address,
      displayName: u.display_name || null,
      totalCalories: Number(u.total_calories || 0),
      bestSeconds: Number(u.best_seconds || 0),
      totalMiles: Number(u.total_miles || 0),
    }))
  );
});

app.get("/leaderboard/v2", async (req: Request, res: Response) => {
  try {
    const window = normalizeWindow(req.query.window || "lifetime") as any;
    const metric = String(req.query.metric || "calories") as any;

    const gameRaw = req.query.game ? String(req.query.game) : undefined;
    const game = gameRaw ? asGameKey(gameRaw) : undefined;

    const limit = req.query.limit ? Number(req.query.limit) : 30;

    const rows = await getLeaderboardV2({ window, metric, game, limit });
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Leaderboard v2 failed" });
  }
});

app.get("/leaderboard/games", async (req: Request, res: Response) => {
  try {
    const window = normalizeWindow(req.query.window || "lifetime");
    const limit = req.query.limit ? Number(req.query.limit) : 3;

    const games: GameKey[] = ["runner", "snack", "lift", "basket", "greed"];
    const out: any = { ok: true, window, limit, games: {} as any };

    for (const g of games) {
      const rows = await getLeaderboardV2({
        window,
        metric: "score",
        game: g,
        limit: Math.max(1, Math.min(50, Number(limit) || 3)),
      });
      out.games[g] = rows;
    }

    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Leaderboard games failed" });
  }
});

// -------------------------------
// Admin
// -------------------------------
app.post("/admin/deposit-credit", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const address = String(req.body?.address || "").trim();
    const txSignature = String(req.body?.txSignature || "").trim();
    const senderWallet = String(req.body?.senderWallet || "").trim() || null;
    const tokenMint = String(req.body?.tokenMint || "").trim() || null;
    const amount = parseAmount3(req.body?.amount);
    const note = String(req.body?.note || "manual admin credit").trim();

    if (!address) return res.status(400).json({ error: "Missing address" });
    if (!txSignature) return res.status(400).json({ error: "Missing txSignature" });
    if (amount == null || amount <= 0) return res.status(400).json({ error: "Invalid amount" });

    const exists = await hasDepositTxSignature(txSignature);
    if (exists) return res.status(400).json({ error: "Transaction already processed" });

    const dep = await recordDeposit({
      address,
      txSignature,
      senderWallet,
      tokenMint,
      amount,
      status: "credited",
      note,
    });

    if (!dep) return res.status(400).json({ error: "Transaction already processed" });

    await creditBalance({ address, amount });

    return res.json({ ok: true, deposit: dep });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Deposit credit failed" });
  }
});

app.post("/admin/greed/fund-intent", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const txSignature = String(req.body?.txSignature || "").trim();
    const senderWallet = String(req.body?.senderWallet || "").trim() || null;
    const tokenMint = String(req.body?.tokenMint || "").trim() || PHAT_TOKEN_MINT;
    const exactAmount = parseAmount3(req.body?.exactAmount);
    const explicitIntentId = Math.floor(Number(req.body?.intentId || 0));

    let intent: any = null;

    if (explicitIntentId > 0) {
      const row = await pool.query(
        `
        SELECT *
        FROM greed_deposit_intents
        WHERE id = $1
        LIMIT 1;
        `,
        [explicitIntentId]
      );
      intent = row.rows[0] || null;
    } else {
      if (exactAmount == null) {
        return res.status(400).json({ error: "Missing exactAmount" });
      }
      intent = await findGreedDepositIntentByExactAmount({
        exactAmount,
        status: "pending",
      });
    }

    if (!intent) {
      return res.status(404).json({ error: "Matching pending intent not found" });
    }

    if (!txSignature) {
      return res.status(400).json({ error: "Missing txSignature" });
    }

    const alreadyProcessed = await hasDepositTxSignature(txSignature);
    if (alreadyProcessed) {
      return res.status(400).json({ error: "Transaction already processed" });
    }

    const funded = await markGreedDepositIntentFunded({
      id: Number(intent.id),
      address: String(intent.address),
      txSignature,
      senderWallet,
      tokenMint,
    });

    if (!funded) {
      return res.status(400).json({ error: "Intent could not be marked funded" });
    }

    const dep = await recordDeposit({
      address: String(funded.address),
      txSignature,
      senderWallet,
      tokenMint,
      amount: Number(funded.exact_amount || 0),
      status: "credited",
      note: "greed intent funded",
    });

    return res.json({
      ok: true,
      intent: serializeGreedIntent(funded),
      deposit: dep,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Fund intent failed" });
  }
});

app.post("/admin/jackpot/reseed", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const amount = parseAmount3(req.body?.amount ?? GREED_JACKPOT_RESEED);
    const row = await setGreedJackpotAmount(amount == null ? GREED_JACKPOT_RESEED : amount);
    return res.json({ ok: true, jackpot: row });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Jackpot reseed failed" });
  }
});

app.get("/admin/launch-reset", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    await pool.query(`
      UPDATE users
      SET total_calories = 0,
          total_miles = 0,
          best_seconds = 0,
          lifetime_makes = 0;
    `);

    await pool.query(`TRUNCATE TABLE sessions CASCADE;`);
    await pool.query(`TRUNCATE TABLE greed_picks CASCADE;`);
    await pool.query(`TRUNCATE TABLE greed_rounds CASCADE;`);
    await pool.query(`TRUNCATE TABLE greed_deposit_intents CASCADE;`);
    await pool.query(`TRUNCATE TABLE deposits CASCADE;`);
    await pool.query(`TRUNCATE TABLE withdrawals CASCADE;`);
    await pool.query(`TRUNCATE TABLE greed_tax_ledger CASCADE;`);
    await pool.query(`UPDATE balances SET available_balance = 0, locked_balance = 0, updated_at = NOW();`);
    await pool.query(`UPDATE jackpot_state SET current_amount = reseed_amount, updated_at = NOW() WHERE key = 'greed';`);

    try {
      await pool.query(`UPDATE pf_users SET total_calories = 0;`);
    } catch {}

    res.type("text/plain").send("✅ Planet Fatness deep cleaned.");
  } catch (err: any) {
    res.status(500).send("❌ Reset failed: " + err.message);
  }
});

// -------------------------------
// Telegram game launch
// -------------------------------
const bot = new Telegraf(process.env.GREED_BOT_TOKEN || "");

bot.command("game", async (ctx) => {
  try {
    await ctx.reply(
      "🍩 Feed Your Greed is live.\nTap below to open the official game.",
      Markup.inlineKeyboard([[Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)]])
    );
  } catch (e) {
    console.error("GREED /game button error:", e);
    try {
      await ctx.replyWithGame(TG_GAME_SHORT_NAME);
    } catch (inner) {
      console.error("GREED fallback replyWithGame error:", inner);
    }
  }
});

bot.start(async (ctx) => {
  try {
    await ctx.reply(
      "🍩 Feed Your Greed is live.\nTap below to open the official game.",
      Markup.inlineKeyboard([[Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)]])
    );
  } catch (e) {
    console.error("GREED /start button error:", e);
  }
});

bot.on("callback_query", async (ctx) => {
  try {
    const q = ctx.callbackQuery;
    if (!q || !("game_short_name" in q)) return;

    const user: any = ctx.from;
    const tgIdNum = Number(user?.id || 0);
    const username = user?.username ? String(user.username) : "";
    const firstName = user?.first_name ? String(user.first_name) : "";
    const lastName = user?.last_name ? String(user.last_name) : "";

    if (!tgIdNum) {
      await ctx.answerGameQuery(GREED_WEBAPP_URL);
      return;
    }

    const address = `tg:${tgIdNum}`;

    try {
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
        } catch {}
      }
    } catch (e) {
      console.error("TG game upsert/identity failed:", e);
      await ctx.answerGameQuery(GREED_WEBAPP_URL);
      return;
    }

    let token = "";
    try {
      token = signTokenForAddress(address);
    } catch (e) {
      console.error("TG game signToken failed:", e);
      await ctx.answerGameQuery(GREED_WEBAPP_URL);
      return;
    }

    const launchUrl = `${GREED_WEBAPP_URL}?t=${encodeURIComponent(token)}`;
    await ctx.answerGameQuery(launchUrl);
  } catch (e) {
    console.error("TG game callback handler error:", e);
    try {
      await ctx.answerGameQuery(GREED_WEBAPP_URL);
    } catch {}
  }
});

// Main gym bot
const gymBot = new Telegraf(process.env.TG_BOT_TOKEN || "");

gymBot.start(async (ctx) => {
  try {
    await ctx.reply(
      "🏋️ Welcome back to Planet Fatness Gym! Tap below to open the app.",
      Markup.inlineKeyboard([
        [Markup.button.webApp("Open Planet Fatness Gym", HUB_WEBAPP_URL)],
        [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
      ])
    );
  } catch (e) {
    console.error("GYM /start button error:", e);
    try {
      await ctx.reply("🏋️ Welcome back to Planet Fatness Gym!");
    } catch {}
  }
});

gymBot.command("greed", async (ctx) => {
  try {
    await ctx.reply(
      "🍩 Feed Your Greed is live.\nTap below to open the official game.",
      Markup.inlineKeyboard([[Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)]])
    );
  } catch (e) {
    console.error("GYM /greed button error:", e);
  }
});

gymBot.command("greedlive", async (ctx) => {
  try {
    await ctx.reply(
      [
        "🍩 FEED YOUR GREED LIVE",
        "Watch the next degen lock a round and scream your donut pick in chat.",
      ].join("\n"),
      Markup.inlineKeyboard([[Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)]])
    );
  } catch (e) {
    console.error("GYM /greedlive button error:", e);
  }
});

gymBot.on("callback_query", async (ctx) => {
  try {
    const q = ctx.callbackQuery;
    if (q && "game_short_name" in q) {
      const address = `tg:${ctx.from.id}`;
      const token = signTokenForAddress(address);
      await ctx.answerGameQuery(`${HUB_WEBAPP_URL}?t=${encodeURIComponent(token)}`);
      return;
    }

    try {
      await ctx.answerCbQuery();
    } catch {}
  } catch (e) {
    console.error("GYM callback handler error:", e);
    try {
      await ctx.answerCbQuery();
    } catch {}
  }
});

// -------------------------------
// Boot
// -------------------------------
try {
  await initDb();

  if (process.env.GREED_BOT_TOKEN) {
    bot.launch().then(() => console.log("🤖 Planet Fatness Greed Bot Engine Active"));
  } else {
    console.warn("⚠️ No GREED_BOT_TOKEN found in environment.");
  }

  if (process.env.TG_BOT_TOKEN) {
    gymBot.launch().then(() => console.log("🤖 Planet Fatness Gym Bot Engine Active"));
  } else {
    console.warn("⚠️ No TG_BOT_TOKEN found in environment.");
  }

  startGreedSolanaWatcher();
  startGreedShoutLoop();

  app.listen(PORT, () => console.log(`✅ Planet Fatness backend on :${PORT}`));
} catch (e) {
  console.error("❌ Failed to boot:", e);
  process.exit(1);
}