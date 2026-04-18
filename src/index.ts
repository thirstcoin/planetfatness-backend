import dotenv from "dotenv";
dotenv.config();

import type { Request, Response } from "express";
import { Telegraf, Markup } from "telegraf";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import bs58 from "bs58";

// --- Dynamic imports (env ready before db.ts reads DATABASE_URL) ---
const expressMod = await import("express");
const express = expressMod.default;

const corsMod = await import("cors");
const cors = corsMod.default;

const solanaWeb3Mod = await import("@solana/web3.js");
const {
  Connection,
  PublicKey,
  Keypair,
  Transaction,
  sendAndConfirmTransaction,
  clusterApiUrl,
} = solanaWeb3Mod;

const splTokenMod = await import("@solana/spl-token");
const {
  getAssociatedTokenAddress,
  createTransferInstruction,
  createAssociatedTokenAccountInstruction,
  getMint,
  TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
  ASSOCIATED_TOKEN_PROGRAM_ID,
} = splTokenMod;

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
  getPendingWithdrawals,
  markWithdrawalProcessing,
  markWithdrawalCompleted,
  markWithdrawalFailed,
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
  recordGreedUnmatchedDeposit,
  getGreedUnmatchedDepositBySignature,
  getOpenGreedUnmatchedDeposits,
  markGreedUnmatchedDepositResolved,
  getGreedAdminTreasurySnapshot,
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
const TG_BOT_USERNAME = String(process.env.TG_BOT_USERNAME || "").trim().replace(/^@/, "");

const DEPOSIT_WALLET = String(process.env.DEPOSIT_WALLET || "").trim();
const PHAT_TOKEN_ACCOUNT = String(process.env.PHAT_TOKEN_ACCOUNT || "").trim();
const PHAT_TOKEN_MINT = String(process.env.PHAT_TOKEN_MINT || "").trim() || "PHAT";
const GREED_INTENT_EXPIRES_MINUTES = Math.max(
  1,
  Math.min(60, Number(process.env.GREED_INTENT_EXPIRES_MINUTES || 10))
);

// Bankroll wallet config
const BANKROLL_PRIVATE_KEY = String(process.env.BANKROLL_PRIVATE_KEY || "").trim();
const BANKROLL_WALLET_ENV = String(process.env.BANKROLL_WALLET || "").trim();

let bankrollKeypair: InstanceType<typeof Keypair> | null = null;
let bankrollWalletAddress: string | null = null;

if (BANKROLL_PRIVATE_KEY) {
  try {
    const decoded = bs58.decode(BANKROLL_PRIVATE_KEY);
    bankrollKeypair = Keypair.fromSecretKey(decoded);
    bankrollWalletAddress = bankrollKeypair.publicKey.toBase58();
    console.log(`🏦 Bankroll wallet loaded: ${bankrollWalletAddress}`);
  } catch (e) {
    console.error("❌ Invalid BANKROLL_PRIVATE_KEY. Could not decode base58 private key.", e);
    process.exit(1);
  }
} else if (BANKROLL_WALLET_ENV) {
  bankrollWalletAddress = BANKROLL_WALLET_ENV;
  console.warn("⚠️ BANKROLL_PRIVATE_KEY missing. Bankroll wallet loaded in read-only/address-only mode.");
} else {
  console.warn("⚠️ No bankroll wallet configured yet (BANKROLL_PRIVATE_KEY / BANKROLL_WALLET missing).");
}

// Solana watcher config
const SOLANA_RPC_URL = String(
  process.env.SOLANA_RPC_URL ||
    process.env.RPC_URL ||
    clusterApiUrl("mainnet-beta")
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

// Withdrawal worker config
const WITHDRAWALS_ENABLED =
  String(process.env.WITHDRAWALS_ENABLED || "true").trim().toLowerCase() !== "false";

const WITHDRAWALS_INTERVAL_MS = Math.max(
  5000,
  Math.min(120000, Number(process.env.WITHDRAWALS_INTERVAL_MS || 15000))
);

const WITHDRAWALS_BATCH_LIMIT = Math.max(
  1,
  Math.min(25, Number(process.env.WITHDRAWALS_BATCH_LIMIT || 5))
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
type GreedIntentType = "single_round" | "balance_fund";

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

type GreedTier =
  | "Light Snacker"
  | "Heavy Feeder"
  | "Glazed Up"
  | "Sugar Hunter"
  | "Stack Builder"
  | "Certified PHAT"
  | "Greed Operator"
  | "Greed God";

type GreedPlayerStats = {
  address: string;
  displayName: string;
  total_wagered: number;
  net_profit: number;
  total_won: number;
  total_lost: number;
  total_rounds: number;
  busts: number;
  cashouts: number;
  perfect_runs: number;
  biggest_cashout: number;
  biggest_jackpot: number;
  high_multiplier_cashouts: number;
  best_run_depth: number;
  cashout_rate: number;
  greed_score: number;
  tier: GreedTier;
  greed_gods_rank: number | null;
};

const liveSpectatorRounds = new Map<number, SpectatorRoundState>();
let greedWatcherTimer: NodeJS.Timeout | null = null;
let greedShoutTimer: NodeJS.Timeout | null = null;
let greedWatcherBusy = false;
let greedWatcherLastSeenSignature = "";

let withdrawalsTimer: NodeJS.Timeout | null = null;
let withdrawalsBusy = false;

const solanaConnection = SOLANA_WATCH_ENABLED || WITHDRAWALS_ENABLED
  ? new Connection(SOLANA_RPC_URL, "confirmed")
  : null;

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}

function asGameKey(x: unknown): GameKey {
  const g = String(x || "").toLowerCase().trim();
  if (g === "runner" || g === "snack" || g === "lift" || g === "basket" || g === "greed") return g as GameKey;
  return "snack";
}

function asGreedIntentType(x: unknown): GreedIntentType {
  const v = String(x || "").trim().toLowerCase();

  if (
    v === "balance_fund" ||
    v === "balance_deposit" ||
    v === "deposit_balance" ||
    v === "balance"
  ) {
    return "balance_fund";
  }

  return "single_round";
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

function formatPct(n: number) {
  const value = Number(n || 0);
  if (!Number.isFinite(value)) return "0.0%";
  return `${value.toFixed(1)}%`;
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

function sanitizeBalanceFundAmount(raw: unknown) {
  const amount = Number(raw);
  if (!Number.isFinite(amount)) return null;
  const rounded = round3(amount);
  if (rounded < GREED_MIN_WAGER || rounded > GREED_MAX_BALANCE_FUND) return null;
  return rounded;
}

function getGreedTier(score: number): GreedTier {
  const s = Number(score || 0);
  if (s < 5_000) return "Light Snacker";
  if (s < 20_000) return "Heavy Feeder";
  if (s < 50_000) return "Glazed Up";
  if (s < 120_000) return "Sugar Hunter";
  if (s < 250_000) return "Stack Builder";
  if (s < 500_000) return "Certified PHAT";
  if (s < 1_000_000) return "Greed Operator";
  return "Greed God";
}

function computeGreedScore(params: {
  netProfit: number;
  totalWagered: number;
  perfectRuns: number;
  highMultiplierCashouts: number;
}) {
  const netProfit = Number(params.netProfit || 0);
  const totalWagered = Number(params.totalWagered || 0);
  const perfectRuns = Number(params.perfectRuns || 0);
  const highMultiplierCashouts = Number(params.highMultiplierCashouts || 0);

  return round3(
    netProfit * 1.0 +
      totalWagered * 0.05 +
      perfectRuns * 5000 +
      highMultiplierCashouts * 2000
  );
}

function serializeGreedIntent(row: any) {
  if (!row) return null;
  return {
    id: Number(row.id),
    address: row.address,
    intentType: asGreedIntentType(row.intent_type),
    status: String(row.status || "pending"),
    fundingMatchStatus: String(row.funding_match_status || "unmatched"),
    requestedWager: Number(row.requested_wager || 0),
    exactAmount: Number(row.exact_amount || 0),
    fundedAmount: row.funded_amount == null ? null : Number(row.funded_amount),
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

function serializeBalanceRow(row: any) {
  if (!row) return null;
  return {
    address: row.address,
    availableBalance: Number(row.available_balance || 0),
    lockedBalance: Number(row.locked_balance || 0),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };
}

function serializeUnmatchedDeposit(row: any) {
  if (!row) return null;
  return {
    id: Number(row.id),
    txSignature: row.tx_signature,
    senderWallet: row.sender_wallet || null,
    tokenMint: row.token_mint || null,
    observedAmount: Number(row.observed_amount || 0),
    watcherTarget: row.watcher_target || null,
    matchedIntentId: row.matched_intent_id == null ? null : Number(row.matched_intent_id),
    matchedAddress: row.matched_address || null,
    reason: row.reason || null,
    resolutionStatus: row.resolution_status || null,
    resolutionNote: row.resolution_note || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    resolvedAt: row.resolved_at || null,
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

async function generateUniqueExactAmount(requestedAmount: number) {
  const base = Math.floor(requestedAmount);

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

async function findGreedIntentByExactAmountLocal(params: {
  exactAmount: number | string;
  status?: "pending" | "funded";
  intentType?: GreedIntentType | null;
}) {
  const exactAmount = formatAmount3(params.exactAmount);
  const status = String(params.status || "pending").trim();
  const intentType = params.intentType ? asGreedIntentType(params.intentType) : null;

  const r = await pool.query(
    `
    SELECT *
    FROM greed_deposit_intents
    WHERE exact_amount = $1::numeric
      AND status = $2
      AND expires_at > NOW()
      AND ($3::text IS NULL OR intent_type = $3)
    ORDER BY created_at ASC
    LIMIT 1;
    `,
    [exactAmount, status, intentType]
  );

  return r.rows[0] || null;
}

async function getOpenUnmatchedDepositCount() {
  const r = await pool.query(
    `
    SELECT COUNT(*)::INT AS count
    FROM greed_unmatched_deposits
    WHERE resolution_status = 'open';
    `
  );
  return Number(r.rows?.[0]?.count || 0);
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

async function sendMeaningfulGreedFeed(params: {
  message: string;
}) {
  if (!GREED_SPECTATOR_ENABLED) return;
  if (!GREED_SPECTATOR_CHAT_ID) return;
  if (!process.env.TG_BOT_TOKEN) return;

  try {
    await sendGymSpectatorMessage(params.message, {
      reply_markup: greedLaunchReplyMarkup("group"),
    });
  } catch (e) {
    console.error("Meaningful greed feed failed:", e);
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
// Greed stats / ranking helpers
// -------------------------------
async function getGreedGlobalStatsLocal() {
  const [aggRes, jackpotRes, roundsSinceRes] = await Promise.all([
    pool.query(
      `
      SELECT
        COALESCE(SUM(wager), 0)::DOUBLE PRECISION AS total_wagered,
        COUNT(*)::INT AS total_rounds,
        COUNT(*) FILTER (WHERE result = 'poison')::INT AS total_busts,
        COUNT(*) FILTER (WHERE result IN ('cashout', 'perfect'))::INT AS total_cashouts,
        COUNT(*) FILTER (WHERE result = 'perfect')::INT AS perfect_runs,
        COALESCE(MAX(payout), 0)::DOUBLE PRECISION AS biggest_cashout
      FROM greed_rounds
      WHERE status = 'closed';
      `
    ),
    getGreedJackpotState(),
    pool.query(
      `
      WITH last_jackpot AS (
        SELECT COALESCE(MAX(id), 0) AS last_id
        FROM greed_rounds
        WHERE status = 'closed'
          AND result = 'perfect'
      )
      SELECT COUNT(*)::INT AS rounds_since_jackpot
      FROM greed_rounds, last_jackpot
      WHERE greed_rounds.status = 'closed'
        AND greed_rounds.id > last_jackpot.last_id;
      `
    ),
  ]);

  const agg = aggRes.rows[0] || {};
  const totalRounds = Number(agg.total_rounds || 0);
  const totalBusts = Number(agg.total_busts || 0);
  const totalCashouts = Number(agg.total_cashouts || 0);

  return {
    total_wagered: Number(agg.total_wagered || 0),
    total_rounds: totalRounds,
    total_busts: totalBusts,
    total_cashouts: totalCashouts,
    perfect_runs: Number(agg.perfect_runs || 0),
    current_jackpot: Number(jackpotRes?.current_amount || 0),
    rounds_since_jackpot: Number(roundsSinceRes.rows?.[0]?.rounds_since_jackpot || 0),
    biggest_cashout: Number(agg.biggest_cashout || 0),
    bust_rate: totalRounds > 0 ? round3((totalBusts / totalRounds) * 100) : 0,
    cashout_rate: totalRounds > 0 ? round3((totalCashouts / totalRounds) * 100) : 0,
  };
}

async function getGreedPlayerStatsLocal(address: string): Promise<GreedPlayerStats | null> {
  const a = String(address || "").trim();
  if (!a) return null;

  const [userRow, aggRes] = await Promise.all([
    getMe(a),
    pool.query(
      `
      SELECT
        COALESCE(SUM(wager), 0)::DOUBLE PRECISION AS total_wagered,
        COALESCE(SUM(CASE WHEN payout > wager THEN payout - wager ELSE 0 END), 0)::DOUBLE PRECISION AS net_profit,
        COALESCE(SUM(CASE WHEN payout > 0 THEN payout ELSE 0 END), 0)::DOUBLE PRECISION AS total_won,
        COALESCE(SUM(CASE WHEN payout < wager THEN wager - payout ELSE 0 END), 0)::DOUBLE PRECISION AS total_lost,
        COUNT(*)::INT AS total_rounds,
        COUNT(*) FILTER (WHERE result = 'poison')::INT AS busts,
        COUNT(*) FILTER (WHERE result IN ('cashout', 'perfect'))::INT AS cashouts,
        COUNT(*) FILTER (WHERE result = 'perfect')::INT AS perfect_runs,
        COALESCE(MAX(payout), 0)::DOUBLE PRECISION AS biggest_cashout,
        COALESCE(MAX(jackpot_won), 0)::DOUBLE PRECISION AS biggest_jackpot,
        COUNT(*) FILTER (WHERE result = 'cashout' AND current_multiplier >= 2.5)::INT AS high_multiplier_cashouts,
        COALESCE(MAX(safe_clicks), 0)::INT AS best_run_depth
      FROM greed_rounds
      WHERE address = $1
        AND status = 'closed';
      `,
      [a]
    ),
  ]);

  const row = aggRes.rows[0] || {};
  const totalRounds = Number(row.total_rounds || 0);
  const cashouts = Number(row.cashouts || 0);
  const netProfit = Number(row.net_profit || 0);
  const totalWagered = Number(row.total_wagered || 0);
  const perfectRuns = Number(row.perfect_runs || 0);
  const highMultiplierCashouts = Number(row.high_multiplier_cashouts || 0);
  const greedScore = computeGreedScore({
    netProfit,
    totalWagered,
    perfectRuns,
    highMultiplierCashouts,
  });

  return {
    address: a,
    displayName: displayNameFromUserRow(userRow, a),
    total_wagered: totalWagered,
    net_profit: netProfit,
    total_won: Number(row.total_won || 0),
    total_lost: Number(row.total_lost || 0),
    total_rounds: totalRounds,
    busts: Number(row.busts || 0),
    cashouts,
    perfect_runs: perfectRuns,
    biggest_cashout: Number(row.biggest_cashout || 0),
    biggest_jackpot: Number(row.biggest_jackpot || 0),
    high_multiplier_cashouts: highMultiplierCashouts,
    best_run_depth: Number(row.best_run_depth || 0),
    cashout_rate: totalRounds > 0 ? round3((cashouts / totalRounds) * 100) : 0,
    greed_score: greedScore,
    tier: getGreedTier(greedScore),
    greed_gods_rank: null,
  };
}

async function getGreedGodsLeaderboardLocal(limit = 25) {
  const lim = Math.max(1, Math.min(200, Number(limit || 25)));

  const r = await pool.query(
    `
    WITH stats AS (
      SELECT
        gr.address,
        COALESCE(SUM(gr.wager), 0)::DOUBLE PRECISION AS total_wagered,
        COALESCE(SUM(CASE WHEN gr.payout > gr.wager THEN gr.payout - gr.wager ELSE 0 END), 0)::DOUBLE PRECISION AS net_profit,
        COUNT(*) FILTER (WHERE gr.result = 'perfect')::INT AS perfect_runs,
        COUNT(*) FILTER (WHERE gr.result = 'cashout' AND gr.current_multiplier >= 2.5)::INT AS high_multiplier_cashouts
      FROM greed_rounds gr
      WHERE gr.status = 'closed'
      GROUP BY gr.address
    )
    SELECT
      s.address,
      u.display_name,
      s.total_wagered,
      s.net_profit,
      s.perfect_runs,
      s.high_multiplier_cashouts,
      (
        (s.net_profit * 1.0) +
        (s.total_wagered * 0.05) +
        (s.perfect_runs * 5000) +
        (s.high_multiplier_cashouts * 2000)
      )::DOUBLE PRECISION AS greed_score
    FROM stats s
    LEFT JOIN users u ON u.address = s.address
    ORDER BY greed_score DESC, s.total_wagered DESC
    LIMIT $1;
    `,
    [lim]
  );

  return (r.rows || []).map((row: any, idx: number) => ({
    rank: idx + 1,
    address: row.address,
    displayName: row.display_name || null,
    totalWagered: Number(row.total_wagered || 0),
    netProfit: Number(row.net_profit || 0),
    perfectRuns: Number(row.perfect_runs || 0),
    highMultiplierCashouts: Number(row.high_multiplier_cashouts || 0),
    greedScore: Number(row.greed_score || 0),
    tier: getGreedTier(Number(row.greed_score || 0)),
  }));
}

async function getGreedGodRankForAddress(address: string) {
  const a = String(address || "").trim();
  if (!a) return null;

  const r = await pool.query(
    `
    WITH stats AS (
      SELECT
        gr.address,
        (
          (COALESCE(SUM(CASE WHEN gr.payout > gr.wager THEN gr.payout - gr.wager ELSE 0 END), 0)::DOUBLE PRECISION * 1.0) +
          (COALESCE(SUM(gr.wager), 0)::DOUBLE PRECISION * 0.05) +
          (COUNT(*) FILTER (WHERE gr.result = 'perfect')::INT * 5000) +
          (COUNT(*) FILTER (WHERE gr.result = 'cashout' AND gr.current_multiplier >= 2.5)::INT * 2000)
        )::DOUBLE PRECISION AS greed_score
      FROM greed_rounds gr
      WHERE gr.status = 'closed'
      GROUP BY gr.address
    ),
    ranked AS (
      SELECT
        address,
        greed_score,
        ROW_NUMBER() OVER (ORDER BY greed_score DESC, address ASC) AS rank_num
      FROM stats
    )
    SELECT rank_num
    FROM ranked
    WHERE address = $1
    LIMIT 1;
    `,
    [a]
  );

  return r.rows[0] ? Number(r.rows[0].rank_num || 0) : null;
}

async function findUserByGreedCardQuery(query: string) {
  const q = String(query || "").trim();
  if (!q) return null;

  if (q.startsWith("tg:")) {
    return getMe(q);
  }

  if (q.startsWith("@")) {
    const username = q.replace(/^@/, "").trim().toLowerCase();

    const r = await pool.query(
      `
      SELECT *
      FROM users
      WHERE LOWER(tg_username) = $1
      LIMIT 1;
      `,
      [username]
    );

    return r.rows[0] || null;
  }

  const r = await pool.query(
    `
    SELECT *
    FROM users
    WHERE LOWER(display_name) = $1
    LIMIT 1;
    `,
    [q.toLowerCase()]
  );

  return r.rows[0] || null;
}

// -------------------------------
// Solana watcher helpers
// -------------------------------
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

  const alreadyLoggedUnmatched = await getGreedUnmatchedDepositBySignature(signature);
  if (alreadyLoggedUnmatched && String(alreadyLoggedUnmatched.resolution_status || "") === "open") {
    return;
  }

  if (!solanaConnection) return;

  const parsedTx = await solanaConnection.getParsedTransaction(signature, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });

  if (!parsedTx || parsedTx.meta?.err) return;

  const observed = extractObservedDepositFromParsedTx(parsedTx);
  if (!observed.exactAmount || observed.exactAmount <= 0) return;

  const exactAmount = Number(formatAmount3(observed.exactAmount));
  const watcherTarget = PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET || null;

  const matchingIntent =
    (await findGreedIntentByExactAmountLocal({
      exactAmount,
      status: "pending",
      intentType: "single_round",
    })) ||
    (await findGreedIntentByExactAmountLocal({
      exactAmount,
      status: "pending",
      intentType: "balance_fund",
    })) ||
    (await findGreedIntentByExactAmountLocal({
      exactAmount,
      status: "pending",
      intentType: null,
    }));

  if (!matchingIntent) {
    await recordGreedUnmatchedDeposit({
      txSignature: signature,
      senderWallet: observed.senderWallet,
      tokenMint: observed.tokenMint || PHAT_TOKEN_MINT,
      observedAmount: exactAmount,
      watcherTarget,
      reason: "no_matching_pending_intent",
      resolutionStatus: "open",
      resolutionNote: "deposit observed but no pending intent matched this exact amount",
    });

    console.warn(
      `⚠️ Unmatched Greed deposit observed: sig=${signature} amount=${formatAmount3(exactAmount)} sender=${observed.senderWallet || "unknown"}`
    );
    return;
  }

  const intentType = asGreedIntentType(matchingIntent.intent_type);

  const funded = await markGreedDepositIntentFunded({
    id: Number(matchingIntent.id),
    address: String(matchingIntent.address),
    txSignature: signature,
    senderWallet: observed.senderWallet,
    tokenMint: observed.tokenMint || PHAT_TOKEN_MINT,
    fundedAmount: exactAmount,
    fundingMatchStatus: "exact",
  });

  if (!funded) {
    await recordGreedUnmatchedDeposit({
      txSignature: signature,
      senderWallet: observed.senderWallet,
      tokenMint: observed.tokenMint || PHAT_TOKEN_MINT,
      observedAmount: exactAmount,
      watcherTarget,
      matchedIntentId: Number(matchingIntent.id),
      matchedAddress: String(matchingIntent.address),
      reason: "intent_match_claim_failed",
      resolutionStatus: "open",
      resolutionNote: "matching intent was found but could not be marked funded",
    });

    console.warn(
      `⚠️ Greed deposit matched amount but funding claim failed: sig=${signature} intent=${matchingIntent.id} amount=${formatAmount3(exactAmount)}`
    );
    return;
  }

  const dep = await recordDeposit({
    address: String(funded.address),
    txSignature: signature,
    senderWallet: observed.senderWallet,
    tokenMint: observed.tokenMint || PHAT_TOKEN_MINT,
    amount: Number(funded.funded_amount || funded.exact_amount || exactAmount),
    status: "credited",
    note:
      intentType === "balance_fund"
        ? "greed balance fund credited by watcher"
        : "greed single-round intent funded by watcher",
  });

  if (!dep) {
    console.warn(`⚠️ Deposit record already existed after funding intent for tx ${signature}`);
  }

  if (intentType === "balance_fund") {
    const fundedAmount = round3(Number(funded.funded_amount || funded.exact_amount || exactAmount));

    await creditBalance({
      address: String(funded.address),
      amount: fundedAmount,
    });

    await consumeFundedGreedDepositIntent({
      id: Number(funded.id),
      address: String(funded.address),
    });
  }

  const existingUnmatched = await getGreedUnmatchedDepositBySignature(signature);
  if (existingUnmatched && String(existingUnmatched.resolution_status || "") === "open") {
    await markGreedUnmatchedDepositResolved({
      txSignature: signature,
      resolutionStatus: "resolved",
      resolutionNote: "deposit later matched and funded successfully",
      matchedIntentId: Number(funded.id),
      matchedAddress: String(funded.address),
    });
  }

  console.log(
    `✅ Greed watcher funded intent #${funded.id} (${intentType}) for ${funded.address} with ${formatAmount3(
      funded.funded_amount || funded.exact_amount || exactAmount
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
// Withdrawal worker helpers
// -------------------------------
function isLikelySolanaAddress(value: string) {
  const v = String(value || "").trim();
  if (!v) return false;
  if (v.startsWith("tg:")) return false;
  if (v.length < 32 || v.length > 44) return false;
  try {
    new PublicKey(v);
    return true;
  } catch {
    return false;
  }
}

async function sendSplWithdrawal(params: {
  destinationWallet: string;
  amount: number;
}) {
  if (!solanaConnection) {
    throw new Error("solana_connection_missing");
  }
  if (!bankrollKeypair) {
    throw new Error("bankroll_signer_missing");
  }
  if (!PHAT_TOKEN_MINT || PHAT_TOKEN_MINT === "PHAT") {
    throw new Error("invalid_phat_token_mint");
  }

  const mintPk = new PublicKey(PHAT_TOKEN_MINT);
  const ownerPk = bankrollKeypair.publicKey;
  const destinationOwnerPk = new PublicKey(params.destinationWallet);

  let tokenProgramId = TOKEN_PROGRAM_ID;
  let mintInfo: any = null;

  try {
    mintInfo = await getMint(solanaConnection, mintPk, "confirmed", TOKEN_PROGRAM_ID);
    tokenProgramId = TOKEN_PROGRAM_ID;
  } catch {
    mintInfo = await getMint(solanaConnection, mintPk, "confirmed", TOKEN_2022_PROGRAM_ID);
    tokenProgramId = TOKEN_2022_PROGRAM_ID;
  }

  const decimals = Number(mintInfo?.decimals ?? 0);
  const rawAmount = BigInt(Math.round(Number(params.amount) * 10 ** decimals));

  if (rawAmount <= 0n) {
    throw new Error("invalid_raw_amount");
  }

  const sourceAta = await getAssociatedTokenAddress(
    mintPk,
    ownerPk,
    false,
    tokenProgramId,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );

  const destinationAta = await getAssociatedTokenAddress(
    mintPk,
    destinationOwnerPk,
    false,
    tokenProgramId,
    ASSOCIATED_TOKEN_PROGRAM_ID
  );

  const destinationAtaInfo = await solanaConnection.getAccountInfo(destinationAta, "confirmed");

  const instructions: any[] = [];

  if (!destinationAtaInfo) {
    instructions.push(
      createAssociatedTokenAccountInstruction(
        ownerPk,
        destinationAta,
        destinationOwnerPk,
        mintPk,
        tokenProgramId,
        ASSOCIATED_TOKEN_PROGRAM_ID
      )
    );
  }

  instructions.push(
    createTransferInstruction(
      sourceAta,
      destinationAta,
      ownerPk,
      rawAmount,
      [],
      tokenProgramId
    )
  );

  const latestBlockhash = await solanaConnection.getLatestBlockhash("confirmed");

  const tx = new Transaction({
    feePayer: ownerPk,
    blockhash: latestBlockhash.blockhash,
    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
  });

  tx.add(...instructions);

  const signature = await sendAndConfirmTransaction(
    solanaConnection,
    tx,
    [bankrollKeypair],
    { commitment: "confirmed" }
  );

  return signature;
}

async function processPendingWithdrawalRow(withdrawal: any) {
  const withdrawalId = Number(withdrawal?.id || 0);
  const address = String(withdrawal?.address || "").trim();
  const destinationWallet = String(withdrawal?.destination_wallet || "").trim();
  const amount = Number(withdrawal?.amount || 0);

  if (!withdrawalId || !address || !destinationWallet || amount <= 0) return;

  const locked = await markWithdrawalProcessing({
    withdrawalId,
    note: "withdrawal worker claimed request",
  });

  if (!locked) return;

  try {
    if (!bankrollKeypair) {
      throw new Error("bankroll_signer_missing");
    }

    if (!isLikelySolanaAddress(destinationWallet)) {
      throw new Error("invalid_destination_wallet");
    }

    const txSignature = await sendSplWithdrawal({
      destinationWallet,
      amount,
    });

    const completed = await markWithdrawalCompleted({
      withdrawalId,
      txSignature,
      note: "on-chain withdrawal completed",
    });

    console.log(
      `✅ Withdrawal #${withdrawalId} completed for ${address} -> ${destinationWallet} amount ${formatAmount3(amount)} tx ${txSignature}`
    );

    return completed;
  } catch (err: any) {
    console.error(`❌ Withdrawal #${withdrawalId} failed:`, err);

    await creditBalance({ address, amount });

    await markWithdrawalFailed({
      withdrawalId,
      note: String(err?.message || "withdrawal failed; balance refunded"),
    });

    return null;
  }
}

async function tickWithdrawalsWorker() {
  if (!WITHDRAWALS_ENABLED) return;
  if (withdrawalsBusy) return;

  withdrawalsBusy = true;

  try {
    if (!bankrollKeypair) {
      return;
    }

    const rows = await getPendingWithdrawals(WITHDRAWALS_BATCH_LIMIT);
    if (!rows?.length) return;

    for (const withdrawal of rows) {
      await processPendingWithdrawalRow(withdrawal);
    }
  } catch (e) {
    console.error("❌ Withdrawals worker tick failed:", e);
  } finally {
    withdrawalsBusy = false;
  }
}

function startWithdrawalsWorker() {
  if (!WITHDRAWALS_ENABLED) {
    console.warn("⚠️ WITHDRAWALS_ENABLED=false. Withdrawal worker disabled.");
    return;
  }

  if (!bankrollKeypair) {
    console.warn("⚠️ No BANKROLL_PRIVATE_KEY signer loaded. Withdrawal worker disabled.");
    return;
  }

  if (!PHAT_TOKEN_MINT || PHAT_TOKEN_MINT === "PHAT") {
    console.warn("⚠️ PHAT_TOKEN_MINT is missing or placeholder. Withdrawal worker disabled.");
    return;
  }

  console.log(`💸 Withdrawal worker enabled`);
  console.log(`🏦 Bankroll wallet: ${bankrollWalletAddress}`);
  console.log(`🪙 PHAT token mint: ${PHAT_TOKEN_MINT}`);
  console.log(`⏱️ Withdrawal interval: ${WITHDRAWALS_INTERVAL_MS}ms`);

    tickWithdrawalsWorker().catch((e) => console.error("Initial withdrawals tick failed:", e));

  withdrawalsTimer = setInterval(() => {
    tickWithdrawalsWorker().catch((e) => console.error("Withdrawals interval failed:", e));
  }, WITHDRAWALS_INTERVAL_MS);
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
const GREED_MAX_BALANCE_FUND = 250000;
const GREED_TOTAL_DONUTS = 12;
const GREED_POISON_COUNT = 2;
const GREED_JACKPOT_RESEED = 25000;
const GREED_MULTIPLIERS = [1.10, 1.24, 1.40, 1.58, 1.80, 2.08, 2.42, 2.85, 3.50, 5.00];

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
        "  GET  /greed/global-stats",
        "  GET  /greed/player/:address",
        "  GET  /greed/gods",
        "",
        "tapping counts as cardio 🟣🟡",
      ].join("\n")
    );
});

app.get("/health", async (_req: Request, res: Response) =>
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
      openUnmatchedCount: await getOpenUnmatchedDepositCount(),
    },
    bankroll: {
      configured: !!bankrollWalletAddress,
      signerLoaded: !!bankrollKeypair,
      wallet: bankrollWalletAddress || null,
    },
    withdrawals: {
      enabled: WITHDRAWALS_ENABLED,
      signerLoaded: !!bankrollKeypair,
      intervalMs: WITHDRAWALS_INTERVAL_MS,
      batchLimit: WITHDRAWALS_BATCH_LIMIT,
      phatMint: PHAT_TOKEN_MINT || null,
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
    const openIntent = await getOpenGreedDepositIntentByAddress(address);

    return res.json({
      ok: true,
      address,
      balance: serializeBalanceRow(balance),
      openIntent: serializeGreedIntent(openIntent),
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
    bankrollWallet: bankrollWalletAddress || null,
    jackpotWallet: String(process.env.JACKPOT_WALLET || "").trim() || null,
    treasuryWallet: String(process.env.TREASURY_WALLET || "").trim() || null,
    acceptedToken: PHAT_TOKEN_MINT,
    mode: "intent-funding",
    intentPrecision: 3,
    fundingModes: {
      singleRoundWager: {
        key: "single_round",
        label: "Single Round Wager",
        description: "Create a unique 3-decimal funding intent for one locked round wager.",
      },
      internalBalanceFund: {
        key: "balance_fund",
        label: "Internal Balance Fund",
        description: "Create a unique 3-decimal funding intent to credit your internal balance for multiple future rounds.",
      },
    },
    quickWagers: [1000, 5000, 10000, 25000, 50000],
    maxBalanceFundAmount: GREED_MAX_BALANCE_FUND,
    bankrollSignerLoaded: !!bankrollKeypair,
    watcher: {
      enabled: SOLANA_WATCH_ENABLED,
      intervalMs: SOLANA_WATCH_INTERVAL_MS,
      watcherTarget: PHAT_TOKEN_ACCOUNT || DEPOSIT_WALLET || null,
      uniqueExactAmountRequired: true,
      decimalsRequired: 3,
      supportsIntentTypes: ["single_round", "balance_fund"],
    },
    withdrawals: {
      enabled: WITHDRAWALS_ENABLED,
      signerLoaded: !!bankrollKeypair,
      payoutWallet: bankrollWalletAddress || null,
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

    if (!destinationWallet) {
      return res.status(400).json({ error: "Missing destination wallet" });
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
      payoutWalletReady: !!bankrollKeypair,
      payoutWallet: bankrollWalletAddress || null,
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
  const openIntent = await getOpenGreedDepositIntentByAddress(address);

  res.json({
    address: me.address,
    displayName: me.display_name || null,
    balance: serializeBalanceRow(balance),
    openIntent: serializeGreedIntent(openIntent),
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
    const requestedTypeRaw = req.query.intentType ?? req.query.fundingMode ?? null;
    const requestedType = requestedTypeRaw ? asGreedIntentType(requestedTypeRaw) : undefined;

    await expireStaleGreedDepositIntents(address);

    const intent = await getOpenGreedDepositIntentByAddress(address, requestedType);

    return res.json({
      ok: true,
      intent: serializeGreedIntent(intent),
      funding: {
        minWager: GREED_MIN_WAGER,
        maxWager: GREED_MAX_WAGER,
        maxBalanceFundAmount: GREED_MAX_BALANCE_FUND,
        quickWagers: [1000, 5000, 10000, 25000, 50000],
        acceptedToken: PHAT_TOKEN_MINT,
        depositWallet: DEPOSIT_WALLET || null,
        expiresInMinutes: GREED_INTENT_EXPIRES_MINUTES,
        intentPrecision: 3,
        uniqueExactAmountRequired: true,
        fundingModes: {
          single_round: {
            label: "Single Round Wager",
            description: "Fund one exact wager amount for one locked round.",
          },
          balance_fund: {
            label: "Internal Balance Fund",
            description: "Fund your internal balance with a unique exact amount for multiple future rounds.",
          },
        },
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
    const intentType = asGreedIntentType(req.body?.intentType ?? req.body?.fundingMode ?? "single_round");

    const requestedWager =
      req.body?.wager == null ? null : sanitizeWager(req.body?.wager);

    const balanceFundAmount =
      req.body?.amount == null ? null : sanitizeBalanceFundAmount(req.body?.amount);

    let requestedAmount: number | null = null;
    let requestedWagerForIntent = 0;

    if (intentType === "single_round") {
      requestedAmount = requestedWager;
      requestedWagerForIntent = requestedWager || 0;

      if (requestedAmount == null) {
        return res.status(400).json({
          error: `Wager must be between ${GREED_MIN_WAGER} and ${GREED_MAX_WAGER}`,
        });
      }
    } else {
      requestedAmount = balanceFundAmount;

      if (requestedAmount == null) {
        return res.status(400).json({
          error: `Balance funding amount must be between ${GREED_MIN_WAGER} and ${GREED_MAX_BALANCE_FUND}`,
        });
      }

      requestedWagerForIntent =
        requestedWager != null ? requestedWager : Math.min(Math.floor(requestedAmount), GREED_MAX_WAGER);
    }

    if (!DEPOSIT_WALLET) {
      return res.status(500).json({ error: "Missing DEPOSIT_WALLET configuration" });
    }

    await expireStaleGreedDepositIntents(address);

    const existingTyped = await getOpenGreedDepositIntentByAddress(address, intentType);
    if (existingTyped) {
      return res.json({
        ok: true,
        reused: true,
        intentType,
        intent: serializeGreedIntent(existingTyped),
      });
    }

    const existingAny = await getOpenGreedDepositIntentByAddress(address);
    if (existingAny) {
      return res.status(409).json({
        error: "Open deposit intent already exists",
        code: "OPEN_INTENT_EXISTS",
        intent: serializeGreedIntent(existingAny),
      });
    }

    const exactAmount = await generateUniqueExactAmount(requestedAmount);

    const intent = await createGreedDepositIntent({
      address,
      intentType,
      requestedWager: requestedWagerForIntent,
      exactAmount,
      depositWallet: DEPOSIT_WALLET,
      tokenMint: PHAT_TOKEN_MINT,
      expiresInMinutes: GREED_INTENT_EXPIRES_MINUTES,
    });

    return res.json({
      ok: true,
      reused: false,
      intentType,
      intent: serializeGreedIntent(intent),
      fundingPreview: {
        requestedAmount,
        requestedWager: requestedWagerForIntent,
        exactAmount,
        intentPrecision: 3,
        useCase:
          intentType === "balance_fund"
            ? "Credit internal balance after watcher/admin funding match."
            : "Use for one single locked round wager.",
      },
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
    let row = await getGreedJackpotState();

    if (!row || Number(row.current_amount || 0) < GREED_JACKPOT_RESEED) {
      row = await setGreedJackpotAmount(GREED_JACKPOT_RESEED);
    }

    return res.json({ ok: true, jackpot: row });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch jackpot" });
  }
});

app.get("/greed/global-stats", async (_req: Request, res: Response) => {
  try {
    const stats = await getGreedGlobalStatsLocal();
    return res.json({ ok: true, stats });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch global stats" });
  }
});

app.get("/greed/card", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const stats = await getGreedPlayerStatsLocal(address);

    if (!stats) {
      return res.status(404).json({ error: "Player not found" });
    }

    stats.greed_gods_rank = await getGreedGodRankForAddress(address);

    return res.json({
      ok: true,
      card: {
        address: stats.address,
        displayName: stats.displayName,
        total_wagered: stats.total_wagered,
        net_profit: stats.net_profit,
        total_won: stats.total_won,
        total_lost: stats.total_lost,
        total_rounds: stats.total_rounds,
        busts: stats.busts,
        cashouts: stats.cashouts,
        perfect_runs: stats.perfect_runs,
        biggest_cashout: stats.biggest_cashout,
        biggest_jackpot: stats.biggest_jackpot,
        high_multiplier_cashouts: stats.high_multiplier_cashouts,
        best_run_depth: stats.best_run_depth,
        cashout_rate: stats.cashout_rate,
        greed_score: stats.greed_score,
        tier: stats.tier,
        greed_gods_rank: stats.greed_gods_rank,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch greed card" });
  }
});

app.get("/greed/player/:address", async (req: Request, res: Response) => {
  try {
    const address = String(req.params.address || "").trim();
    const stats = await getGreedPlayerStatsLocal(address);

    if (!stats) {
      return res.status(404).json({ error: "Player not found" });
    }

    stats.greed_gods_rank = await getGreedGodRankForAddress(address);

    return res.json({ ok: true, stats });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch player stats" });
  }
});

app.get("/greed/gods", async (req: Request, res: Response) => {
  try {
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 25)));
    const rows = await getGreedGodsLeaderboardLocal(limit);
    return res.json({ ok: true, rows });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch greed gods leaderboard" });
  }
});

app.get("/greed/greed-gods", async (req: Request, res: Response) => {
  try {
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 25)));
    const rows = await getGreedGodsLeaderboardLocal(limit);
    return res.json({ ok: true, rows });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch greed gods leaderboard" });
  }
});

app.post("/greed/start", requireAuth, async (req: Request, res: Response) => {
  try {
    const address = (req as any).user?.address as string;
    const requestedWager = sanitizeWager(req.body?.wager);
    const spectatorChatId = getSpectatorChatIdFromReq(req);

    if (requestedWager == null) {
      return res.status(400).json({
        error: `Wager must be between ${GREED_MIN_WAGER} and ${GREED_MAX_WAGER}`,
      });
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
    let fundingModeUsed: "internal_balance" | "single_round_intent" = "internal_balance";

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
      fundingModeUsed = "internal_balance";
    } else {
      const openIntent = await getOpenGreedDepositIntentByAddress(address, "single_round");

      if (!openIntent) {
        return res.status(400).json({
          error: "Funding required",
          code: "FUNDING_REQUIRED",
          message: "Not enough internal balance and no funded single-round intent found.",
          wallet: {
            balance: serializeBalanceRow(bal),
            availableBalance: available,
            neededAmount: requestedWager,
            shortfall: round3(Math.max(0, requestedWager - available)),
          },
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
      const exactAmount = round3(Number(openIntent.funded_amount || openIntent.exact_amount || 0));

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

      lockedWager = round3(requestedWager);
      fundedIntentId = Number(consumedIntent.id);
      fundingSource = "intent";
      fundingModeUsed = "single_round_intent";
    }

    const { totalTax, devCut, treasuryCut, jackpotCut, netStake } =
      getGreedTaxBreakdown(lockedWager);

    const jackpotFeed = round3(lockedWager * GREED_JACKPOT_FEED_RATE);
    await addToGreedJackpot(jackpotFeed);

    const serverSeed = crypto.randomBytes(32).toString("hex");
    const commitHash = sha256Hex(serverSeed);
    const nonce = Date.now();
    const poisonIndices = derivePoisonIndicesFromSeed(
      serverSeed,
      nonce,
      GREED_TOTAL_DONUTS,
      GREED_POISON_COUNT
    );

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
      source:
        fundingSource === "balance"
          ? "greed_start_balance"
          : "greed_start_intent",
      grossWager: lockedWager,
      totalTax,
      devCut,
      treasuryCut,
      jackpotCut,
      note:
        fundingSource === "balance"
          ? "greed round started from internal balance"
          : `greed round started from funded single-round intent${
              fundedIntentId ? ` #${fundedIntentId}` : ""
            }`,
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
    `Single round wager: ${formatAmount3(requestedWager)} PHAT`,
    `Locked amount: ${formatAmount3(lockedWager)} PHAT`,
    `Funding source: ${
      fundingModeUsed === "internal_balance"
        ? "Internal Balance"
        : "Single Round Intent"
    }`,
    `Round ID: #${Number(round.id)}`,
    ``,
    `Pick your donut in chat before they do 👇`,
    `${formatDonutBoardLine()}`,
  ].join("\n"),
  {
    reply_markup: greedLaunchReplyMarkup(
      spectatorChatId ? "group" : "private"
    ),
  }
);

    return res.json({
      ok: true,
      roundId: round.id,
      singleRoundWager: requestedWager,
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
      fundingModeUsed,
      internalBalanceUsed: fundingModeUsed === "internal_balance",
      singleRoundIntentUsed: fundingModeUsed === "single_round_intent",
      totalDonuts: GREED_TOTAL_DONUTS,
      poisonCount: GREED_POISON_COUNT,
      currentMultiplier: 1.0,
      cashoutAvailable: false,
      fundedIntentId,
      wallet: {
        balanceBefore: available,
        usedAmount: lockedWager,
        remainingEstimated:
          fundingModeUsed === "internal_balance"
            ? round3(Math.max(0, available - lockedWager))
            : available,
      },
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

    if (
      !Number.isFinite(pickedIndex) ||
      pickedIndex < 0 ||
      pickedIndex >= GREED_TOTAL_DONUTS
    ) {
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

// reset jackpot cleanly to new 25k floor
await reseedGreedJackpot();
await setGreedJackpotAmount(GREED_JACKPOT_RESEED);

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

      await sendMeaningfulGreedFeed({
        message: [
          `👑 ${displayNameFromUserRow(await getMe(address), address)} just cleared a 10/10 box`,
          `Full payout secured: ${formatAmount3(totalPayout)} PHAT`,
          `Jackpot hit: ${formatAmount3(jackpotWon)} PHAT`,
        ].join("\n"),
      });

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

    if (currentMultiplier >= 2.5) {
      await sendMeaningfulGreedFeed({
        message: [
          `💸 ${displayNameFromUserRow(await getMe(address), address)} cashed out at ${currentMultiplier.toFixed(2)}x`,
          `Stacking ${formatAmount3(payout)} PHAT`,
        ].join("\n"),
      });
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
    const balance = await getBalance(address);
    const openIntent = await getOpenGreedDepositIntentByAddress(address);

    if (!round) {
      return res.json({
        active: false,
        round: null,
        wallet: {
          balance: serializeBalanceRow(balance),
          openIntent: serializeGreedIntent(openIntent),
        },
      });
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
      wallet: {
        balance: serializeBalanceRow(balance),
        openIntent: serializeGreedIntent(openIntent),
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

    const [mostWagered, mostWon, perfectRuns, biggestCashout, topGlazeSacrifices, greedGods] = await Promise.all([
      getGreedLeaderboard({ board: "most_wagered", window, limit }),
      getGreedLeaderboard({ board: "most_won", window, limit }),
      getGreedLeaderboard({ board: "perfect_runs", window, limit }),
      getGreedLeaderboard({ board: "biggest_cashout", window, limit }),
      getGreedLeaderboard({ board: "top_glaze_sacrifices", window, limit }),
      getGreedGodsLeaderboardLocal(limit),
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
        greedGods,
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
    const tokenMint = String(req.body?.tokenMint || "").trim() || PHAT_TOKEN_MINT;
    const amount = parseAmount3(req.body?.amount);
    const note = String(req.body?.note || "manual admin balance credit").trim();

    const resolveUnmatched = String(req.body?.resolveUnmatched || "true").trim().toLowerCase() !== "false";
    const resolutionStatus = String(req.body?.resolutionStatus || "resolved").trim() || "resolved";
    const resolutionNote =
      String(req.body?.resolutionNote || "").trim() || "admin manually credited deposit to user balance";

    if (!address) {
      return res.status(400).json({ error: "Missing address" });
    }

    if (!txSignature) {
      return res.status(400).json({ error: "Missing txSignature" });
    }

    if (amount == null || amount <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    const exists = await hasDepositTxSignature(txSignature);
    if (exists) {
      return res.status(400).json({ error: "Transaction already processed" });
    }

    const dep = await recordDeposit({
      address,
      txSignature,
      senderWallet,
      tokenMint,
      amount,
      status: "credited",
      note,
    });

    if (!dep) {
      return res.status(400).json({ error: "Transaction already processed" });
    }

    await creditBalance({ address, amount });

    let unmatchedResolution: any = null;
    if (resolveUnmatched) {
      const existingUnmatched = await getGreedUnmatchedDepositBySignature(txSignature);
      if (existingUnmatched && String(existingUnmatched.resolution_status || "") === "open") {
        unmatchedResolution = await markGreedUnmatchedDepositResolved({
          txSignature,
          resolutionStatus,
          resolutionNote,
          matchedAddress: address,
        });
      }
    }

    return res.json({
      ok: true,
      deposit: dep,
      creditedBalanceAmount: amount,
      unmatchedResolution: unmatchedResolution
        ? serializeUnmatchedDeposit(unmatchedResolution)
        : null,
    });
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

      intent =
        (await findGreedIntentByExactAmountLocal({
          exactAmount,
          status: "pending",
          intentType: "single_round",
        })) ||
        (await findGreedIntentByExactAmountLocal({
          exactAmount,
          status: "pending",
          intentType: "balance_fund",
        })) ||
        (await findGreedIntentByExactAmountLocal({
          exactAmount,
          status: "pending",
          intentType: null,
        }));
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

    const intentType = asGreedIntentType(intent.intent_type);

    const funded = await markGreedDepositIntentFunded({
      id: Number(intent.id),
      address: String(intent.address),
      txSignature,
      senderWallet,
      tokenMint,
      fundedAmount: Number(intent.exact_amount || exactAmount || 0),
      fundingMatchStatus: "exact",
    });

    if (!funded) {
      return res.status(400).json({ error: "Intent could not be marked funded" });
    }

    const fundedAmount = Number(funded.funded_amount || funded.exact_amount || 0);

    const dep = await recordDeposit({
      address: String(funded.address),
      txSignature,
      senderWallet,
      tokenMint,
      amount: fundedAmount,
      status: "credited",
      note:
        intentType === "balance_fund"
          ? "greed balance fund credited by admin"
          : "greed single-round intent funded by admin",
    });

    if (intentType === "balance_fund") {
      await creditBalance({
        address: String(funded.address),
        amount: fundedAmount,
      });

      await consumeFundedGreedDepositIntent({
        id: Number(funded.id),
        address: String(funded.address),
      });
    }

    let unmatchedResolution: any = null;
    const existingUnmatched = await getGreedUnmatchedDepositBySignature(txSignature);
    if (existingUnmatched && String(existingUnmatched.resolution_status || "") === "open") {
      unmatchedResolution = await markGreedUnmatchedDepositResolved({
        txSignature,
        resolutionStatus: "resolved",
        resolutionNote: "admin matched this deposit to a valid greed intent",
        matchedIntentId: Number(funded.id),
        matchedAddress: String(funded.address),
      });
    }

    return res.json({
      ok: true,
      intent: serializeGreedIntent(funded),
      deposit: dep,
      creditedToBalance: intentType === "balance_fund",
      creditedAmount: intentType === "balance_fund" ? fundedAmount : 0,
      unmatchedResolution: unmatchedResolution
        ? serializeUnmatchedDeposit(unmatchedResolution)
        : null,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Fund intent failed" });
  }
});

app.get("/admin/greed/unmatched", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const limit = Math.max(1, Math.min(200, Number(req.query.limit || 50)));
    const rows = await getOpenGreedUnmatchedDeposits(limit);

    return res.json({
      ok: true,
      count: rows.length,
      rows: rows.map(serializeUnmatchedDeposit),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch unmatched deposits" });
  }
});

app.get("/admin/greed/treasury", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const snapshot = await getGreedAdminTreasurySnapshot();
    return res.json({ ok: true, snapshot });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to fetch treasury snapshot" });
  }
});

app.post("/admin/greed/unmatched/resolve", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const txSignature = String(req.body?.txSignature || "").trim();
    const resolutionStatus = String(req.body?.resolutionStatus || "resolved").trim() || "resolved";
    const resolutionNote = String(req.body?.resolutionNote || "").trim() || null;
    const matchedIntentId =
      req.body?.matchedIntentId == null
        ? null
        : Math.floor(Number(req.body.matchedIntentId || 0)) || null;
    const matchedAddress = String(req.body?.matchedAddress || "").trim() || null;

    if (!txSignature) {
      return res.status(400).json({ error: "Missing txSignature" });
    }

    const row = await markGreedUnmatchedDepositResolved({
      txSignature,
      resolutionStatus,
      resolutionNote,
      matchedIntentId,
      matchedAddress,
    });

    if (!row) {
      return res.status(404).json({ error: "Unmatched deposit not found" });
    }

    return res.json({
      ok: true,
      row: serializeUnmatchedDeposit(row),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Failed to resolve unmatched deposit" });
  }
});

app.post("/admin/jackpot/reseed", async (req: Request, res: Response) => {
  if (!requireAdmin(req, res)) return;

  try {
    const amount = parseAmount3(req.body?.amount ?? GREED_JACKPOT_RESEED);
    const row = await setGreedJackpotAmount(
      amount == null ? GREED_JACKPOT_RESEED : amount
    );

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
    await pool.query(`TRUNCATE TABLE greed_unmatched_deposits CASCADE;`);
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

function isPrivateChat(ctx: any) {
  return ctx?.chat?.type === "private";
}

function getBotUsername(ctx: any) {
  const fromCtx = String(ctx?.botInfo?.username || "").trim().replace(/^@/, "");
  return fromCtx || TG_BOT_USERNAME || "";
}

function buildBotDeepLink(ctx: any, startParam: string) {
  const username = getBotUsername(ctx);
  if (!username) return null;
  return `https://t.me/${username}?start=${encodeURIComponent(startParam)}`;
}

function gymLaunchKeyboard(ctx: any) {
  const privateChat = isPrivateChat(ctx);
  const greedDmLink = buildBotDeepLink(ctx, "greed");

  if (!privateChat) {
    const rows: any[] = [
      [Markup.button.webApp("Open Planet Fatness Gym", HUB_WEBAPP_URL)],
      [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
    ];

    if (greedDmLink) {
      rows.push([Markup.button.url("Open Greed in DM", greedDmLink)]);
    }

    return Markup.inlineKeyboard(rows);
  }

  return Markup.inlineKeyboard([
    [Markup.button.webApp("Open Planet Fatness Gym", HUB_WEBAPP_URL)],
    [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
  ]);
}

function greedLaunchKeyboard(ctx: any) {
  const privateChat = isPrivateChat(ctx);
  const greedDmLink = buildBotDeepLink(ctx, "greed");

  if (!privateChat) {
    const rows: any[] = [
      [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
    ];

    if (greedDmLink) {
      rows.push([Markup.button.url("Open in DM", greedDmLink)]);
    }

    return Markup.inlineKeyboard(rows);
  }

  return Markup.inlineKeyboard([
    [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
  ]);
}

function startLaunchKeyboard(ctx: any) {
  return gymLaunchKeyboard(ctx);
}

function greedLaunchReplyMarkup(chatType?: string) {
  const privateChat = chatType === "private";

  if (!privateChat && TG_BOT_USERNAME) {
    return Markup.inlineKeyboard([
      [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
      [Markup.button.url("Open in DM", `https://t.me/${TG_BOT_USERNAME}?start=greed`)],
    ]).reply_markup;
  }

  return Markup.inlineKeyboard([
    [Markup.button.webApp("Open Feed Your Greed", GREED_WEBAPP_URL)],
  ]).reply_markup;
}

// -------------------------------
// Telegram game launch
// -------------------------------
const bot = new Telegraf(process.env.GREED_BOT_TOKEN || "");

bot.command("game", async (ctx) => {
  try {
    await ctx.reply(
      "🍩 Feed Your Greed is live.\nTap below to open the official game.",
      greedLaunchKeyboard(ctx)
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
      greedLaunchKeyboard(ctx)
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
    const rawText = String(ctx.message?.text || "").trim().toLowerCase();
    const isGreedDeepLink = rawText.includes("greed");

    if (isGreedDeepLink) {
      await ctx.reply(
        [
          "🍩 FEED YOUR GREED",
          "",
          "Fund. Pick. Cash out — or get wiped.",
          "",
          "12 donuts. 2 poison.",
          "Multipliers climb every click.",
          "",
          "Tap below to launch the game.",
        ].join("\n"),
        greedLaunchKeyboard(ctx)
      );
      return;
    }

    await ctx.reply(
      "🏋️ Welcome back to Planet Fatness Gym! Tap below to open the app.",
      startLaunchKeyboard(ctx)
    );
  } catch (e) {
    console.error("GYM /start button error:", e);
    try {
      await ctx.reply("🏋️ Welcome back to Planet Fatness Gym!");
    } catch {}
  }
});

gymBot.command("gym", async (ctx) => {
  try {
    await ctx.reply(
      isPrivateChat(ctx)
        ? "🏋️ Welcome back to Planet Fatness Gym! Tap below to open the app."
        : "🏋️ Open the gym below. Greed also has a backup DM launcher if Telegram acts weird in group.",
      gymLaunchKeyboard(ctx)
    );
  } catch (e) {
    console.error("GYM /gym button error:", e);
    try {
      await ctx.reply("🏋️ Welcome back to Planet Fatness Gym!");
    } catch {}
  }
});

gymBot.command("greed", async (ctx) => {
  try {
    await ctx.reply(
      [
        "🍩 FEED YOUR GREED",
        "",
        "Fund. Pick. Cash out — or get wiped.",
        "",
        "12 donuts. 2 poison.",
        "Multipliers climb every click.",
        "",
        isPrivateChat(ctx)
          ? "Tap below to launch the game."
          : "Tap below to launch now, or use the DM backup if Telegram gets weird in group.",
      ].join("\n"),
      greedLaunchKeyboard(ctx)
    );
  } catch (e) {
    console.error("GYM /greed button error:", e);
    try {
      await ctx.reply("🍩 Open Feed Your Greed");
    } catch {}
  }
});

gymBot.command("greedguide", async (ctx) => {
  try {
    await ctx.reply(
      [
        "🍩 FEED YOUR GREED — QUICK GUIDE",
        "",
        "Choose how you want to play:",
        "",
        "• Play One Round → fund a single wager",
        "• Load Balance → deposit once, play multiple rounds",
        "",
        "When funding is needed, you will get:",
        "• a wallet",
        "• an exact PHAT amount",
        "",
        "Send that exact amount to that exact wallet.",
        "",
        "Do not round it.",
        "Do not change it.",
        "Do not send to a different wallet.",
        "",
        "If it matches exactly, your account gets credited.",
        "",
        "After funding:",
        "• default wager is 1,000 PHAT",
        "• increase it if your balance covers more",
        "",
        "Game:",
        "12 donuts",
        "2 are poison",
        "Each safe pick raises the multiplier",
        "",
        "Cash out after 1 safe pick…",
        "or push for the full clear.",
        "",
        "Flow:",
        "Deposit → balance updates → choose wager → run it",
      ].join("\n"),
      greedLaunchKeyboard(ctx)
    );
  } catch (e) {
    console.error("GYM /greedguide error:", e);
    try {
      await ctx.reply("🍩 Use /greed to play Feed Your Greed.");
    } catch {}
  }
});

gymBot.command("greedlive", async (ctx) => {
  try {
    await ctx.reply(
      [
        "🍩 FEED YOUR GREED LIVE",
        "Watch the next degen lock a round and scream your donut pick in chat.",
      ].join("\n"),
      greedLaunchKeyboard(ctx)
    );
  } catch (e) {
    console.error("GYM /greedlive button error:", e);
  }
});

gymBot.command("greedcard", async (ctx) => {
  try {
    const rawText = String(ctx.message?.text || "").trim();
    const parts = rawText.split(/\s+/);
    const query = parts.slice(1).join(" ").trim();

    let userRow: any = null;

    if (query) {
      userRow = await findUserByGreedCardQuery(query);
    } else {
      userRow = await getMe(`tg:${ctx.from.id}`);
    }

    if (!userRow) {
      await ctx.reply("User not found.");
      return;
    }

    const stats = await getGreedPlayerStatsLocal(String(userRow.address || ""));
    if (!stats) {
      await ctx.reply("No Greed stats yet.");
      return;
    }

    stats.greed_gods_rank = await getGreedGodRankForAddress(stats.address);

    const msg = [
      `🏆 GREED CARD`,
      `${stats.displayName}`,
      `Greed Gods Rank #${stats.greed_gods_rank || "-"}`,
      `Tier ${stats.tier}`,
      ``,
      `Action`,
      `Big Appetites ${formatAmount3(stats.total_wagered)} PHAT`,
      `Total Rounds ${stats.total_rounds}`,
      ``,
      `Performance`,
      `Phat Stacks ${formatAmount3(stats.total_won)} PHAT`,
      `Cashout Rate ${formatPct(stats.cashout_rate)}`,
      `10/10 Boxes ${stats.perfect_runs}`,
      ``,
      `Damage`,
      `Glaze Donors ${formatAmount3(stats.total_lost)} PHAT`,
      `Busts ${stats.busts}`,
      ``,
      `Highlights`,
      `Biggest Cashout ${formatAmount3(stats.biggest_cashout)} PHAT`,
      `Best Run Depth ${stats.best_run_depth}`,
      ``,
      `Prestige`,
      `Greed Score ${formatAmount3(stats.greed_score)}`,
    ].join("\n");

        await ctx.reply(
      msg,
      greedLaunchKeyboard(ctx)
    );
  } catch (e) {
    console.error("GYM /greedcard error:", e);
    try {
      await ctx.reply("Failed to load Greed Card.");
    } catch {}
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
  startWithdrawalsWorker();
  startGreedShoutLoop();

  app.listen(PORT, () => console.log(`✅ Planet Fatness backend on :${PORT}`));
} catch (e) {
  console.error("❌ Failed to boot:", e);
  process.exit(1);
}