const path = require("path");
require("dotenv").config({
  path: path.join(__dirname, ".env"),
});

function trimSlash(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function normalizeFuturesRestUrl(rawValue) {
  const raw = trimSlash(rawValue);
  if (!raw) return "https://fapi.binance.com";

  const lower = raw.toLowerCase();

  if (
    lower.includes("api.binance.com/api/v3") ||
    lower.includes("api-gcp.binance.com/api/v3") ||
    /https:\/\/api[1-4]\.binance\.com\/api\/v3/.test(lower)
  ) {
    return "https://fapi.binance.com";
  }

  if (lower.includes("/fapi/")) {
    return raw.slice(0, lower.indexOf("/fapi/"));
  }

  return raw;
}

function parseIntervalList(rawValue, fallback = []) {
  if (!rawValue) return fallback;
  const values = String(rawValue)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return values.length ? [...new Set(values)] : fallback;
}

function parseIdList(rawValue) {
  if (!rawValue) return [];
  return [...new Set(
    String(rawValue)
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
  )];
}

function normalizeCapitalMode(value, fallback = "SIMPLE") {
  return String(value || fallback).trim().toUpperCase() === "COMPOUNDING"
    ? "COMPOUNDING"
    : "SIMPLE";
}

function normalizeExecutionMode(value, fallback = "DEMO") {
  return String(value || fallback).trim().toUpperCase() === "REAL"
    ? "REAL"
    : "DEMO";
}

const storageDir = path.join(__dirname, "storage");
const strategiesDir = path.join(storageDir, "strategies");
const exportsDir = path.join(storageDir, "exports");
const notifyMinScore = Number(
  process.env.NOTIFY_MIN_SCORE || process.env.NOTIFY_ABOVE_SCORE || 80
);

module.exports = {
  storageDir,
  exportsDir,
  env: process.env.NODE_ENV || "development",
  sqlitePath: path.join(storageDir, "bot-state.sqlite"),

  binanceApiUrl: normalizeFuturesRestUrl(
    process.env.BINANCE_FUTURES_API_URL || process.env.BINANCE_API_URL
  ),
  binanceWsUrl: trimSlash(
    process.env.BINANCE_FUTURES_WS_URL ||
      process.env.BINANCE_WS_URL ||
      "wss://fstream.binance.com"
  ),

  telegramBotToken: process.env.TELEGRAM_BOT_TOKEN || "",
  telegramChatId: process.env.TELEGRAM_CHAT_ID || "",
  telegramAdminIds: parseIdList(
    process.env.TELEGRAM_ADMIN_IDS || process.env.TELEGRAM_ADMIN_ID
  ),
  telegramPolling:
    String(process.env.TELEGRAM_POLLING || "true").toLowerCase() === "true",
  telegramEphemeralUiTtlMs: Number(process.env.TELEGRAM_EPHEMERAL_UI_TTL_MS || 30_000),

  hyperliquidApiUrl: trimSlash(
    process.env.HYPERLIQUID_API_URL || "https://api.hyperliquid.xyz"
  ),
  hyperliquidSecretKey: process.env.HYPERLIQUID_SECRET_KEY || "",
  hyperliquidAccountAddress: String(process.env.HYPERLIQUID_ACCOUNT_ADDRESS || "").toLowerCase(),
  hyperliquidVaultAddress: String(process.env.HYPERLIQUID_VAULT_ADDRESS || "").toLowerCase(),
  hyperliquidPythonBin: process.env.HYPERLIQUID_PYTHON_BIN || "python3",
  hyperliquidSlippage: Number(process.env.HYPERLIQUID_SLIPPAGE || 0.05),
  hyperliquidMetaCacheMs: Number(process.env.HYPERLIQUID_META_CACHE_MS || 10 * 60 * 1000),
  hyperliquidRequestConcurrency: Number(process.env.HYPERLIQUID_REQUEST_CONCURRENCY || 1),
  autoTradeEnabled:
    String(process.env.AUTO_TRADE_ENABLED || "true").toLowerCase() === "true",
  defaultTradeLeverage: Number(process.env.DEFAULT_TRADE_LEVERAGE || 10),
  defaultTradeBalance: Number(process.env.DEFAULT_TRADE_BALANCE || 100),
  defaultCapitalMode: normalizeCapitalMode(process.env.DEFAULT_CAPITAL_MODE || "SIMPLE"),
  defaultExecutionMode: normalizeExecutionMode(process.env.DEFAULT_EXECUTION_MODE || "DEMO"),
  defaultBaselinePrincipal: Number(
    process.env.DEFAULT_BASELINE_PRINCIPAL || process.env.DEFAULT_TRADE_BALANCE || 100
  ),
  defaultSimpleSlots: Math.max(1, Math.trunc(Number(process.env.DEFAULT_SIMPLE_SLOTS || 1))),
  defaultDemoBalance: Number(process.env.DEFAULT_DEMO_BALANCE || 100),
  defaultEntryTimeoutMs: Number(process.env.DEFAULT_ENTRY_TIMEOUT_MS || 10 * 60 * 1000),
  demoFeeRate: Number(process.env.DEMO_FEE_RATE || 0.00045),
  demoFundingRatePerHour: Number(process.env.DEMO_FUNDING_RATE_PER_HOUR || 0),
  forceModeChangeEnabled:
    String(process.env.FORCE_MODE_CHANGE_ENABLED || "false").toLowerCase() === "true",
  stateEncryptionKey:
    process.env.BOT_STATE_ENCRYPTION_KEY ||
    process.env.STATE_ENCRYPTION_KEY ||
    process.env.TELEGRAM_BOT_TOKEN ||
    "local-dev-fallback-key",
  tradeMonitorMs: Number(process.env.TRADE_MONITOR_MS || 5_000),
  fillLookbackMs: Number(process.env.FILL_LOOKBACK_MS || 6 * 60 * 60 * 1000),

  scanEveryMs: Number(process.env.SCAN_EVERY_MS || 60_000),
  maxKlinesPerRequest: Number(process.env.MAX_KLINES_PER_REQUEST || 300),
  maxParallelRequests: Number(process.env.MAX_PARALLEL_REQUESTS || 4),

  watchThreshold: Number(process.env.WATCH_THRESHOLD || 70),
  strongThreshold: Number(process.env.STRONG_THRESHOLD || 80),
  alertThreshold: Number(process.env.ALERT_THRESHOLD || 90),
  scoreRiseThreshold: Number(process.env.SCORE_RISE_THRESHOLD || 5),
  notifyMinScore,
  notifyAboveScore: notifyMinScore,

  scanAllValidUsdtPairs:
    String(process.env.SCAN_ALL_VALID_USDT_PAIRS || "false").toLowerCase() === "true",
  maxScanPairs: Number(process.env.MAX_SCAN_PAIRS || 0),
  prioritizeWatchedPairs:
    String(process.env.PRIORITIZE_WATCHED_PAIRS || "true").toLowerCase() === "true",

  pairsPath: path.join(storageDir, "pairs.json"),
  scoreStatePath: path.join(storageDir, "score-state.json"),
  activeSignalsPath: path.join(storageDir, "active-signals.json"),
  dryRunPositionsPath: path.join(storageDir, "dryrun-positions.json"),
  closedTradesPath: path.join(storageDir, "closed-trades.json"),
  learnedPumpsPath: path.join(storageDir, "learned-pumps.json"),

  strategiesDir,
  strategiesIndexPath: path.join(strategiesDir, "index.json"),

  supportedKlineIntervals: [
    "1m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w",
  ],

  scanKlineIntervals: parseIntervalList(process.env.SCAN_KLINE_INTERVALS, [
    "1m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w",
  ]),

  supportedFlowPeriods: ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],

  timeframeHierarchyMap: {
    "1m": ["1m", "5m", "15m"],
    "5m": ["5m", "15m", "30m", "1h"],
    "15m": ["15m", "30m", "1h", "4h"],
    "30m": ["30m", "1h", "2h", "4h"],
    "1h": ["1h", "2h", "4h", "1d"],
    "2h": ["2h", "4h", "6h", "1d"],
    "4h": ["4h", "6h", "12h", "1d", "3d"],
    "6h": ["6h", "12h", "1d", "3d"],
    "8h": ["8h", "12h", "1d", "3d"],
    "12h": ["12h", "1d", "3d", "1w"],
    "1d": ["4h", "12h", "1d", "3d", "1w"],
    "3d": ["12h", "1d", "3d", "1w"],
    "1w": ["1d", "3d", "1w"],
  },

  strategyCap: Math.max(1, Math.trunc(Number(process.env.STRATEGY_CAP || 500))),
  strategyRetentionDays: Math.max(
    1,
    Math.trunc(Number(process.env.STRATEGY_RETENTION_DAYS || 7))
  ),
  strategyPruneIntervalMs: Number(
    process.env.STRATEGY_PRUNE_INTERVAL_MS || 7 * 24 * 60 * 60 * 1000
  ),
};
