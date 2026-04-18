const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const config = require("./config");
const defaultPairs = require("./pair");
const { encryptSecret, decryptSecret, fingerprintSecret } = require("./security");

const SETTINGS_KEYS = {
  legacyMigrated: "__legacyMigrated",
  strategyCap: "strategyCap",
  strategyRetentionDays: "strategyRetentionDays",
  lastStrategyPruneAt: "lastStrategyPruneAt",
};

const PROFILE_SETTINGS_KEYS = {
  autoTradeEnabled: "autoTradeEnabled",
  tradeBalanceTarget: "tradeBalanceTarget",
  tradeLeverage: "tradeLeverage",
  capitalMode: "capitalMode",
  executionMode: "executionMode",
  baselinePrincipal: "baselinePrincipal",
  simpleSlots: "simpleSlots",
  demoPerpBalance: "demoPerpBalance",
  demoSpotBalance: "demoSpotBalance",
  demoStartingBalance: "demoStartingBalance",
  totalSweptToSpot: "totalSweptToSpot",
  lastSweepAmount: "lastSweepAmount",
  demoTotalSweptToSpot: "demoTotalSweptToSpot",
  demoLastSweepAmount: "demoLastSweepAmount",
  entryTimeoutMs: "entryTimeoutMs",
  lastFillSyncAt: "lastFillSyncAt",
  lastReconcileAt: "lastReconcileAt",
  lastReconcileSummary: "lastReconcileSummary",
  strategyCap: "strategyCap",
  strategyRetentionDays: "strategyRetentionDays",
};

const DEFAULT_PROFILE_ID = "system:default";
const TERMINAL_TRADE_STATUSES = ["CLOSED", "REJECTED", "CANCELED", "ORPHANED"];
const ACTIVE_LOCKED_TRADE_STATUSES = [
  "ENTRY_PENDING",
  "ENTRY_PLACED",
  "PARTIALLY_FILLED",
  "OPEN",
  "PROTECTED",
  "EXIT_PENDING",
  "RECONCILING",
];

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function nowIso() {
  return new Date().toISOString();
}

function uniqueUpper(values) {
  return [...new Set((values || []).map((value) => String(value).trim().toUpperCase()).filter(Boolean))];
}

function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function parseJsonSafe(raw, fallback = null) {
  if (raw == null || raw === "") return fallback;
  try {
    return JSON.parse(raw);
  } catch (error) {
    return fallback;
  }
}

function jsonValue(value) {
  return sqlQuote(JSON.stringify(value ?? null));
}

function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function safeInteger(value, fallback = 0) {
  const num = Math.trunc(Number(value));
  return Number.isFinite(num) ? num : fallback;
}

function normalizeCapitalMode(value, fallback = config.defaultCapitalMode || "SIMPLE") {
  return String(value || fallback).trim().toUpperCase() === "COMPOUNDING"
    ? "COMPOUNDING"
    : "SIMPLE";
}

function normalizeExecutionMode(value, fallback = config.defaultExecutionMode || "DEMO") {
  return String(value || fallback).trim().toUpperCase() === "REAL"
    ? "REAL"
    : "DEMO";
}

function normalizeProfileId(profileId) {
  const raw = String(profileId || "").trim();
  return raw || DEFAULT_PROFILE_ID;
}

function normalizeAddress(value) {
  const raw = String(value || "").trim().toLowerCase();
  return raw || null;
}

function readLegacyJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, "utf8");
    return raw.trim() ? JSON.parse(raw) : fallback;
  } catch (error) {
    return fallback;
  }
}

function runSql(sql, { json = false } = {}) {
  ensureDir(path.dirname(config.sqlitePath));
  const args = ["-batch", "-cmd", ".timeout 10000"];
  if (json) args.push("-json");
  args.push(config.sqlitePath, sql);

  try {
    return execFileSync("sqlite3", args, {
      encoding: "utf8",
      maxBuffer: 20 * 1024 * 1024,
    });
  } catch (error) {
    const stderr = error.stderr ? String(error.stderr) : error.message;
    throw new Error(`sqlite failure: ${stderr.trim()}`);
  }
}

function execute(sql) {
  runSql(sql);
}

function selectRows(sql) {
  const raw = runSql(sql, { json: true }).trim();
  return raw ? JSON.parse(raw) : [];
}

function selectOne(sql, fallback = null) {
  const rows = selectRows(sql);
  return rows.length ? rows[0] : fallback;
}

function transaction(statements) {
  const sql = [
    "BEGIN IMMEDIATE;",
    ...statements.filter(Boolean),
    "COMMIT;",
  ].join("\n");
  execute(sql);
}

function rowCount(tableName) {
  const row = selectOne(`SELECT COUNT(*) AS count FROM ${tableName};`, { count: 0 });
  return safeInteger(row.count, 0);
}

function tableColumns(tableName) {
  return selectRows(`PRAGMA table_info(${tableName});`).map((row) => String(row.name || ""));
}

function tableExists(tableName) {
  const row = selectOne(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${sqlQuote(tableName)} LIMIT 1;`
  );
  return Boolean(row?.name);
}

function ensureColumn(tableName, columnName, definitionSql) {
  if (tableColumns(tableName).includes(columnName)) return;
  execute(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definitionSql};`);
}

function ensureIndex(indexName, tableName, columnsSql) {
  execute(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${columnsSql});`);
}

function defaultProfileTemplate() {
  return {
    id: DEFAULT_PROFILE_ID,
    telegramUserId: null,
    chatId: config.telegramChatId || null,
    username: "system",
    displayName: "System Profile",
    label: "Default",
    masterWalletAddress: normalizeAddress(config.hyperliquidAccountAddress || config.hyperliquidVaultAddress),
    agentWalletAddress: normalizeAddress(config.hyperliquidAccountAddress),
    encryptedAgentSecret: config.hyperliquidSecretKey ? encryptSecret(config.hyperliquidSecretKey) : null,
    agentSecretFingerprint: config.hyperliquidSecretKey
      ? fingerprintSecret(config.hyperliquidSecretKey)
      : null,
    status: "ACTIVE",
    automationStatus: config.hyperliquidSecretKey ? "APPROVED" : "NOT_CONFIGURED",
    automationEnabled: Boolean(config.hyperliquidSecretKey),
    walletEnabled: true,
    role: "SYSTEM",
  };
}

function profileSettingsDefaults() {
  return {
    autoTradeEnabled: Boolean(config.autoTradeEnabled),
    tradeBalanceTarget: safeNumber(config.defaultTradeBalance, 100),
    tradeLeverage: safeInteger(config.defaultTradeLeverage, 10),
    capitalMode: normalizeCapitalMode(config.defaultCapitalMode),
    executionMode: normalizeExecutionMode(config.defaultExecutionMode),
    baselinePrincipal: safeNumber(config.defaultBaselinePrincipal, 100),
    simpleSlots: Math.max(1, safeInteger(config.defaultSimpleSlots, 1)),
    demoPerpBalance: safeNumber(config.defaultDemoBalance, 100),
    demoSpotBalance: 0,
    demoStartingBalance: safeNumber(config.defaultDemoBalance, 100),
    totalSweptToSpot: 0,
    lastSweepAmount: 0,
    demoTotalSweptToSpot: 0,
    demoLastSweepAmount: 0,
    entryTimeoutMs: safeNumber(config.defaultEntryTimeoutMs, 10 * 60 * 1000),
    lastFillSyncAt: Date.now() - safeNumber(config.fillLookbackMs, 6 * 60 * 60 * 1000),
    lastReconcileAt: null,
    lastReconcileSummary: null,
    strategyCap: Math.max(1, safeInteger(config.strategyCap, 500)),
    strategyRetentionDays: Math.max(1, safeInteger(config.strategyRetentionDays, 7)),
  };
}

function buildStrategySummary(strategy) {
  return {
    id: strategy.id,
    pair: strategy.pair,
    direction: strategy.direction,
    eventTime: strategy.eventTime,
    fileName: strategy.fileName,
    mainSourceTimeframe:
      strategy.mainSourceTimeframe ||
      strategy.timeframe ||
      strategy.fingerprint?.timeframe ||
      strategy.sourceTimeframes?.[0] ||
      null,
    sourceTimeframes: strategy.sourceTimeframes || [],
    savedTimeframes:
      strategy.savedTimeframes ||
      (strategy.allTimeframes ? Object.keys(strategy.allTimeframes) : []),
    supportingTimeframes: strategy.supportingTimeframes || [],
    resultingExpansionPct: strategy.resultingExpansionPct,
  };
}

function ensureSchema() {
  execute(`
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS snapshots (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS profiles (
      id TEXT PRIMARY KEY,
      telegram_user_id TEXT,
      chat_id TEXT,
      username TEXT,
      display_name TEXT,
      label TEXT,
      master_wallet_address TEXT,
      agent_wallet_address TEXT,
      encrypted_agent_secret TEXT,
      agent_secret_fingerprint TEXT,
      status TEXT NOT NULL,
      automation_status TEXT NOT NULL,
      automation_enabled INTEGER NOT NULL DEFAULT 0,
      wallet_enabled INTEGER NOT NULL DEFAULT 1,
      role TEXT NOT NULL DEFAULT 'USER',
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_profiles_telegram_user_id ON profiles(telegram_user_id);
    CREATE INDEX IF NOT EXISTS idx_profiles_master_wallet ON profiles(master_wallet_address);
    CREATE INDEX IF NOT EXISTS idx_profiles_agent_wallet ON profiles(agent_wallet_address);

    CREATE TABLE IF NOT EXISTS profile_settings (
      profile_id TEXT NOT NULL,
      key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (profile_id, key)
    );

    CREATE TABLE IF NOT EXISTS profile_watched_pairs (
      profile_id TEXT NOT NULL,
      pair TEXT NOT NULL,
      coin TEXT,
      added_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (profile_id, pair)
    );

    CREATE INDEX IF NOT EXISTS idx_profile_watched_pairs_profile_id
      ON profile_watched_pairs(profile_id);

    CREATE TABLE IF NOT EXISTS profile_active_signals (
      profile_id TEXT NOT NULL,
      signal_key TEXT NOT NULL,
      pair TEXT NOT NULL,
      side TEXT NOT NULL,
      base_timeframe TEXT,
      message_id INTEGER,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (profile_id, signal_key)
    );

    CREATE TABLE IF NOT EXISTS trades (
      id TEXT PRIMARY KEY,
      profile_id TEXT,
      pair TEXT NOT NULL,
      coin TEXT,
      side TEXT NOT NULL,
      status TEXT NOT NULL,
      phase TEXT,
      signal_key TEXT,
      signal_message_id INTEGER,
      capital_mode TEXT,
      execution_mode TEXT,
      strategy_bucket TEXT,
      user_profile_id TEXT,
      idempotency_key TEXT,
      opened_at TEXT,
      filled_at TEXT,
      closed_at TEXT,
      gross_pnl REAL DEFAULT 0,
      net_pnl REAL DEFAULT 0,
      fees REAL DEFAULT 0,
      funding_impact REAL DEFAULT 0,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
    CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair);
    CREATE INDEX IF NOT EXISTS idx_trades_closed_at ON trades(closed_at);

    CREATE TABLE IF NOT EXISTS strategies (
      id TEXT PRIMARY KEY,
      pair TEXT NOT NULL,
      direction TEXT NOT NULL,
      event_time TEXT,
      main_source_timeframe TEXT,
      file_name TEXT,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_strategies_pair ON strategies(pair);
    CREATE INDEX IF NOT EXISTS idx_strategies_event_time ON strategies(event_time);

    CREATE TABLE IF NOT EXISTS profile_pair_state (
      profile_id TEXT NOT NULL,
      pair TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (profile_id, pair)
    );

    CREATE TABLE IF NOT EXISTS profile_processed_fills (
      profile_id TEXT NOT NULL,
      fill_key TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      processed_at TEXT NOT NULL,
      PRIMARY KEY (profile_id, fill_key)
    );

    CREATE TABLE IF NOT EXISTS automation_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      profile_id TEXT NOT NULL,
      telegram_user_id TEXT,
      username TEXT,
      display_name TEXT,
      master_wallet_address TEXT,
      agent_wallet_address TEXT,
      encrypted_agent_secret TEXT,
      agent_secret_fingerprint TEXT,
      requested_defaults_json TEXT,
      status TEXT NOT NULL,
      rejection_reason TEXT,
      reviewed_by TEXT,
      requested_at TEXT NOT NULL,
      reviewed_at TEXT,
      payload_json TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_automation_requests_status
      ON automation_requests(status, requested_at DESC);

    CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      profile_id TEXT,
      actor_user_id TEXT,
      action TEXT NOT NULL,
      reason TEXT,
      old_value_json TEXT,
      new_value_json TEXT,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reconciliation_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      profile_id TEXT NOT NULL,
      summary_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);

  ensureColumn("trades", "profile_id", "TEXT");
  ensureColumn("trades", "capital_mode", "TEXT");
  ensureColumn("trades", "execution_mode", "TEXT");
  ensureColumn("trades", "strategy_bucket", "TEXT");
  ensureColumn("trades", "user_profile_id", "TEXT");
  ensureColumn("trades", "idempotency_key", "TEXT");
  ensureColumn("trades", "gross_pnl", "REAL DEFAULT 0");
  ensureColumn("trades", "net_pnl", "REAL DEFAULT 0");
  ensureColumn("trades", "fees", "REAL DEFAULT 0");
  ensureColumn("trades", "funding_impact", "REAL DEFAULT 0");

  ensureIndex("idx_trades_profile_id", "trades", "profile_id");
  ensureIndex("idx_trades_modes", "trades", "execution_mode, capital_mode");
}

function setSetting(key, value) {
  const updatedAt = nowIso();
  execute(`
    INSERT INTO settings (key, value_json, updated_at)
    VALUES (${sqlQuote(key)}, ${jsonValue(value)}, ${sqlQuote(updatedAt)})
    ON CONFLICT(key) DO UPDATE SET
      value_json = excluded.value_json,
      updated_at = excluded.updated_at;
  `);
  return value;
}

function getSetting(key, fallback = null) {
  const row = selectOne(
    `SELECT value_json FROM settings WHERE key = ${sqlQuote(key)} LIMIT 1;`
  );
  if (!row) return fallback;
  return parseJsonSafe(row.value_json, fallback);
}

function setSnapshot(key, value) {
  const updatedAt = nowIso();
  execute(`
    INSERT INTO snapshots (key, value_json, updated_at)
    VALUES (${sqlQuote(key)}, ${jsonValue(value)}, ${sqlQuote(updatedAt)})
    ON CONFLICT(key) DO UPDATE SET
      value_json = excluded.value_json,
      updated_at = excluded.updated_at;
  `);
  return value;
}

function getSnapshot(key, fallback = null) {
  const row = selectOne(
    `SELECT value_json FROM snapshots WHERE key = ${sqlQuote(key)} LIMIT 1;`
  );
  if (!row) return fallback;
  return parseJsonSafe(row.value_json, fallback);
}

function seedDefaultSettings() {
  if (getSetting(SETTINGS_KEYS.strategyCap) == null) {
    setSetting(SETTINGS_KEYS.strategyCap, Math.max(1, safeInteger(config.strategyCap, 500)));
  }
  if (getSetting(SETTINGS_KEYS.strategyRetentionDays) == null) {
    setSetting(
      SETTINGS_KEYS.strategyRetentionDays,
      Math.max(1, safeInteger(config.strategyRetentionDays, 7))
    );
  }
}

function getAllowedPairs() {
  return uniqueUpper(defaultPairs);
}

function filterToAllowedPairs(pairs) {
  return uniqueUpper(pairs);
}

function upsertProfile(profile) {
  const createdAt = profile.createdAt || nowIso();
  const updatedAt = nowIso();
  execute(`
    INSERT INTO profiles (
      id, telegram_user_id, chat_id, username, display_name, label,
      master_wallet_address, agent_wallet_address, encrypted_agent_secret,
      agent_secret_fingerprint, status, automation_status, automation_enabled,
      wallet_enabled, role, last_error, created_at, updated_at
    ) VALUES (
      ${sqlQuote(normalizeProfileId(profile.id))},
      ${profile.telegramUserId ? sqlQuote(String(profile.telegramUserId)) : "NULL"},
      ${profile.chatId ? sqlQuote(String(profile.chatId)) : "NULL"},
      ${profile.username ? sqlQuote(profile.username) : "NULL"},
      ${profile.displayName ? sqlQuote(profile.displayName) : "NULL"},
      ${profile.label ? sqlQuote(profile.label) : "NULL"},
      ${profile.masterWalletAddress ? sqlQuote(normalizeAddress(profile.masterWalletAddress)) : "NULL"},
      ${profile.agentWalletAddress ? sqlQuote(normalizeAddress(profile.agentWalletAddress)) : "NULL"},
      ${profile.encryptedAgentSecret ? sqlQuote(profile.encryptedAgentSecret) : "NULL"},
      ${profile.agentSecretFingerprint ? sqlQuote(profile.agentSecretFingerprint) : "NULL"},
      ${sqlQuote(profile.status || "ACTIVE")},
      ${sqlQuote(profile.automationStatus || "NOT_CONFIGURED")},
      ${profile.automationEnabled ? 1 : 0},
      ${profile.walletEnabled === false ? 0 : 1},
      ${sqlQuote(profile.role || "USER")},
      ${profile.lastError ? sqlQuote(profile.lastError) : "NULL"},
      ${sqlQuote(createdAt)},
      ${sqlQuote(updatedAt)}
    )
    ON CONFLICT(id) DO UPDATE SET
      telegram_user_id = excluded.telegram_user_id,
      chat_id = excluded.chat_id,
      username = excluded.username,
      display_name = excluded.display_name,
      label = excluded.label,
      master_wallet_address = excluded.master_wallet_address,
      agent_wallet_address = excluded.agent_wallet_address,
      encrypted_agent_secret = excluded.encrypted_agent_secret,
      agent_secret_fingerprint = excluded.agent_secret_fingerprint,
      status = excluded.status,
      automation_status = excluded.automation_status,
      automation_enabled = excluded.automation_enabled,
      wallet_enabled = excluded.wallet_enabled,
      role = excluded.role,
      last_error = excluded.last_error,
      updated_at = excluded.updated_at;
  `);
  return getProfileById(profile.id, { includeSecret: true });
}

function hydrateProfile(row, { includeSecret = false } = {}) {
  if (!row) return null;
  const profile = {
    id: normalizeProfileId(row.id),
    telegramUserId: row.telegram_user_id || null,
    chatId: row.chat_id || null,
    username: row.username || null,
    displayName: row.display_name || null,
    label: row.label || null,
    masterWalletAddress: row.master_wallet_address || null,
    agentWalletAddress: row.agent_wallet_address || null,
    encryptedAgentSecret: includeSecret ? row.encrypted_agent_secret || null : null,
    agentSecretFingerprint: row.agent_secret_fingerprint || null,
    status: row.status || "ACTIVE",
    automationStatus: row.automation_status || "NOT_CONFIGURED",
    automationEnabled: Boolean(Number(row.automation_enabled || 0)),
    walletEnabled: !row.wallet_enabled || Boolean(Number(row.wallet_enabled)),
    role: row.role || "USER",
    lastError: row.last_error || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };

  if (includeSecret && profile.encryptedAgentSecret) {
    try {
      profile.agentSecret = decryptSecret(profile.encryptedAgentSecret);
    } catch (error) {
      profile.agentSecret = "";
    }
  }

  return profile;
}

function getProfileById(profileId, options = {}) {
  const row = selectOne(
    `SELECT * FROM profiles WHERE id = ${sqlQuote(normalizeProfileId(profileId))} LIMIT 1;`
  );
  return hydrateProfile(row, options);
}

function findProfileByTelegramUserId(userId, options = {}) {
  const row = selectOne(
    `SELECT * FROM profiles WHERE telegram_user_id = ${sqlQuote(String(userId || ""))} LIMIT 1;`
  );
  return hydrateProfile(row, options);
}

function findProfile(ref, options = {}) {
  const target = String(ref || "").trim();
  if (!target) return null;

  const byId = getProfileById(target, options);
  if (byId) return byId;

  const row = selectOne(`
    SELECT *
    FROM profiles
    WHERE telegram_user_id = ${sqlQuote(target)}
       OR lower(master_wallet_address) = ${sqlQuote(target.toLowerCase())}
       OR lower(agent_wallet_address) = ${sqlQuote(target.toLowerCase())}
    LIMIT 1;
  `);
  return hydrateProfile(row, options);
}

function listProfiles(options = {}) {
  const includeDisabled = options.includeDisabled !== false;
  const rows = selectRows(`
    SELECT *
    FROM profiles
    ${includeDisabled ? "" : "WHERE status = 'ACTIVE' AND wallet_enabled = 1"}
    ORDER BY updated_at DESC;
  `);
  return rows.map((row) => hydrateProfile(row, options)).filter(Boolean);
}

function ensureProfileSettings(profileId) {
  const defaults = profileSettingsDefaults();
  for (const [key, value] of Object.entries(defaults)) {
    if (getProfileSetting(profileId, key, null) == null) {
      setProfileSetting(profileId, key, value);
    }
  }
}

function ensureDefaultProfile() {
  const existing = getProfileById(DEFAULT_PROFILE_ID, { includeSecret: true });
  const defaults = defaultProfileTemplate();
  if (!existing) {
    upsertProfile(defaults);
  } else {
    upsertProfile({
      ...existing,
      chatId: existing.chatId || defaults.chatId,
      username: existing.username || defaults.username,
      displayName: existing.displayName || defaults.displayName,
      label: existing.label || defaults.label,
      masterWalletAddress: existing.masterWalletAddress || defaults.masterWalletAddress,
      agentWalletAddress: existing.agentWalletAddress || defaults.agentWalletAddress,
      encryptedAgentSecret: existing.encryptedAgentSecret || defaults.encryptedAgentSecret,
      agentSecretFingerprint: existing.agentSecretFingerprint || defaults.agentSecretFingerprint,
      automationStatus: existing.automationStatus || defaults.automationStatus,
      automationEnabled: existing.automationEnabled || defaults.automationEnabled,
      role: existing.role || defaults.role,
    });
  }
  ensureProfileSettings(DEFAULT_PROFILE_ID);
  if (config.hyperliquidSecretKey) {
    setProfileSetting(
      DEFAULT_PROFILE_ID,
      PROFILE_SETTINGS_KEYS.executionMode,
      "REAL"
    );
  }
  if (!getWatchedPairs(DEFAULT_PROFILE_ID).length) {
    saveWatchedPairs(DEFAULT_PROFILE_ID, getAllowedPairs());
  }
}

function getOrCreateProfileFromTelegram(user = {}, chat = {}) {
  const telegramUserId = user?.id ? String(user.id) : null;
  if (!telegramUserId) {
    ensureDefaultProfile();
    return getProfileById(DEFAULT_PROFILE_ID);
  }

  const existing = findProfileByTelegramUserId(telegramUserId, { includeSecret: true });
  const displayName = [user.first_name, user.last_name].filter(Boolean).join(" ").trim() || user.username || `User ${telegramUserId}`;
  const profileId = existing?.id || `tg:${telegramUserId}`;

  const profile = upsertProfile({
    ...(existing || {}),
    id: profileId,
    telegramUserId,
    chatId: chat?.id ? String(chat.id) : existing?.chatId || null,
    username: user.username || existing?.username || null,
    displayName,
    label: existing?.label || displayName,
    status: existing?.status || "ACTIVE",
    automationStatus: existing?.automationStatus || "NOT_CONFIGURED",
    automationEnabled: existing?.automationEnabled || false,
    walletEnabled: existing?.walletEnabled !== false,
    role: existing?.role || "USER",
    encryptedAgentSecret: existing?.encryptedAgentSecret || null,
    agentSecretFingerprint: existing?.agentSecretFingerprint || null,
    masterWalletAddress: existing?.masterWalletAddress || null,
    agentWalletAddress: existing?.agentWalletAddress || null,
  });
  ensureProfileSettings(profile.id);
  if (!getWatchedPairs(profile.id).length) {
    saveWatchedPairs(profile.id, getAllowedPairs());
  }
  return profile;
}

function getProfileSetting(profileId, key, fallback = null) {
  const row = selectOne(`
    SELECT value_json
    FROM profile_settings
    WHERE profile_id = ${sqlQuote(normalizeProfileId(profileId))}
      AND key = ${sqlQuote(key)}
    LIMIT 1;
  `);
  if (!row) return fallback;
  return parseJsonSafe(row.value_json, fallback);
}

function setProfileSetting(profileId, key, value) {
  const updatedAt = nowIso();
  execute(`
    INSERT INTO profile_settings (profile_id, key, value_json, updated_at)
    VALUES (
      ${sqlQuote(normalizeProfileId(profileId))},
      ${sqlQuote(key)},
      ${jsonValue(value)},
      ${sqlQuote(updatedAt)}
    )
    ON CONFLICT(profile_id, key) DO UPDATE SET
      value_json = excluded.value_json,
      updated_at = excluded.updated_at;
  `);
  return value;
}

function updateProfileSettings(profileId, values = {}) {
  const normalizedId = normalizeProfileId(profileId);
  Object.entries(values).forEach(([key, value]) => setProfileSetting(normalizedId, key, value));
  return getRuntimeSettings(normalizedId);
}

function getRuntimeSettings(profileId = DEFAULT_PROFILE_ID) {
  const normalizedId = normalizeProfileId(profileId);
  ensureProfileSettings(normalizedId);
  const defaults = profileSettingsDefaults();
  const runtime = {};

  for (const [key, defaultValue] of Object.entries(defaults)) {
    runtime[key] = getProfileSetting(normalizedId, key, defaultValue);
  }

  runtime.tradeBalanceTarget = safeNumber(runtime.tradeBalanceTarget, defaults.tradeBalanceTarget);
  runtime.tradeLeverage = Math.max(1, safeInteger(runtime.tradeLeverage, defaults.tradeLeverage));
  runtime.capitalMode = normalizeCapitalMode(runtime.capitalMode, defaults.capitalMode);
  runtime.executionMode = normalizeExecutionMode(runtime.executionMode, defaults.executionMode);
  runtime.baselinePrincipal = safeNumber(runtime.baselinePrincipal, defaults.baselinePrincipal);
  runtime.simpleSlots = Math.max(1, safeInteger(runtime.simpleSlots, defaults.simpleSlots));
  runtime.demoPerpBalance = safeNumber(runtime.demoPerpBalance, defaults.demoPerpBalance);
  runtime.demoSpotBalance = safeNumber(runtime.demoSpotBalance, defaults.demoSpotBalance);
  runtime.demoStartingBalance = safeNumber(runtime.demoStartingBalance, defaults.demoStartingBalance);
  runtime.totalSweptToSpot = safeNumber(runtime.totalSweptToSpot, 0);
  runtime.lastSweepAmount = safeNumber(runtime.lastSweepAmount, 0);
  runtime.demoTotalSweptToSpot = safeNumber(runtime.demoTotalSweptToSpot, 0);
  runtime.demoLastSweepAmount = safeNumber(runtime.demoLastSweepAmount, 0);
  runtime.entryTimeoutMs = safeNumber(runtime.entryTimeoutMs, defaults.entryTimeoutMs);
  runtime.lastFillSyncAt = safeNumber(runtime.lastFillSyncAt, defaults.lastFillSyncAt);
  runtime.strategyCap = Math.max(1, safeInteger(runtime.strategyCap, defaults.strategyCap));
  runtime.strategyRetentionDays = Math.max(
    1,
    safeInteger(runtime.strategyRetentionDays, defaults.strategyRetentionDays)
  );
  runtime.demoBalance = runtime.demoPerpBalance;
  return runtime;
}

function setAutoTradeEnabled(profileOrValue, maybeValue) {
  if (maybeValue === undefined) {
    return setProfileSetting(DEFAULT_PROFILE_ID, PROFILE_SETTINGS_KEYS.autoTradeEnabled, Boolean(profileOrValue));
  }
  return setProfileSetting(normalizeProfileId(profileOrValue), PROFILE_SETTINGS_KEYS.autoTradeEnabled, Boolean(maybeValue));
}

function setTradeBalanceTarget(profileOrValue, maybeValue) {
  if (maybeValue === undefined) {
    return setProfileSetting(DEFAULT_PROFILE_ID, PROFILE_SETTINGS_KEYS.tradeBalanceTarget, safeNumber(profileOrValue, 0));
  }
  return setProfileSetting(normalizeProfileId(profileOrValue), PROFILE_SETTINGS_KEYS.tradeBalanceTarget, safeNumber(maybeValue, 0));
}

function setTradeLeverage(profileOrValue, maybeValue) {
  if (maybeValue === undefined) {
    return setProfileSetting(DEFAULT_PROFILE_ID, PROFILE_SETTINGS_KEYS.tradeLeverage, Math.max(1, safeInteger(profileOrValue, 1)));
  }
  return setProfileSetting(normalizeProfileId(profileOrValue), PROFILE_SETTINGS_KEYS.tradeLeverage, Math.max(1, safeInteger(maybeValue, 1)));
}

function setLastFillSyncAt(profileOrValue, maybeValue) {
  if (maybeValue === undefined) {
    return setProfileSetting(DEFAULT_PROFILE_ID, PROFILE_SETTINGS_KEYS.lastFillSyncAt, safeNumber(profileOrValue, Date.now()));
  }
  return setProfileSetting(normalizeProfileId(profileOrValue), PROFILE_SETTINGS_KEYS.lastFillSyncAt, safeNumber(maybeValue, Date.now()));
}

function setCapitalMode(profileId, capitalMode) {
  return setProfileSetting(
    normalizeProfileId(profileId),
    PROFILE_SETTINGS_KEYS.capitalMode,
    normalizeCapitalMode(capitalMode)
  );
}

function setExecutionMode(profileId, executionMode) {
  return setProfileSetting(
    normalizeProfileId(profileId),
    PROFILE_SETTINGS_KEYS.executionMode,
    normalizeExecutionMode(executionMode)
  );
}

function setBaselinePrincipal(profileId, value) {
  return setProfileSetting(
    normalizeProfileId(profileId),
    PROFILE_SETTINGS_KEYS.baselinePrincipal,
    safeNumber(value, 0)
  );
}

function setSimpleSlots(profileId, value) {
  return setProfileSetting(
    normalizeProfileId(profileId),
    PROFILE_SETTINGS_KEYS.simpleSlots,
    Math.max(1, safeInteger(value, 1))
  );
}

function setDemoBalances(profileId, { perpBalance, spotBalance, startingBalance }) {
  const updates = {};
  if (perpBalance != null) updates[PROFILE_SETTINGS_KEYS.demoPerpBalance] = safeNumber(perpBalance, 0);
  if (spotBalance != null) updates[PROFILE_SETTINGS_KEYS.demoSpotBalance] = safeNumber(spotBalance, 0);
  if (startingBalance != null) {
    updates[PROFILE_SETTINGS_KEYS.demoStartingBalance] = safeNumber(startingBalance, 0);
  }
  return updateProfileSettings(normalizeProfileId(profileId), updates);
}

function setSweepStats(profileId, { totalSweptToSpot, lastSweepAmount, demoTotalSweptToSpot, demoLastSweepAmount }) {
  const updates = {};
  if (totalSweptToSpot != null) {
    updates[PROFILE_SETTINGS_KEYS.totalSweptToSpot] = safeNumber(totalSweptToSpot, 0);
  }
  if (lastSweepAmount != null) {
    updates[PROFILE_SETTINGS_KEYS.lastSweepAmount] = safeNumber(lastSweepAmount, 0);
  }
  if (demoTotalSweptToSpot != null) {
    updates[PROFILE_SETTINGS_KEYS.demoTotalSweptToSpot] = safeNumber(demoTotalSweptToSpot, 0);
  }
  if (demoLastSweepAmount != null) {
    updates[PROFILE_SETTINGS_KEYS.demoLastSweepAmount] = safeNumber(demoLastSweepAmount, 0);
  }
  return updateProfileSettings(normalizeProfileId(profileId), updates);
}

function setLastReconcile(profileId, summary) {
  const normalizedId = normalizeProfileId(profileId);
  updateProfileSettings(normalizedId, {
    [PROFILE_SETTINGS_KEYS.lastReconcileAt]: nowIso(),
    [PROFILE_SETTINGS_KEYS.lastReconcileSummary]: summary,
  });
  return summary;
}

function getWatchedPairs(profileId = DEFAULT_PROFILE_ID) {
  const normalizedId = normalizeProfileId(profileId);
  const rows = selectRows(`
    SELECT pair
    FROM profile_watched_pairs
    WHERE profile_id = ${sqlQuote(normalizedId)}
    ORDER BY pair ASC;
  `);
  const pairs = rows.map((row) => String(row.pair || "").toUpperCase()).filter(Boolean);
  return pairs;
}

function listWatchedPairsByProfile() {
  const rows = selectRows(`
    SELECT profile_id, pair, coin
    FROM profile_watched_pairs
    ORDER BY profile_id ASC, pair ASC;
  `);
  return rows.map((row) => ({
    profileId: row.profile_id,
    pair: String(row.pair || "").toUpperCase(),
    coin: String(row.coin || "").toUpperCase(),
  }));
}

function saveWatchedPairs(profileOrPairs, maybePairs) {
  const profileId =
    maybePairs === undefined ? DEFAULT_PROFILE_ID : normalizeProfileId(profileOrPairs);
  const pairs = maybePairs === undefined ? profileOrPairs : maybePairs;
  const normalized = uniqueUpper(pairs).sort();
  const now = nowIso();

  transaction([
    `DELETE FROM profile_watched_pairs WHERE profile_id = ${sqlQuote(profileId)};`,
    ...normalized.map((pair) => `
      INSERT INTO profile_watched_pairs (profile_id, pair, coin, added_at, updated_at)
      VALUES (
        ${sqlQuote(profileId)},
        ${sqlQuote(pair)},
        ${sqlQuote(pair.replace(/USDT$/, ""))},
        ${sqlQuote(now)},
        ${sqlQuote(now)}
      );
    `),
  ]);
  return normalized;
}

function addWatchedPair(profileId, pair) {
  const current = getWatchedPairs(profileId);
  const normalized = String(pair || "").trim().toUpperCase();
  if (normalized && !current.includes(normalized)) {
    current.push(normalized);
    saveWatchedPairs(profileId, current);
  }
  return getWatchedPairs(profileId);
}

function removeWatchedPair(profileId, pair) {
  const normalized = String(pair || "").trim().toUpperCase();
  const next = getWatchedPairs(profileId).filter((item) => item !== normalized);
  saveWatchedPairs(profileId, next);
  return next;
}

function getActiveSignals(profileId = DEFAULT_PROFILE_ID) {
  const normalizedId = normalizeProfileId(profileId);
  const rows = selectRows(`
    SELECT signal_key, payload_json
    FROM profile_active_signals
    WHERE profile_id = ${sqlQuote(normalizedId)};
  `);
  return rows.reduce((accumulator, row) => {
    accumulator[row.signal_key] = parseJsonSafe(row.payload_json, {});
    return accumulator;
  }, {});
}

function saveActiveSignals(profileOrSignals, maybeSignals) {
  const profileId =
    maybeSignals === undefined ? DEFAULT_PROFILE_ID : normalizeProfileId(profileOrSignals);
  const activeSignals = maybeSignals === undefined ? profileOrSignals : maybeSignals;
  const now = nowIso();
  const entries = Object.entries(activeSignals || {});
  transaction([
    `DELETE FROM profile_active_signals WHERE profile_id = ${sqlQuote(profileId)};`,
    ...entries.map(([signalKey, payload]) => `
      INSERT INTO profile_active_signals (
        profile_id, signal_key, pair, side, base_timeframe, message_id,
        payload_json, created_at, updated_at
      ) VALUES (
        ${sqlQuote(profileId)},
        ${sqlQuote(signalKey)},
        ${sqlQuote(payload.pair || "")},
        ${sqlQuote(payload.side || "")},
        ${sqlQuote(payload.baseTimeframe || payload.baseTf || "")},
        ${
          payload.messageId != null && Number.isFinite(Number(payload.messageId))
            ? Number(payload.messageId)
            : "NULL"
        },
        ${jsonValue(payload)},
        ${sqlQuote(payload.createdAt || now)},
        ${sqlQuote(payload.updatedAt || now)}
      );
    `),
  ]);
  return activeSignals;
}

function deleteActiveSignal(profileId, signalKey) {
  execute(`
    DELETE FROM profile_active_signals
    WHERE profile_id = ${sqlQuote(normalizeProfileId(profileId))}
      AND signal_key = ${sqlQuote(signalKey)};
  `);
}

function buildTradePayload(trade) {
  const normalizedProfileId = normalizeProfileId(trade.profileId || trade.userProfileId || DEFAULT_PROFILE_ID);
  return {
    ...trade,
    profileId: normalizedProfileId,
    userProfileId: normalizedProfileId,
    capitalMode: normalizeCapitalMode(trade.capitalMode),
    executionMode: normalizeExecutionMode(trade.executionMode),
    grossPnl: safeNumber(trade.grossPnl ?? trade.realizedPnl, 0),
    netPnl: safeNumber(trade.netPnl ?? trade.realizedPnl, 0),
    fees: safeNumber(trade.fees ?? (safeNumber(trade.entryFee, 0) + safeNumber(trade.exitFee, 0)), 0),
    fundingImpact: safeNumber(trade.fundingImpact, 0),
    entryFee: safeNumber(trade.entryFee, 0),
    exitFee: safeNumber(trade.exitFee, 0),
    realizedPnl: safeNumber(trade.realizedPnl, 0),
    strategyBucket: trade.strategyBucket || trade.strategyUsed || null,
  };
}

function saveTrade(trade) {
  if (!trade?.id) {
    throw new Error("Trade id is required");
  }

  const payload = buildTradePayload(trade);
  const createdAt = payload.createdAt || payload.openedAt || nowIso();
  const updatedAt = nowIso();

  execute(`
    INSERT INTO trades (
      id, profile_id, pair, coin, side, status, phase, signal_key, signal_message_id,
      capital_mode, execution_mode, strategy_bucket, user_profile_id, idempotency_key,
      opened_at, filled_at, closed_at, gross_pnl, net_pnl, fees, funding_impact,
      payload_json, created_at, updated_at
    ) VALUES (
      ${sqlQuote(payload.id)},
      ${sqlQuote(payload.profileId)},
      ${sqlQuote(payload.pair || "")},
      ${sqlQuote(payload.coin || "")},
      ${sqlQuote(payload.side || "")},
      ${sqlQuote(payload.status || "UNKNOWN")},
      ${sqlQuote(payload.phase || "")},
      ${sqlQuote(payload.signalKey || "")},
      ${
        payload.signalMessageId != null && Number.isFinite(Number(payload.signalMessageId))
          ? Number(payload.signalMessageId)
          : "NULL"
      },
      ${sqlQuote(payload.capitalMode || "SIMPLE")},
      ${sqlQuote(payload.executionMode || "DEMO")},
      ${payload.strategyBucket ? sqlQuote(payload.strategyBucket) : "NULL"},
      ${sqlQuote(payload.userProfileId || payload.profileId)},
      ${payload.idempotencyKey ? sqlQuote(payload.idempotencyKey) : "NULL"},
      ${payload.openedAt ? sqlQuote(payload.openedAt) : "NULL"},
      ${payload.filledAt ? sqlQuote(payload.filledAt) : "NULL"},
      ${payload.closedAt ? sqlQuote(payload.closedAt) : "NULL"},
      ${safeNumber(payload.grossPnl, 0)},
      ${safeNumber(payload.netPnl, 0)},
      ${safeNumber(payload.fees, 0)},
      ${safeNumber(payload.fundingImpact, 0)},
      ${jsonValue({ ...payload, createdAt, updatedAt })},
      ${sqlQuote(createdAt)},
      ${sqlQuote(updatedAt)}
    )
    ON CONFLICT(id) DO UPDATE SET
      profile_id = excluded.profile_id,
      pair = excluded.pair,
      coin = excluded.coin,
      side = excluded.side,
      status = excluded.status,
      phase = excluded.phase,
      signal_key = excluded.signal_key,
      signal_message_id = excluded.signal_message_id,
      capital_mode = excluded.capital_mode,
      execution_mode = excluded.execution_mode,
      strategy_bucket = excluded.strategy_bucket,
      user_profile_id = excluded.user_profile_id,
      idempotency_key = excluded.idempotency_key,
      opened_at = excluded.opened_at,
      filled_at = excluded.filled_at,
      closed_at = excluded.closed_at,
      gross_pnl = excluded.gross_pnl,
      net_pnl = excluded.net_pnl,
      fees = excluded.fees,
      funding_impact = excluded.funding_impact,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at;
  `);

  return getTradeById(payload.id);
}

function hydrateRows(rows) {
  return rows.map((row) => parseJsonSafe(row.payload_json, {})).filter(Boolean);
}

function getTradeById(id) {
  const row = selectOne(`SELECT payload_json FROM trades WHERE id = ${sqlQuote(id)} LIMIT 1;`);
  return row ? parseJsonSafe(row.payload_json, null) : null;
}

function profileFilterSql(profileId) {
  return profileId ? `AND profile_id = ${sqlQuote(normalizeProfileId(profileId))}` : "";
}

function listOpenTrades(profileId = null) {
  return hydrateRows(
    selectRows(`
      SELECT payload_json
      FROM trades
      WHERE status NOT IN (${TERMINAL_TRADE_STATUSES.map(sqlQuote).join(", ")})
      ${profileFilterSql(profileId)}
      ORDER BY COALESCE(filled_at, opened_at, created_at) DESC, updated_at DESC;
    `)
  );
}

function listClosedTrades(limit = 0) {
  const limitSql = limit > 0 ? `LIMIT ${safeInteger(limit, 0)}` : "";
  return hydrateRows(
    selectRows(`
      SELECT payload_json
      FROM trades
      WHERE status IN (${TERMINAL_TRADE_STATUSES.map(sqlQuote).join(", ")})
      ORDER BY COALESCE(closed_at, updated_at) DESC
      ${limitSql};
    `)
  );
}

function listClosedTradesByProfile(profileId, limit = 0) {
  const limitSql = limit > 0 ? `LIMIT ${safeInteger(limit, 0)}` : "";
  return hydrateRows(
    selectRows(`
      SELECT payload_json
      FROM trades
      WHERE status IN (${TERMINAL_TRADE_STATUSES.map(sqlQuote).join(", ")})
        AND profile_id = ${sqlQuote(normalizeProfileId(profileId))}
      ORDER BY COALESCE(closed_at, updated_at) DESC
      ${limitSql};
    `)
  );
}

function listAllTrades(profileId = null) {
  return hydrateRows(
    selectRows(`
      SELECT payload_json
      FROM trades
      WHERE 1 = 1
      ${profileFilterSql(profileId)}
      ORDER BY COALESCE(closed_at, filled_at, opened_at, created_at) DESC, updated_at DESC;
    `)
  );
}

function findOpenTradeByPair(profileOrPair, maybePair) {
  if (maybePair === undefined) {
    const pair = String(profileOrPair || "").toUpperCase();
    return listOpenTrades().find((trade) => trade.pair === pair) || null;
  }
  const profileId = normalizeProfileId(profileOrPair);
  const pair = String(maybePair || "").toUpperCase();
  return listOpenTrades(profileId).find((trade) => trade.pair === pair) || null;
}

function getPairState(profileOrPair, maybePair) {
  const profileId = maybePair === undefined ? DEFAULT_PROFILE_ID : normalizeProfileId(profileOrPair);
  const pair = maybePair === undefined ? profileOrPair : maybePair;
  const row = selectOne(`
    SELECT payload_json
    FROM profile_pair_state
    WHERE profile_id = ${sqlQuote(profileId)}
      AND pair = ${sqlQuote(String(pair || "").toUpperCase())}
    LIMIT 1;
  `);
  return row ? parseJsonSafe(row.payload_json, null) : null;
}

function savePairState(profileOrPair, maybePair, maybeValue) {
  const profileId = maybeValue === undefined ? DEFAULT_PROFILE_ID : normalizeProfileId(profileOrPair);
  const pair = maybeValue === undefined ? profileOrPair : maybePair;
  const value = maybeValue === undefined ? maybePair : maybeValue;
  const targetPair = String(pair || "").toUpperCase();
  const updatedAt = nowIso();
  execute(`
    INSERT INTO profile_pair_state (profile_id, pair, payload_json, updated_at)
    VALUES (
      ${sqlQuote(profileId)},
      ${sqlQuote(targetPair)},
      ${jsonValue(value)},
      ${sqlQuote(updatedAt)}
    )
    ON CONFLICT(profile_id, pair) DO UPDATE SET
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at;
  `);
  return getPairState(profileId, targetPair);
}

function listPairStates(profileId = null) {
  return selectRows(`
    SELECT payload_json
    FROM profile_pair_state
    ${profileId ? `WHERE profile_id = ${sqlQuote(normalizeProfileId(profileId))}` : ""}
    ORDER BY pair ASC;
  `).map((row) => parseJsonSafe(row.payload_json, {}));
}

function saveStrategy(strategy) {
  if (!strategy?.id) {
    throw new Error("Strategy id is required");
  }

  const summary = buildStrategySummary(strategy);
  const createdAt = strategy.detectedAt || strategy.createdAt || nowIso();
  const updatedAt = nowIso();

  execute(`
    INSERT INTO strategies (
      id, pair, direction, event_time, main_source_timeframe, file_name,
      payload_json, created_at, updated_at
    ) VALUES (
      ${sqlQuote(summary.id)},
      ${sqlQuote(summary.pair || "")},
      ${sqlQuote(summary.direction || "")},
      ${summary.eventTime ? sqlQuote(summary.eventTime) : "NULL"},
      ${summary.mainSourceTimeframe ? sqlQuote(summary.mainSourceTimeframe) : "NULL"},
      ${summary.fileName ? sqlQuote(summary.fileName) : "NULL"},
      ${jsonValue(strategy)},
      ${sqlQuote(createdAt)},
      ${sqlQuote(updatedAt)}
    )
    ON CONFLICT(id) DO UPDATE SET
      pair = excluded.pair,
      direction = excluded.direction,
      event_time = excluded.event_time,
      main_source_timeframe = excluded.main_source_timeframe,
      file_name = excluded.file_name,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at;
  `);

  return strategy;
}

function loadStrategies() {
  return hydrateRows(
    selectRows(`
      SELECT payload_json
      FROM strategies
      ORDER BY COALESCE(event_time, updated_at) DESC;
    `)
  );
}

function loadStrategiesIndex() {
  return loadStrategies().map(buildStrategySummary);
}

function getStrategyByPair(pair) {
  const target = String(pair || "").trim().toUpperCase();
  return loadStrategies().filter((strategy) => strategy.pair === target);
}

function replaceStrategies(strategies) {
  transaction([
    "DELETE FROM strategies;",
    ...(strategies || []).map((strategy) => {
      const summary = buildStrategySummary(strategy);
      const createdAt = strategy.detectedAt || strategy.createdAt || nowIso();
      const updatedAt = nowIso();
      return `
        INSERT INTO strategies (
          id, pair, direction, event_time, main_source_timeframe, file_name,
          payload_json, created_at, updated_at
        ) VALUES (
          ${sqlQuote(summary.id)},
          ${sqlQuote(summary.pair || "")},
          ${sqlQuote(summary.direction || "")},
          ${summary.eventTime ? sqlQuote(summary.eventTime) : "NULL"},
          ${summary.mainSourceTimeframe ? sqlQuote(summary.mainSourceTimeframe) : "NULL"},
          ${summary.fileName ? sqlQuote(summary.fileName) : "NULL"},
          ${jsonValue(strategy)},
          ${sqlQuote(createdAt)},
          ${sqlQuote(updatedAt)}
        );
      `;
    }),
  ]);
  return loadStrategies();
}

function deleteStrategiesByIds(ids = []) {
  const normalized = [...new Set(ids.map((id) => String(id).trim()).filter(Boolean))];
  if (!normalized.length) return 0;
  execute(`DELETE FROM strategies WHERE id IN (${normalized.map(sqlQuote).join(", ")});`);
  return normalized.length;
}

function markProcessedFill(profileOrFillKey, maybeFillKey, maybePayload = {}) {
  const profileId = maybeFillKey === undefined ? DEFAULT_PROFILE_ID : normalizeProfileId(profileOrFillKey);
  const fillKey = maybeFillKey === undefined ? profileOrFillKey : maybeFillKey;
  const payload = maybeFillKey === undefined ? maybePayload : maybePayload;
  const processedAt = nowIso();
  execute(`
    INSERT INTO profile_processed_fills (profile_id, fill_key, payload_json, processed_at)
    VALUES (
      ${sqlQuote(profileId)},
      ${sqlQuote(fillKey)},
      ${jsonValue(payload)},
      ${sqlQuote(processedAt)}
    )
    ON CONFLICT(profile_id, fill_key) DO NOTHING;
  `);
}

function hasProcessedFill(profileOrFillKey, maybeFillKey) {
  const profileId = maybeFillKey === undefined ? DEFAULT_PROFILE_ID : normalizeProfileId(profileOrFillKey);
  const fillKey = maybeFillKey === undefined ? profileOrFillKey : maybeFillKey;
  const row = selectOne(`
    SELECT fill_key
    FROM profile_processed_fills
    WHERE profile_id = ${sqlQuote(profileId)}
      AND fill_key = ${sqlQuote(fillKey)}
    LIMIT 1;
  `);
  return Boolean(row);
}

function cleanupProcessedFills(profileId = null, olderThanMs = safeNumber(config.fillLookbackMs, 6 * 60 * 60 * 1000)) {
  const cutoff = new Date(Date.now() - olderThanMs).toISOString();
  execute(`
    DELETE FROM profile_processed_fills
    WHERE processed_at < ${sqlQuote(cutoff)}
    ${profileId ? `AND profile_id = ${sqlQuote(normalizeProfileId(profileId))}` : ""};
  `);
}

function exportStrategiesText() {
  return JSON.stringify(loadStrategies(), null, 2);
}

function importStrategiesText(rawText, { replace = false } = {}) {
  const text = String(rawText || "").trim();
  if (!text) {
    return { imported: 0, strategies: loadStrategies() };
  }

  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    parsed = text
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  }

  const strategies = Array.isArray(parsed) ? parsed : [parsed];
  if (replace) {
    replaceStrategies(strategies);
  } else {
    for (const strategy of strategies) saveStrategy(strategy);
  }

  return { imported: strategies.length, strategies: loadStrategies() };
}

function appendAuditLog({
  profileId = null,
  actorUserId = null,
  action,
  reason = null,
  oldValue = null,
  newValue = null,
}) {
  const createdAt = nowIso();
  execute(`
    INSERT INTO audit_log (
      profile_id, actor_user_id, action, reason, old_value_json, new_value_json, created_at
    ) VALUES (
      ${profileId ? sqlQuote(normalizeProfileId(profileId)) : "NULL"},
      ${actorUserId ? sqlQuote(String(actorUserId)) : "NULL"},
      ${sqlQuote(action)},
      ${reason ? sqlQuote(reason) : "NULL"},
      ${jsonValue(oldValue)},
      ${jsonValue(newValue)},
      ${sqlQuote(createdAt)}
    );
  `);
}

function listAuditLog(profileId = null, limit = 50) {
  return selectRows(`
    SELECT *
    FROM audit_log
    ${profileId ? `WHERE profile_id = ${sqlQuote(normalizeProfileId(profileId))}` : ""}
    ORDER BY id DESC
    LIMIT ${Math.max(1, safeInteger(limit, 50))};
  `);
}

function recordReconciliation(profileId, summary) {
  const createdAt = nowIso();
  execute(`
    INSERT INTO reconciliation_log (profile_id, summary_json, created_at)
    VALUES (
      ${sqlQuote(normalizeProfileId(profileId))},
      ${jsonValue(summary)},
      ${sqlQuote(createdAt)}
    );
  `);
  setLastReconcile(profileId, summary);
  return summary;
}

function getRecentReconciliation(profileId) {
  const row = selectOne(`
    SELECT summary_json
    FROM reconciliation_log
    WHERE profile_id = ${sqlQuote(normalizeProfileId(profileId))}
    ORDER BY id DESC
    LIMIT 1;
  `);
  return row ? parseJsonSafe(row.summary_json, null) : null;
}

function updateProfileFields(profileId, fields = {}) {
  const current = getProfileById(profileId, { includeSecret: true }) || { id: normalizeProfileId(profileId) };
  const next = {
    ...current,
    ...fields,
  };
  if (fields.agentSecret) {
    next.encryptedAgentSecret = encryptSecret(fields.agentSecret);
    next.agentSecretFingerprint = fingerprintSecret(fields.agentSecret);
    delete next.agentSecret;
  }
  if (fields.masterWalletAddress !== undefined) {
    next.masterWalletAddress = normalizeAddress(fields.masterWalletAddress);
  }
  if (fields.agentWalletAddress !== undefined) {
    next.agentWalletAddress = normalizeAddress(fields.agentWalletAddress);
  }
  return upsertProfile(next);
}

function getProfileExecutionCredentials(profileId) {
  const profile = getProfileById(profileId, { includeSecret: true });
  if (!profile) return null;
  return {
    profileId: profile.id,
    secretKey: profile.agentSecret || "",
    accountAddress: normalizeAddress(profile.agentWalletAddress || profile.masterWalletAddress),
    vaultAddress: normalizeAddress(profile.masterWalletAddress),
  };
}

function submitAutomationRequest(profileId, payload = {}) {
  const profile = getProfileById(profileId, { includeSecret: true }) || { id: normalizeProfileId(profileId) };
  const requestedAt = nowIso();
  const masterWalletAddress = normalizeAddress(payload.masterWalletAddress || profile.masterWalletAddress);
  const agentWalletAddress = normalizeAddress(payload.agentWalletAddress || profile.agentWalletAddress);
  const rawSecret = String(payload.agentSecret || "").trim();
  const encryptedAgentSecret = rawSecret
    ? encryptSecret(rawSecret)
    : profile.encryptedAgentSecret || null;
  const agentSecretFingerprint = rawSecret
    ? fingerprintSecret(rawSecret)
    : profile.agentSecretFingerprint || null;
  const requestedDefaults = payload.requestedDefaults || {};

  const nextProfile = upsertProfile({
    ...profile,
    id: normalizeProfileId(profileId),
    telegramUserId: payload.telegramUserId || profile.telegramUserId || null,
    chatId: payload.chatId || profile.chatId || null,
    username: payload.username || profile.username || null,
    displayName: payload.displayName || profile.displayName || null,
    label: payload.label || profile.label || null,
    masterWalletAddress,
    agentWalletAddress,
    encryptedAgentSecret,
    agentSecretFingerprint,
    automationStatus: "PENDING",
    automationEnabled: false,
    walletEnabled: true,
    status: "ACTIVE",
    role: profile.role || "USER",
  });

  execute(`
    INSERT INTO automation_requests (
      profile_id, telegram_user_id, username, display_name, master_wallet_address,
      agent_wallet_address, encrypted_agent_secret, agent_secret_fingerprint,
      requested_defaults_json, status, rejection_reason, reviewed_by, requested_at,
      reviewed_at, payload_json
    ) VALUES (
      ${sqlQuote(nextProfile.id)},
      ${nextProfile.telegramUserId ? sqlQuote(nextProfile.telegramUserId) : "NULL"},
      ${nextProfile.username ? sqlQuote(nextProfile.username) : "NULL"},
      ${nextProfile.displayName ? sqlQuote(nextProfile.displayName) : "NULL"},
      ${masterWalletAddress ? sqlQuote(masterWalletAddress) : "NULL"},
      ${agentWalletAddress ? sqlQuote(agentWalletAddress) : "NULL"},
      ${encryptedAgentSecret ? sqlQuote(encryptedAgentSecret) : "NULL"},
      ${agentSecretFingerprint ? sqlQuote(agentSecretFingerprint) : "NULL"},
      ${jsonValue(requestedDefaults)},
      'PENDING',
      NULL,
      NULL,
      ${sqlQuote(requestedAt)},
      NULL,
      ${jsonValue({
        profileId: nextProfile.id,
        telegramUserId: nextProfile.telegramUserId,
        username: nextProfile.username,
        displayName: nextProfile.displayName,
        label: nextProfile.label,
        masterWalletAddress,
        agentWalletAddress,
        agentSecretFingerprint,
        requestedDefaults,
      })}
    );
  `);

  appendAuditLog({
    profileId: nextProfile.id,
    actorUserId: nextProfile.telegramUserId,
    action: "automation-request-submitted",
    newValue: {
      masterWalletAddress,
      agentWalletAddress,
      agentSecretFingerprint,
      requestedDefaults,
    },
  });

  const request = selectOne("SELECT * FROM automation_requests ORDER BY id DESC LIMIT 1;");
  return {
    profile: nextProfile,
    request: request
      ? {
          id: safeInteger(request.id, 0),
          profileId: request.profile_id,
          telegramUserId: request.telegram_user_id,
          username: request.username,
          displayName: request.display_name,
          masterWalletAddress: request.master_wallet_address,
          agentWalletAddress: request.agent_wallet_address,
          agentSecretFingerprint: request.agent_secret_fingerprint,
          requestedDefaults: parseJsonSafe(request.requested_defaults_json, {}),
          status: request.status,
          requestedAt: request.requested_at,
        }
      : null,
  };
}

function listAutomationRequests(status = null) {
  const rows = selectRows(`
    SELECT *
    FROM automation_requests
    ${status ? `WHERE status = ${sqlQuote(String(status).toUpperCase())}` : ""}
    ORDER BY requested_at DESC, id DESC;
  `);
  return rows.map((row) => ({
    id: safeInteger(row.id, 0),
    profileId: row.profile_id,
    telegramUserId: row.telegram_user_id,
    username: row.username,
    displayName: row.display_name,
    masterWalletAddress: row.master_wallet_address,
    agentWalletAddress: row.agent_wallet_address,
    agentSecretFingerprint: row.agent_secret_fingerprint,
    requestedDefaults: parseJsonSafe(row.requested_defaults_json, {}),
    status: row.status,
    rejectionReason: row.rejection_reason,
    reviewedBy: row.reviewed_by,
    requestedAt: row.requested_at,
    reviewedAt: row.reviewed_at,
    payload: parseJsonSafe(row.payload_json, {}),
  }));
}

function updateAutomationRequestStatus(requestId, status, { actorUserId = null, reason = null } = {}) {
  const request = listAutomationRequests().find((item) => item.id === safeInteger(requestId, 0));
  if (!request) return null;
  const reviewedAt = nowIso();
  execute(`
    UPDATE automation_requests
    SET status = ${sqlQuote(status)},
        rejection_reason = ${reason ? sqlQuote(reason) : "NULL"},
        reviewed_by = ${actorUserId ? sqlQuote(String(actorUserId)) : "NULL"},
        reviewed_at = ${sqlQuote(reviewedAt)}
    WHERE id = ${safeInteger(requestId, 0)};
  `);

  const profile = getProfileById(request.profileId, { includeSecret: true });
  if (profile) {
    const oldValue = {
      automationStatus: profile.automationStatus,
      automationEnabled: profile.automationEnabled,
      walletEnabled: profile.walletEnabled,
    };
    const nextStatus =
      status === "APPROVED" ? "APPROVED" : status === "REJECTED" ? "REJECTED" : profile.automationStatus;
    const updated = upsertProfile({
      ...profile,
      automationStatus: nextStatus,
      automationEnabled: status === "APPROVED" ? true : false,
      walletEnabled: status === "APPROVED" ? profile.walletEnabled : false,
      status: status === "APPROVED" ? "ACTIVE" : profile.status,
    });
    appendAuditLog({
      profileId: profile.id,
      actorUserId,
      action: status === "APPROVED" ? "automation-approved" : "automation-rejected",
      reason,
      oldValue,
      newValue: {
        automationStatus: updated.automationStatus,
        automationEnabled: updated.automationEnabled,
        walletEnabled: updated.walletEnabled,
      },
    });
    return updated;
  }

  return null;
}

function setAutomationEnabled(profileId, enabled, actorUserId = null, reason = null) {
  const profile = getProfileById(profileId, { includeSecret: true });
  if (!profile) return null;
  const updated = upsertProfile({
    ...profile,
    automationEnabled: Boolean(enabled),
    walletEnabled: enabled ? profile.walletEnabled : false,
    automationStatus:
      enabled && profile.automationStatus === "NOT_CONFIGURED"
        ? "PENDING"
        : profile.automationStatus,
  });
  appendAuditLog({
    profileId: profile.id,
    actorUserId,
    action: enabled ? "automation-enabled" : "automation-disabled",
    reason,
    oldValue: {
      automationEnabled: profile.automationEnabled,
      walletEnabled: profile.walletEnabled,
    },
    newValue: {
      automationEnabled: updated.automationEnabled,
      walletEnabled: updated.walletEnabled,
    },
  });
  return updated;
}

function setWalletEnabled(profileId, enabled, actorUserId = null, reason = null) {
  const profile = getProfileById(profileId, { includeSecret: true });
  if (!profile) return null;
  const updated = upsertProfile({
    ...profile,
    walletEnabled: Boolean(enabled),
    status: enabled ? "ACTIVE" : "DISABLED",
  });
  appendAuditLog({
    profileId: profile.id,
    actorUserId,
    action: enabled ? "wallet-enabled" : "wallet-disabled",
    reason,
    oldValue: { walletEnabled: profile.walletEnabled, status: profile.status },
    newValue: { walletEnabled: updated.walletEnabled, status: updated.status },
  });
  return updated;
}

function removeAutomationProfile(profileId, actorUserId = null, reason = null) {
  const profile = getProfileById(profileId, { includeSecret: true });
  if (!profile) return null;
  const updated = upsertProfile({
    ...profile,
    encryptedAgentSecret: null,
    agentSecretFingerprint: null,
    automationStatus: "REMOVED",
    automationEnabled: false,
    walletEnabled: false,
    status: "DISABLED",
  });
  appendAuditLog({
    profileId: updated.id,
    actorUserId,
    action: "automation-removed",
    reason,
    oldValue: {
      automationStatus: profile.automationStatus,
      automationEnabled: profile.automationEnabled,
      walletEnabled: profile.walletEnabled,
    },
    newValue: {
      automationStatus: updated.automationStatus,
      automationEnabled: updated.automationEnabled,
      walletEnabled: updated.walletEnabled,
    },
  });
  return updated;
}

function getModeLockState(profileId) {
  const activeTrades = listOpenTrades(profileId).filter((trade) =>
    ACTIVE_LOCKED_TRADE_STATUSES.includes(String(trade.status || "").toUpperCase())
  );
  return {
    locked: activeTrades.length > 0,
    trades: activeTrades,
  };
}

function getStrategyRuntimeSettings(profileId = DEFAULT_PROFILE_ID) {
  const runtime = getRuntimeSettings(profileId);
  return {
    strategyCap: runtime.strategyCap || getSetting(SETTINGS_KEYS.strategyCap, config.strategyCap),
    strategyRetentionDays:
      runtime.strategyRetentionDays ||
      getSetting(SETTINGS_KEYS.strategyRetentionDays, config.strategyRetentionDays),
    lastStrategyPruneAt: getSetting(SETTINGS_KEYS.lastStrategyPruneAt, null),
  };
}

function setStrategyCap(value, profileId = DEFAULT_PROFILE_ID) {
  setSetting(SETTINGS_KEYS.strategyCap, Math.max(1, safeInteger(value, 500)));
  return setProfileSetting(
    normalizeProfileId(profileId),
    PROFILE_SETTINGS_KEYS.strategyCap,
    Math.max(1, safeInteger(value, 500))
  );
}

function setStrategyRetentionDays(value, profileId = DEFAULT_PROFILE_ID) {
  setSetting(SETTINGS_KEYS.strategyRetentionDays, Math.max(1, safeInteger(value, 7)));
  return setProfileSetting(
    normalizeProfileId(profileId),
    PROFILE_SETTINGS_KEYS.strategyRetentionDays,
    Math.max(1, safeInteger(value, 7))
  );
}

function setLastStrategyPruneAt(value) {
  return setSetting(SETTINGS_KEYS.lastStrategyPruneAt, value);
}

function migrateLegacyJsonIfNeeded() {
  ensureDefaultProfile();
  if (getSetting(SETTINGS_KEYS.legacyMigrated, false)) {
    return;
  }

  if (rowCount("profile_watched_pairs") === 0) {
    const legacyPairs =
      readLegacyJson(config.pairsPath, null) ||
      (tableExists("watched_pairs")
        ? selectRows("SELECT pair FROM watched_pairs ORDER BY pair ASC;").map((row) => row.pair)
        : []) ||
      [];
    saveWatchedPairs(DEFAULT_PROFILE_ID, legacyPairs.length ? legacyPairs : getAllowedPairs());
  }

  if (rowCount("profile_active_signals") === 0) {
    const legacySignals = readLegacyJson(config.activeSignalsPath, null);
    if (legacySignals && typeof legacySignals === "object") {
      saveActiveSignals(DEFAULT_PROFILE_ID, legacySignals);
    } else {
      const rows = tableExists("active_signals")
        ? selectRows("SELECT signal_key, payload_json FROM active_signals;")
        : [];
      if (rows.length) {
        const next = rows.reduce((accumulator, row) => {
          accumulator[row.signal_key] = parseJsonSafe(row.payload_json, {});
          return accumulator;
        }, {});
        saveActiveSignals(DEFAULT_PROFILE_ID, next);
      }
    }
  }

  execute(`
    UPDATE trades
    SET profile_id = ${sqlQuote(DEFAULT_PROFILE_ID)}
    WHERE profile_id IS NULL OR profile_id = '';
  `);

  if (rowCount("trades") === 0) {
    const openTrades = readLegacyJson(config.dryRunPositionsPath, []) || [];
    const closedTrades = readLegacyJson(config.closedTradesPath, []) || [];
    for (const trade of [...openTrades, ...closedTrades]) {
      if (trade?.id || trade?.signalId) {
        saveTrade({
          ...trade,
          id: trade.id || trade.signalId,
          profileId: DEFAULT_PROFILE_ID,
          status:
            trade.status ||
            (trade.closeTime || trade.closedAt ? "CLOSED" : "OPEN"),
          phase: trade.phase || "LEGACY",
          pair: String(trade.pair || "").toUpperCase(),
          capitalMode: trade.capitalMode || config.defaultCapitalMode,
          executionMode: trade.executionMode || config.defaultExecutionMode,
          strategyBucket: trade.strategyBucket || trade.strategyUsed || null,
        });
      }
    }
  }

  if (rowCount("profile_pair_state") === 0) {
    const rows = tableExists("pair_state")
      ? selectRows("SELECT pair, payload_json FROM pair_state ORDER BY pair ASC;")
      : [];
    for (const row of rows) {
      savePairState(DEFAULT_PROFILE_ID, row.pair, parseJsonSafe(row.payload_json, {}));
    }
  }

  if (rowCount("profile_processed_fills") === 0) {
    const rows = tableExists("processed_fills")
      ? selectRows("SELECT fill_key, payload_json, processed_at FROM processed_fills;")
      : [];
    for (const row of rows) {
      execute(`
        INSERT INTO profile_processed_fills (profile_id, fill_key, payload_json, processed_at)
        VALUES (
          ${sqlQuote(DEFAULT_PROFILE_ID)},
          ${sqlQuote(row.fill_key)},
          ${jsonValue(parseJsonSafe(row.payload_json, {}))},
          ${sqlQuote(row.processed_at || nowIso())}
        )
        ON CONFLICT(profile_id, fill_key) DO NOTHING;
      `);
    }
  }

  if (rowCount("strategies") === 0 && fs.existsSync(config.strategiesDir)) {
    const strategyFiles = fs
      .readdirSync(config.strategiesDir)
      .filter((file) => file.endsWith(".json") && file !== "index.json");

    for (const fileName of strategyFiles) {
      const strategy = readLegacyJson(path.join(config.strategiesDir, fileName), null);
      if (strategy?.id) {
        saveStrategy({
          ...strategy,
          fileName: strategy.fileName || fileName,
        });
      }
    }
  }

  if (getSnapshot("scoreState", null) == null) {
    setSnapshot("scoreState", readLegacyJson(config.scoreStatePath, {}));
  }
  if (getSnapshot("learnedPumps", null) == null) {
    setSnapshot("learnedPumps", readLegacyJson(config.learnedPumpsPath, []));
  }

  setSetting(SETTINGS_KEYS.legacyMigrated, true);
}

function ensureStorage() {
  ensureDir(config.storageDir);
  ensureDir(config.strategiesDir);
  ensureDir(config.exportsDir);
  ensureSchema();
  seedDefaultSettings();
  ensureDefaultProfile();
  migrateLegacyJsonIfNeeded();
}

function readJson(filePath, defaultValue = null) {
  if (filePath === config.pairsPath) return getWatchedPairs(DEFAULT_PROFILE_ID);
  if (filePath === config.scoreStatePath) return getSnapshot("scoreState", defaultValue);
  if (filePath === config.activeSignalsPath) return getActiveSignals(DEFAULT_PROFILE_ID);
  if (filePath === config.dryRunPositionsPath) return listOpenTrades(DEFAULT_PROFILE_ID);
  if (filePath === config.closedTradesPath) return listClosedTradesByProfile(DEFAULT_PROFILE_ID);
  if (filePath === config.learnedPumpsPath) return getSnapshot("learnedPumps", defaultValue);
  if (filePath === config.strategiesIndexPath) return loadStrategiesIndex();
  return defaultValue;
}

function writeJson(filePath, data) {
  if (filePath === config.pairsPath) return saveWatchedPairs(DEFAULT_PROFILE_ID, data);
  if (filePath === config.scoreStatePath) return setSnapshot("scoreState", data);
  if (filePath === config.activeSignalsPath) return saveActiveSignals(DEFAULT_PROFILE_ID, data);
  if (filePath === config.learnedPumpsPath) return setSnapshot("learnedPumps", data);
  if (filePath === config.strategiesIndexPath) return replaceStrategies(data);
  return data;
}

function appendJsonArray(filePath, item) {
  const current = readJson(filePath, []);
  const next = [...current, item];
  writeJson(filePath, next);
  return next;
}

module.exports = {
  SETTINGS_KEYS,
  PROFILE_SETTINGS_KEYS,
  DEFAULT_PROFILE_ID,
  TERMINAL_TRADE_STATUSES,
  ACTIVE_LOCKED_TRADE_STATUSES,
  ensureDir,
  ensureStorage,
  nowIso,
  uniqueUpper,
  safeNumber,
  safeInteger,
  normalizeCapitalMode,
  normalizeExecutionMode,
  getAllowedPairs,
  filterToAllowedPairs,
  getSetting,
  setSetting,
  getSnapshot,
  setSnapshot,
  buildStrategySummary,
  upsertProfile,
  getProfileById,
  findProfileByTelegramUserId,
  findProfile,
  listProfiles,
  getOrCreateProfileFromTelegram,
  getProfileSetting,
  setProfileSetting,
  updateProfileSettings,
  updateProfileFields,
  getProfileExecutionCredentials,
  getRuntimeSettings,
  setAutoTradeEnabled,
  setTradeBalanceTarget,
  setTradeLeverage,
  setLastFillSyncAt,
  setCapitalMode,
  setExecutionMode,
  setBaselinePrincipal,
  setSimpleSlots,
  setDemoBalances,
  setSweepStats,
  setLastReconcile,
  getWatchedPairs,
  listWatchedPairsByProfile,
  saveWatchedPairs,
  addWatchedPair,
  removeWatchedPair,
  getActiveSignals,
  saveActiveSignals,
  deleteActiveSignal,
  saveTrade,
  getTradeById,
  listOpenTrades,
  listClosedTrades,
  listClosedTradesByProfile,
  listAllTrades,
  findOpenTradeByPair,
  getPairState,
  savePairState,
  listPairStates,
  saveStrategy,
  loadStrategies,
  loadStrategiesIndex,
  getStrategyByPair,
  replaceStrategies,
  deleteStrategiesByIds,
  exportStrategiesText,
  importStrategiesText,
  markProcessedFill,
  hasProcessedFill,
  cleanupProcessedFills,
  appendAuditLog,
  listAuditLog,
  recordReconciliation,
  getRecentReconciliation,
  submitAutomationRequest,
  listAutomationRequests,
  updateAutomationRequestStatus,
  setAutomationEnabled,
  setWalletEnabled,
  removeAutomationProfile,
  getModeLockState,
  getStrategyRuntimeSettings,
  setStrategyCap,
  setStrategyRetentionDays,
  setLastStrategyPruneAt,
  readJson,
  writeJson,
  appendJsonArray,
};
