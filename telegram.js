const fs = require("fs");
const path = require("path");
const axios = require("axios");
const TelegramBot = require("node-telegram-bot-api");
const config = require("./config");
const state = require("./state");
const pairUniverse = require("./pairUniverse");
const tradeManager = require("./dryrun");
const strategyMaintenance = require("./strategyMaintenance");
const {
  loadStrategiesIndex,
  getStrategyByPair,
  rebuildStrategiesIndexFromFiles,
} = require("./strategyLearner");
const {
  buildModeSwitchBlockedMessage,
  buildReconciliationCompleteMessage,
  buildAutomationApprovedMessage,
  buildAutomationDisabledMessage,
} = require("./telegramMessageBuilder");
const { maskAddress } = require("./security");

const COMMANDS = [
  { command: "start", description: "Open control panel" },
  { command: "menu", description: "Open main menu" },
  { command: "help", description: "Show command list" },
  { command: "status", description: "Show bot status" },
  { command: "scan", description: "Open manual scan screen" },
  { command: "positions", description: "Show active positions" },
  { command: "closed", description: "Show recent closed trades" },
  { command: "pnl", description: "Show PnL dashboard" },
  { command: "balance", description: "Show balance screen" },
  { command: "pairs", description: "Open pair management" },
  { command: "capitalmode", description: "Open capital mode screen" },
  { command: "executionmode", description: "Open execution mode screen" },
  { command: "reconcile", description: "Open reconciliation screen" },
  { command: "automationstatus", description: "Show automation status" },
  { command: "strategystatus", description: "Show strategy status" },
  { command: "exportstrategies", description: "Export learned strategies" },
  { command: "importstrategies", description: "Import saved strategies" },
  { command: "importstrategyfile", description: "Import a local strategy file" },
  { command: "strategyexports", description: "List strategy backups" },
  { command: "cleardemohistory", description: "Clear closed demo trade history" },
  { command: "admin", description: "Open admin panel" },
];

const BLOCKING_TRADE_STATUSES = new Set([
  "ENTRY_PENDING",
  "ENTRY_PLACED",
  "PARTIALLY_FILLED",
  "OPEN",
  "PROTECTED",
  "EXIT_PENDING",
  "RECONCILING",
]);
const PENDING_ORDER_STATUSES = new Set(["ENTRY_PENDING", "ENTRY_PLACED", "PARTIALLY_FILLED"]);
const PROTECTED_TRADE_STATUSES = new Set(["PROTECTED"]);

const SCREEN = {
  HOME: "home",
  STATUS: "status",
  TRADING: "trading",
  CAPITAL: "capital",
  EXECUTION: "execution",
  PAIRS: "pairs",
  POSITIONS: "positions",
  CLOSED: "closed",
  PNL: "pnl",
  BALANCE: "balance",
  SCAN: "scan",
  DRYRUN: "dryrun",
  AUTOMATION: "automation",
  RECONCILE: "reconcile",
  STRATEGIES: "strategies",
  ADMIN: "admin",
  APPROVALS: "approvals",
  HELP: "help",
};

const MAIN_MENU = {
  STATUS: "📊 Status",
  PNL: "📈 PnL",
  BALANCE: "💼 Balance",
  POSITIONS: "📍 Positions",
  CLOSED: "✅ Closed",
  SCAN: "🔎 Scan",
  TRADING: "⚙️ Trading",
  PAIRS: "🪙 Pairs",
  STRATEGIES: "🧠 Strategies",
  AUTOMATION: "🛡️ Automation",
  EXECUTION: "🧪 Demo/Real",
  RECONCILE: "🔄 Reconcile",
  HELP: "❓ Help",
  ADMIN: "👑 Admin",
  APPROVALS: "📋 Approvals",
};
const ephemeralMessageTimers = new Map();
const MENU_ANCHOR_REFRESH_MS = 12 * 60 * 60 * 1000;

function createBot(options = {}) {
  if (!config.telegramBotToken) {
    console.error("Missing TELEGRAM_BOT_TOKEN in .env");
    return null;
  }

  const polling =
    typeof options.polling === "boolean" ? options.polling : config.telegramPolling;

  return new TelegramBot(config.telegramBotToken, {
    polling: polling
      ? {
          autoStart: true,
          interval: 1000,
          params: {
            timeout: 20,
          },
        }
      : false,
    request: {
      timeout: 30_000,
    },
  });
}

async function setupCommands(bot) {
  if (!bot) return;

  try {
    await bot.setMyCommands(COMMANDS);
    if (typeof bot.setChatMenuButton === "function") {
      await bot.setChatMenuButton({
        menu_button: {
          type: "commands",
        },
      });
    }
    console.log("Telegram commands registered.");
  } catch (error) {
    console.error("Failed to register Telegram commands:", error.message);
  }
}

function htmlEscape(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function splitLongMessage(text, chunkSize = 3500) {
  const lines = String(text).split("\n");
  const chunks = [];
  let current = "";

  for (const line of lines) {
    if ((current + "\n" + line).length > chunkSize) {
      if (current.trim()) chunks.push(current.trim());
      current = line;
    } else {
      current += `${current ? "\n" : ""}${line}`;
    }
  }

  if (current.trim()) chunks.push(current.trim());
  return chunks;
}

function formatNumber(value, digits = null) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  if (digits != null) return num.toFixed(digits);
  if (Math.abs(num) >= 1000) return num.toFixed(2);
  if (Math.abs(num) >= 1) return num.toFixed(4);
  return num.toFixed(6);
}

function formatPercent(value, digits = 2) {
  const num = Number(value);
  return Number.isFinite(num) ? `${num.toFixed(digits)}%` : "N/A";
}

function formatTimestamp(value) {
  if (!value) return "never";
  const raw = value.timestamp || value.updatedAt || value.createdAt || value.fetchedAt || value;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return String(raw);
  return parsed.toISOString().replace("T", " ").replace(".000Z", " UTC");
}

function shortText(value, maxLength = 80) {
  const text = String(value || "").trim();
  if (!text) return "none";
  return text.length > maxLength ? `${text.slice(0, maxLength - 3)}...` : text;
}

function isBenignEditError(error) {
  const message = String(error?.message || "");
  return /message is not modified|message to edit not found/i.test(message);
}

function formatFileSize(bytes) {
  const size = Number(bytes);
  if (!Number.isFinite(size) || size < 0) return "N/A";
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(2)} MB`;
}

function uniqueStrings(values) {
  return [...new Set((values || []).map((value) => String(value).trim()).filter(Boolean))];
}

function listStrategyExportFiles(limit = 10) {
  state.ensureDir(config.exportsDir);
  return fs
    .readdirSync(config.exportsDir)
    .filter((fileName) => /^strategies-\d+\.txt$/i.test(fileName))
    .map((fileName) => {
      const filePath = path.join(config.exportsDir, fileName);
      const stats = fs.statSync(filePath);
      return {
        name: fileName,
        path: filePath,
        sizeBytes: stats.size,
        modifiedAt: stats.mtime.toISOString(),
      };
    })
    .sort((a, b) => Date.parse(b.modifiedAt) - Date.parse(a.modifiedAt))
    .slice(0, Math.max(1, limit));
}

function resolveStrategyExportFile(selector = "latest") {
  const exports = listStrategyExportFiles(100);
  if (!exports.length) return null;

  const raw = String(selector || "latest").trim();
  if (!raw || raw.toLowerCase() === "latest") {
    return exports[0];
  }

  const normalized = path.basename(raw);
  return exports.find((item) => item.name === normalized) || null;
}

function stripWrappedQuotes(value) {
  const text = String(value || "").trim();
  if (
    (text.startsWith('"') && text.endsWith('"')) ||
    (text.startsWith("'") && text.endsWith("'"))
  ) {
    return text.slice(1, -1).trim();
  }
  return text;
}

function parseImportPathArgs(argsText = "") {
  const raw = String(argsText || "").trim();
  if (!raw) {
    return { replace: false, filePath: "" };
  }

  const lower = raw.toLowerCase();
  if (lower.startsWith("replace ")) {
    return {
      replace: true,
      filePath: stripWrappedQuotes(raw.slice("replace ".length)),
    };
  }
  if (lower.startsWith("merge ")) {
    return {
      replace: false,
      filePath: stripWrappedQuotes(raw.slice("merge ".length)),
    };
  }

  return {
    replace: false,
    filePath: stripWrappedQuotes(raw),
  };
}

function normalizeTradeStatus(trade) {
  return String(trade?.status || "").toUpperCase();
}

function isBlockingTrade(trade) {
  return BLOCKING_TRADE_STATUSES.has(normalizeTradeStatus(trade));
}

function isPendingTrade(trade) {
  return PENDING_ORDER_STATUSES.has(normalizeTradeStatus(trade));
}

function isProtectedTrade(trade) {
  return PROTECTED_TRADE_STATUSES.has(normalizeTradeStatus(trade));
}

function profileLabel(profile) {
  return profile?.label || profile?.displayName || profile?.username || profile?.id || "unknown";
}

function isAdmin(userId) {
  return config.telegramAdminIds.includes(String(userId || ""));
}

function buildHelpText() {
  return [
    "❓ Hyperliquid Telegram Control Panel",
    "",
    "Use the reply keyboard for the main menu, or use these commands directly.",
    "",
    "General",
    "/start",
    "/menu",
    "/help",
    "/status",
    "/scan",
    "/positions",
    "/closed",
    "/balance",
    "/pnl",
    "",
    "Capital Mode",
    "/capitalmode",
    "/setcapitalmode simple",
    "/setcapitalmode compounding",
    "/principal",
    "/setprincipal 100",
    "/slots",
    "/setslots 2",
    "/capitalstatus",
    "",
    "Execution Mode",
    "/executionmode",
    "/setexecutionmode real",
    "/setexecutionmode demo",
    "/demobalance",
    "/setdemobalance 100",
    "/realstatus",
    "/demostatus",
    "",
    "Trading",
    "/autotrade on",
    "/autotrade off",
    "/tradebalance",
    "/settradebalance 100",
    "/leverage",
    "/setleverage 10",
    "/dryrun",
    "/dryrunlong",
    "/dryrunshort",
    "/dryrunpair BTCUSDT long",
    "",
    "Pairs",
    "/pairs",
    "/addpair BTCUSDT",
    "/removepair BTCUSDT",
    "/reloadpairs",
    "/pairstatus BTCUSDT",
    "",
    "Reconciliation",
    "/reconcile",
    "/reconcile user <user>",
    "/reconcileall",
    "",
    "Automation",
    "/connecttrading",
    "/setwallet 0x...",
    "/setagentaddress 0x...",
    "/setagentprivatekey <secret>",
    "/submitautomationrequest",
    "/automationstatus",
    "/enableautomation",
    "/disableautomation",
    "",
    "Strategy",
    "/strategystatus",
    "/strategytop",
    "/strategyprune",
    "/exportstrategies",
    "/importstrategies latest",
    "/importstrategies strategies-1234567890.txt",
    "/importstrategyfile /abs/path/file.json",
    "/strategyexports",
    "/setstrategycap 500",
    "/setstrategyretentiondays 7",
    "/cleardemohistory",
    "",
    "Admin",
    "/admin",
    "/pendingapprovals",
    "/approveautomation <request_id>",
    "/rejectautomation <request_id> <reason>",
    "/viewuserconfig <user>",
    "/forcesetcapitalmode <user> simple|compounding",
    "/forcesetexecutionmode <user> real|demo",
  ].join("\n");
}

function buildMainMenu(userId = null) {
  const keyboard = [
    [MAIN_MENU.STATUS, MAIN_MENU.PNL, MAIN_MENU.BALANCE],
    [MAIN_MENU.POSITIONS, MAIN_MENU.CLOSED, MAIN_MENU.SCAN],
    [MAIN_MENU.TRADING, MAIN_MENU.PAIRS, MAIN_MENU.STRATEGIES],
    [MAIN_MENU.AUTOMATION, MAIN_MENU.EXECUTION, MAIN_MENU.RECONCILE],
    [MAIN_MENU.HELP],
  ];

  if (isAdmin(userId)) {
    keyboard.push([MAIN_MENU.ADMIN, MAIN_MENU.APPROVALS]);
  }

  return {
    reply_markup: {
      keyboard,
      resize_keyboard: true,
      one_time_keyboard: false,
      is_persistent: true,
      input_field_placeholder: "Use menu buttons or type a command",
    },
  };
}

function menuAnchorSnapshotKey(chatId, userId = null) {
  return `telegramMenuAnchor:${chatId}:${userId || "anon"}`;
}

function getMenuAnchor(chatId, userId = null) {
  return state.getSnapshot(menuAnchorSnapshotKey(chatId, userId), null);
}

function setMenuAnchor(chatId, userId = null, anchor = null) {
  state.setSnapshot(
    menuAnchorSnapshotKey(chatId, userId),
    anchor
      ? {
          ...anchor,
          updatedAt: state.nowIso(),
        }
      : null
  );
}

function timerKey(chatId, messageId) {
  return `${chatId}:${messageId}`;
}

function clearEphemeralTimer(chatId, messageId) {
  const key = timerKey(chatId, messageId);
  const timer = ephemeralMessageTimers.get(key);
  if (timer) {
    clearTimeout(timer);
    ephemeralMessageTimers.delete(key);
  }
}

function armEphemeralDelete(bot, chatId, messageId, ttlMs = config.telegramEphemeralUiTtlMs) {
  const ttl = Number(ttlMs || 0);
  if (!bot || !chatId || !messageId || !Number.isFinite(ttl) || ttl <= 0) return;

  clearEphemeralTimer(chatId, messageId);
  const key = timerKey(chatId, messageId);
  const timer = setTimeout(async () => {
    try {
      await bot.deleteMessage(chatId, messageId);
    } catch (error) {
      // Best effort only. User-message deletion may require admin rights in groups.
    } finally {
      ephemeralMessageTimers.delete(key);
    }
  }, ttl);

  if (typeof timer.unref === "function") timer.unref();
  ephemeralMessageTimers.set(key, timer);
}

function armEphemeralDeleteForMessage(bot, message, ttlMs = config.telegramEphemeralUiTtlMs) {
  if (!message?.chat?.id || !message?.message_id) return;
  armEphemeralDelete(bot, message.chat.id, message.message_id, ttlMs);
}

function withPersistentMenu(extra = {}, userId = null) {
  if (extra.reply_markup?.inline_keyboard) return extra;
  return {
    ...buildMainMenu(userId),
    ...extra,
  };
}

async function sendMessage(bot, chatId, text, extra = {}, context = {}) {
  const message = await bot.sendMessage(chatId, text, withPersistentMenu(extra, context.userId));
  armEphemeralDeleteForMessage(bot, message, context.ttlMs);
  return message;
}

async function sendHtml(bot, chatId, html, extra = {}, context = {}) {
  const message = await bot.sendMessage(chatId, html, withPersistentMenu(
    {
      parse_mode: "HTML",
      ...extra,
    },
    context.userId
  ));
  armEphemeralDeleteForMessage(bot, message, context.ttlMs);
  return message;
}

async function ensureMainMenu(bot, chatId, userId) {
  return ensureMenuAnchor(bot, chatId, userId);
}

async function ensureMenuAnchor(
  bot,
  chatId,
  userId,
  { force = false, replyToMessageId = null } = {}
) {
  if (!bot || !chatId) return null;

  const existing = getMenuAnchor(chatId, userId);
  const existingUpdatedAt = existing?.updatedAt ? Date.parse(existing.updatedAt) : NaN;
  const isFresh =
    existing?.messageId &&
    Number.isFinite(existingUpdatedAt) &&
    Date.now() - existingUpdatedAt < MENU_ANCHOR_REFRESH_MS;

  if (!force && isFresh) {
    return existing;
  }

  if (existing?.messageId) {
    try {
      await bot.deleteMessage(chatId, existing.messageId);
    } catch (error) {
      // Best effort only. Old anchor may already be gone.
    }
  }

  const message = await bot.sendMessage(
    chatId,
    "📌 Main menu buttons are available below the input field.",
    {
      ...buildMainMenu(userId),
      disable_notification: true,
      ...(replyToMessageId ? { reply_to_message_id: replyToMessageId } : {}),
    }
  );

  setMenuAnchor(chatId, userId, {
    chatId,
    userId,
    messageId: message.message_id,
  });
  return message;
}

function inlineKeyboard(rows) {
  return {
    reply_markup: {
      inline_keyboard: rows,
    },
  };
}

function screenCallback(screen) {
  return `scr:${screen}`;
}

function promptCallback(kind, backScreen = SCREEN.HOME) {
  return `prm:${kind}:${backScreen}`;
}

function pnlCallback(executionMode = "all", capitalMode = "all") {
  return `pnl:${executionMode}:${capitalMode}`;
}

function button(text, callbackData) {
  return {
    text,
    callback_data: callbackData,
  };
}

function urlButton(text, url) {
  return {
    text,
    url,
  };
}

function parseCommand(text = "") {
  const raw = String(text || "").trim();
  const match = raw.match(/^\/([a-z_]+)(?:@\w+)?(?:\s+(.+))?$/i);
  if (!match) return null;
  const command = String(match[1] || "").toLowerCase();
  const argsText = String(match[2] || "").trim();
  const args = argsText ? argsText.split(/\s+/).filter(Boolean) : [];
  return { command, args, argsText };
}

function resolveMenuTextAction(text = "") {
  const raw = String(text || "").trim();
  switch (raw) {
    case MAIN_MENU.STATUS:
      return { type: "screen", screen: SCREEN.STATUS };
    case MAIN_MENU.PNL:
      return { type: "screen", screen: SCREEN.PNL };
    case MAIN_MENU.BALANCE:
      return { type: "screen", screen: SCREEN.BALANCE };
    case MAIN_MENU.POSITIONS:
      return { type: "screen", screen: SCREEN.POSITIONS };
    case MAIN_MENU.CLOSED:
      return { type: "screen", screen: SCREEN.CLOSED };
    case MAIN_MENU.SCAN:
      return { type: "screen", screen: SCREEN.SCAN };
    case MAIN_MENU.TRADING:
      return { type: "screen", screen: SCREEN.TRADING };
    case MAIN_MENU.PAIRS:
      return { type: "screen", screen: SCREEN.PAIRS };
    case MAIN_MENU.STRATEGIES:
      return { type: "screen", screen: SCREEN.STRATEGIES };
    case MAIN_MENU.AUTOMATION:
      return { type: "screen", screen: SCREEN.AUTOMATION };
    case MAIN_MENU.EXECUTION:
      return { type: "screen", screen: SCREEN.EXECUTION };
    case MAIN_MENU.RECONCILE:
      return { type: "screen", screen: SCREEN.RECONCILE };
    case MAIN_MENU.HELP:
      return { type: "screen", screen: SCREEN.HELP };
    case MAIN_MENU.ADMIN:
      return { type: "screen", screen: SCREEN.ADMIN };
    case MAIN_MENU.APPROVALS:
      return { type: "screen", screen: SCREEN.APPROVALS };
    default:
      return null;
  }
}

function promptSnapshotKey(profileId) {
  return `telegramPrompt:${profileId}`;
}

function dryRunSelectionKey(profileId) {
  return `dryRunSelection:${profileId}`;
}

function setPendingPrompt(profileId, prompt = null) {
  state.setSnapshot(promptSnapshotKey(profileId), prompt ? { ...prompt, updatedAt: state.nowIso() } : null);
}

function getPendingPrompt(profileId) {
  return state.getSnapshot(promptSnapshotKey(profileId), null);
}

function clearPendingPrompt(profileId) {
  setPendingPrompt(profileId, null);
}

function setDryRunSelection(profileId, selection = {}) {
  state.setSnapshot(dryRunSelectionKey(profileId), {
    pair: selection.pair || null,
    updatedAt: state.nowIso(),
  });
}

function getDryRunSelection(profileId) {
  return state.getSnapshot(dryRunSelectionKey(profileId), null) || {};
}

function buildPromptText(kind) {
  switch (kind) {
    case "principal":
      return [
        "✏️ Set Principal",
        "",
        "Send the max principal amount in USDT.",
        "",
        "Example:",
        "`/setprincipal 100`",
        "",
        "In SIMPLE mode:",
        "- If wallet balance is above this amount, bot uses only this amount.",
        "- If wallet balance is below this amount, bot uses available balance.",
        "- Profit above this amount stays in wallet but is not used for trade sizing.",
      ].join("\n");
    case "slots":
      return [
        "🔢 Set Trade Slots",
        "",
        "Send the slot count for SIMPLE mode.",
        "",
        "Example:",
        "`/setslots 2`",
      ].join("\n");
    case "demo_balance":
      return [
        "✏️ Set Demo Balance",
        "",
        "Send the new demo balance in USDT.",
        "",
        "Example:",
        "`/setdemobalance 100`",
      ].join("\n");
    case "trade_balance":
      return [
        "💵 Set Trade Balance",
        "",
        "Send the target trade balance in USDT.",
        "",
        "Example:",
        "`/settradebalance 100`",
      ].join("\n");
    case "leverage":
      return [
        "⚡ Set Leverage",
        "",
        "Send the leverage multiplier.",
        "",
        "Example:",
        "`/setleverage 10`",
      ].join("\n");
    case "add_pair":
      return [
        "➕ Add Pair",
        "",
        "Send the pair in uppercase USDT format.",
        "",
        "Example:",
        "`/addpair BTCUSDT`",
      ].join("\n");
    case "remove_pair":
      return [
        "➖ Remove Pair",
        "",
        "Send the pair to remove.",
        "",
        "Example:",
        "`/removepair BTCUSDT`",
      ].join("\n");
    case "pair_status":
      return [
        "🔍 Pair Status",
        "",
        "Send the pair you want to inspect.",
        "",
        "Example:",
        "`/pairstatus BTCUSDT`",
      ].join("\n");
    case "wallet":
      return [
        "👛 Set Wallet",
        "",
        "Send the master wallet address.",
        "",
        "Example:",
        "`/setwallet 0x...`",
      ].join("\n");
    case "agent_address":
      return [
        "🤖 Set Agent Address",
        "",
        "Send the Hyperliquid agent or API wallet address.",
        "",
        "Example:",
        "`/setagentaddress 0x...`",
      ].join("\n");
    case "agent_secret":
      return [
        "🔐 Set Agent Secret",
        "",
        "Send the agent signing secret only in a private chat with the bot.",
        "",
        "Example:",
        "`/setagentprivatekey <secret>`",
      ].join("\n");
    case "strategy_cap":
      return [
        "⚙️ Set Strategy Cap",
        "",
        "Send the maximum number of retained strategies.",
        "",
        "Example:",
        "`/setstrategycap 500`",
      ].join("\n");
    case "strategy_retention":
      return [
        "📅 Set Strategy Retention",
        "",
        "Send the retention window in days.",
        "",
        "Example:",
        "`/setstrategyretentiondays 7`",
      ].join("\n");
    case "dryrun_pair":
      return [
        "🪙 Choose Dry Run Pair",
        "",
        "Send the pair to use for dry-run testing.",
        "",
        "Example:",
        "`/dryrunpair BTCUSDT long`",
      ].join("\n");
    case "view_user_config":
      return [
        "👁 View User Config",
        "",
        "Send the target user ID, profile ID, or wallet address.",
        "",
        "Example:",
        "`/viewuserconfig 123456789`",
      ].join("\n");
    case "force_capital_mode":
      return [
        "💰 Force Capital Mode",
        "",
        "Send `<user> <simple|compounding>`.",
        "",
        "Example:",
        "`/forcesetcapitalmode 123456789 simple`",
      ].join("\n");
    case "force_execution_mode":
      return [
        "🧪 Force Execution Mode",
        "",
        "Send `<user> <real|demo>`.",
        "",
        "Example:",
        "`/forcesetexecutionmode 123456789 demo`",
      ].join("\n");
    case "reconcile_user":
      return [
        "🔄 Reconcile User",
        "",
        "Send the target user ID, profile ID, or wallet address.",
        "",
        "Example:",
        "`/reconcile user 123456789`",
      ].join("\n");
    default:
      return "Send the requested value.";
  }
}

async function sendPrompt(bot, target, profile, kind, backScreen = SCREEN.HOME, edit = false) {
  setPendingPrompt(profile.id, { kind, backScreen });
  const html = buildPromptText(kind)
    .split("\n")
    .map((line) => {
      if (line.startsWith("`") && line.endsWith("`")) {
        return `<code>${htmlEscape(line.slice(1, -1))}</code>`;
      }
      return htmlEscape(line);
    })
    .join("\n");
  const extra = inlineKeyboard([
    [button("⬅️ Back", screenCallback(backScreen))],
  ]);

  if (edit && target.message) {
    try {
      await target.bot.editMessageText(html, {
        chat_id: target.message.chat.id,
        message_id: target.message.message_id,
        parse_mode: "HTML",
        ...extra,
      });
      armEphemeralDelete(target.bot, target.message.chat.id, target.message.message_id);
      await answerCallbackSafe(target.bot, target.id);
      return;
    } catch (error) {
      if (!isBenignEditError(error)) {
        console.error("Prompt edit failed:", error.message);
      }
    }
  }

  await sendHtml(target.bot || bot, target.chat?.id || target.message?.chat.id, html, extra, {
    userId: target.userId,
  });
}

async function sendInvalidInput(
  bot,
  chatId,
  reason,
  examples,
  { userId = null, retryPrompt = null, backScreen = SCREEN.HOME } = {}
) {
  const rows = [];
  if (retryPrompt) {
    rows.push([
      button("🔁 Try Again", promptCallback(retryPrompt.kind, retryPrompt.backScreen || backScreen)),
      button("⬅️ Back", screenCallback(backScreen)),
    ]);
  } else {
    rows.push([button("⬅️ Back", screenCallback(backScreen))]);
  }

  const html = [
    "❌ Invalid input.",
    "",
    `Reason: ${htmlEscape(reason)}`,
    "",
    "Use:",
    ...examples.map((example) => `<code>${htmlEscape(example)}</code>`),
  ].join("\n");

  return sendHtml(bot, chatId, html, inlineKeyboard(rows), { userId });
}

async function answerCallbackSafe(bot, callbackQueryId, text = null) {
  if (!callbackQueryId) return;
  try {
    await bot.answerCallbackQuery(callbackQueryId, text ? { text } : {});
  } catch (error) {
    // Best effort only.
  }
}

function describeTrade(trade) {
  return [
    `${trade.pair} ${trade.side}`,
    `[${trade.executionMode}/${trade.capitalMode}]`,
    `${trade.status}`,
    `entry=${formatNumber(trade.entryPrice)}`,
    `tp=${formatNumber(trade.targetPrice)}`,
    `sl=${formatNumber(trade.stopLoss)}`,
    `net=${formatNumber(trade.netPnl || trade.realizedPnl || 0)}`,
  ].join(" | ");
}

function formatPnlText(summary, label = "Overall") {
  return [
    `📈 PnL Dashboard${label ? ` - ${label}` : ""}`,
    "",
    `• Trades: ${summary.summary.totalTrades}`,
    `• Wins: ${summary.summary.wins}`,
    `• Losses: ${summary.summary.losses}`,
    `• Win Rate: ${formatPercent(summary.summary.winRate)}`,
    `• Gross PnL: ${formatNumber(summary.summary.grossPnl)}`,
    `• Entry Fees + Exit Fees: ${formatNumber(summary.summary.fees)}`,
    `• Funding Impact: ${formatNumber(summary.summary.funding)}`,
    `• Net PnL: ${formatNumber(summary.summary.netPnl)}`,
    `• Average PnL: ${formatNumber(summary.summary.avgPnl)}`,
    `• Max Drawdown: ${formatNumber(summary.summary.drawdown)}`,
    `• Best Strategy: ${summary.summary.bestStrategyBucket || "N/A"}`,
  ].join("\n");
}

function formatReconcileText(summary) {
  return [
    "✅ Reconciliation Complete",
    "",
    "Checked:",
    `• Trades: ${summary.reconciledTrades || 0}`,
    `• Positions: ${summary.positionCount || 0}`,
    `• Orders: ${summary.orderCount || 0}`,
    `• Fills: ${summary.fillCount || 0}`,
    "",
    "Fixed:",
    `• Missing TP/SL Repaired: ${summary.missingProtectionsRepaired || 0}`,
    `• Stale Orders Canceled: ${summary.staleOrdersCanceled || 0}`,
    `• Local Records Updated: ${summary.localRecordsUpdated || 0}`,
    "",
    "Unresolved:",
    `• Issues: ${(summary.unresolved || []).length}`,
    "",
    `Last Run: ${formatTimestamp(summary.lastRun || summary.createdAt || state.nowIso())}`,
  ].join("\n");
}

function extractApprovalState(profile) {
  if (profile.automationStatus === "APPROVED") return "approved";
  if (profile.automationStatus === "PENDING") return "pending";
  if (profile.automationStatus === "REJECTED") return "rejected";
  if (profile.status === "DISABLED" || !profile.walletEnabled) return "disabled";
  return "not configured";
}

async function buildDashboard(profile) {
  const capitalStatus = await tradeManager.getCapitalStatus(profile.id);
  const runtime = capitalStatus.runtime;
  const capital = capitalStatus.capitalSnapshot || {};
  const pnl = capitalStatus.pnl || tradeManager.pnlSummary({ profileId: profile.id });
  const strategyStatus = strategyMaintenance.getStrategyStatus(profile.id);
  const watchedPairs = state.getWatchedPairs(profile.id);
  const openTrades = tradeManager.loadOpenPositions(profile.id);
  const activeTrades = openTrades.filter((trade) => isBlockingTrade(trade));
  const pendingOrderCount = openTrades.filter((trade) => isPendingTrade(trade)).length;
  const protectedTradeCount = openTrades.filter((trade) => isProtectedTrade(trade)).length;
  const metadata = await pairUniverse.getMetadata().catch(() => state.getSnapshot("hyperliquidMetaCache", null));
  const lastScan = state.getSnapshot(`profile:${profile.id}:lastScan`, null) || state.getSnapshot("system:lastScan", null);
  const lastScanResult =
    state.getSnapshot(`profile:${profile.id}:lastScanResult`, null) ||
    state.getSnapshot("system:lastScanResult", null);
  const lastSignal = state.getSnapshot(`profile:${profile.id}:lastSignal`, null);
  const lastError =
    state.getSnapshot(`profile:${profile.id}:lastError`, null) ||
    state.getSnapshot("system:lastError", null);
  const lastReconcile = capitalStatus.lastReconcile || state.getRecentReconciliation(profile.id);
  const approvalState = extractApprovalState(profile);
  const dryRunSelection = getDryRunSelection(profile.id);

  return {
    profile,
    runtime,
    capital,
    pnl,
    strategyStatus,
    watchedPairs,
    openTrades,
    activeTradeCount: activeTrades.length,
    pendingOrderCount,
    protectedTradeCount,
    metadata,
    metadataStatus: metadata?.pairs?.length ? "READY" : "UNAVAILABLE",
    lastScan,
    lastScanResult,
    lastSignal,
    lastError,
    lastReconcile,
    approvalState,
    dryRunSelection,
  };
}

function buildModeSwitchBlockedScreen(profile, lockState) {
  return {
    text: [
      "⚠️ Mode Switch Blocked",
      "",
      "Mode cannot be changed while trades are active, pending, partially filled, protected, or reconciling.",
      "",
      "Close or reconcile active trades first.",
      "",
      `Profile: ${profileLabel(profile)}`,
      `Blocking Trades: ${(lockState.trades || []).length}`,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("📍 View Active Trades", screenCallback(SCREEN.POSITIONS)), button("🔄 Reconcile", screenCallback(SCREEN.RECONCILE))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildHomeScreen(data) {
  return {
    text: [
      "📍 Trading Control Panel",
      "",
      "Manage signals, trades, pairs, modes, balance, strategies, and automation from one place.",
      "",
      "Current Overview:",
      `• Bot: running`,
      `• AutoTrade: ${data.runtime.autoTradeEnabled ? "ON" : "OFF"}`,
      `• Capital Mode: ${data.runtime.capitalMode}`,
      `• Execution Mode: ${data.runtime.executionMode}`,
      `• Active Trades: ${data.activeTradeCount}`,
      `• Pairs: ${data.watchedPairs.length}`,
      `• Approval: ${data.approvalState}`,
      "",
      "Choose an option below.",
    ].join("\n"),
    extra: inlineKeyboard([
      [button("📊 Status", screenCallback(SCREEN.STATUS)), button("⚙️ Trading", screenCallback(SCREEN.TRADING)), button("🪙 Pairs", screenCallback(SCREEN.PAIRS))],
      [button("📈 PnL", screenCallback(SCREEN.PNL)), button("📍 Positions", screenCallback(SCREEN.POSITIONS)), button("❓ Help", screenCallback(SCREEN.HELP))],
    ]),
  };
}

function buildStatusScreen(data) {
  const selectedPrincipal = data.capital.selectedPrincipal ?? data.runtime.baselinePrincipal;
  const availableBalance = data.capital.availableTradableBalance ?? data.capital.accountValue;
  const tradePrincipal = data.capital.tradePrincipal ?? data.capital.currentPrincipal;

  return {
    text: [
      "📊 Bot Status",
      "",
      "System:",
      `• Bot State: running`,
      `• Last Scan: ${formatTimestamp(data.lastScan)}`,
      `• Last Signal: ${formatTimestamp(data.lastSignal)}`,
      `• Last Error: ${shortText(data.lastError?.message || "none")}`,
      "",
      "Trading:",
      `• AutoTrade: ${data.runtime.autoTradeEnabled ? "ON" : "OFF"}`,
      `• Capital Mode: ${data.runtime.capitalMode}`,
      `• Execution Mode: ${data.runtime.executionMode}`,
      `• Leverage: ${data.runtime.tradeLeverage}x`,
      `• Active Trades: ${data.activeTradeCount}`,
      `• Pending Orders: ${data.pendingOrderCount}`,
      "",
      data.runtime.capitalMode === "SIMPLE" ? "Simple Mode:" : "Compounding Mode:",
      `• Selected Principal: ${formatNumber(selectedPrincipal)} USDT`,
      `• Available Balance: ${formatNumber(availableBalance)} USDT`,
      `• Trade Principal: ${formatNumber(tradePrincipal)} USDT`,
      `• Slots: ${data.capital.simpleSlots}`,
      `• Slot Size: ${formatNumber(data.capital.slotSize)} USDT`,
      "",
      "Pairs:",
      `• Active Pairs: ${data.watchedPairs.length}`,
      `• Metadata Source: Hyperliquid`,
      `• Metadata Status: ${data.metadataStatus}`,
      "",
      "Automation:",
      `• Approval: ${data.approvalState}`,
      `• Wallet: ${maskAddress(data.profile.masterWalletAddress)}`,
      `• Agent Wallet: ${maskAddress(data.profile.agentWalletAddress)}`,
      "",
      "Formula:",
      "Trade Principal = min(Selected Principal, Available Balance)",
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🔄 Refresh", screenCallback(SCREEN.STATUS)), button("⚙️ Trading", screenCallback(SCREEN.TRADING)), button("🧪 Demo/Real", screenCallback(SCREEN.EXECUTION))],
      [button("📍 Positions", screenCallback(SCREEN.POSITIONS)), button("📈 PnL", screenCallback(SCREEN.PNL)), button("💼 Balance", screenCallback(SCREEN.BALANCE))],
      [button("🪙 Pairs", screenCallback(SCREEN.PAIRS)), button("❓ Help", screenCallback(SCREEN.HELP)), button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildTradingScreen(data) {
  return {
    text: [
      "⚙️ Trading Settings",
      "",
      "Control AutoTrade, capital mode, principal, slots, leverage, and dry-run testing.",
      "",
      "Current:",
      `• AutoTrade: ${data.runtime.autoTradeEnabled ? "ON" : "OFF"}`,
      `• Capital Mode: ${data.runtime.capitalMode}`,
      `• Execution Mode: ${data.runtime.executionMode}`,
      `• Selected Principal: ${formatNumber(data.capital.selectedPrincipal)} USDT`,
      `• Trade Principal: ${formatNumber(data.capital.tradePrincipal)} USDT`,
      `• Slots: ${data.capital.simpleSlots}`,
      `• Slot Size: ${formatNumber(data.capital.slotSize)} USDT`,
      `• Leverage: ${data.runtime.tradeLeverage}x`,
      `• Active Trades: ${data.activeTradeCount}`,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🟢 Enable AutoTrade", "set:auto:on"), button("🔴 Disable AutoTrade", "set:auto:off")],
      [button("💰 Capital Mode", screenCallback(SCREEN.CAPITAL)), button("🧪 Execution Mode", screenCallback(SCREEN.EXECUTION))],
      [button("✏️ Set Principal", promptCallback("principal", SCREEN.TRADING)), button("🔢 Set Slots", promptCallback("slots", SCREEN.TRADING))],
      [button("⚡ Set Leverage", promptCallback("leverage", SCREEN.TRADING)), button("💵 Trade Balance", promptCallback("trade_balance", SCREEN.TRADING))],
      [button("🧪 Dry Run Long", "dry:long"), button("🧪 Dry Run Short", "dry:short")],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildCapitalScreen(data) {
  return {
    text: [
      "💰 Capital Mode",
      "",
      `Current Mode: ${data.runtime.capitalMode}`,
      "",
      "SIMPLE:",
      "Trades with maximum up to your selected principal.",
      "If wallet balance is above selected principal, only selected principal is used.",
      "If wallet balance is below selected principal, available balance is used.",
      "If wallet balance later recovers above selected principal, selected principal is used again.",
      "Profit stays in wallet but does not increase trade size above selected principal.",
      "Profit always remains in the trading balance.",
      "No transfer to spot is performed.",
      "",
      "COMPOUNDING:",
      "Uses full available trading balance for the next trade.",
      "Profit increases next trade size.",
      "Loss decreases next trade size.",
      "Only one active trade is allowed.",
      "",
      "Current:",
      `• Selected Principal: ${formatNumber(data.capital.selectedPrincipal)} USDT`,
      `• Available Balance: ${formatNumber(data.capital.availableTradableBalance)} USDT`,
      `• Trade Principal: ${formatNumber(data.capital.tradePrincipal)} USDT`,
      `• Active Trades: ${data.activeTradeCount}`,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("✅ Simple Mode", "set:capital:simple"), button("📈 Compounding Mode", "set:capital:compounding")],
      [button("✏️ Set Principal", promptCallback("principal", SCREEN.CAPITAL)), button("🔢 Set Slots", promptCallback("slots", SCREEN.CAPITAL))],
      [button("📊 Capital Status", screenCallback(SCREEN.STATUS))],
      [button("⬅️ Back", screenCallback(SCREEN.TRADING))],
    ]),
  };
}

function buildExecutionScreen(data) {
  return {
    text: [
      "🧪 Execution Mode",
      "",
      `Current Mode: ${data.runtime.executionMode}`,
      "",
      "REAL:",
      "Uses actual Hyperliquid wallet balance and places real orders.",
      "Requires approved automation.",
      "",
      "DEMO:",
      "Paper trading only.",
      "No real orders are placed.",
      "Default demo balance is 100 USDT unless changed.",
      "",
      "Current:",
      `• Real Approval: ${data.approvalState}`,
      `• Demo Balance: ${formatNumber(data.runtime.demoPerpBalance)} USDT`,
      `• Active Trades: ${data.activeTradeCount}`,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🔴 Real Mode", "set:execution:real"), button("🟢 Demo Mode", "set:execution:demo")],
      [button("✏️ Set Demo Balance", promptCallback("demo_balance", SCREEN.EXECUTION)), button("📊 Execution Status", screenCallback(SCREEN.STATUS))],
      [button("⬅️ Back", screenCallback(SCREEN.TRADING))],
    ]),
  };
}

function buildPairsScreen(data) {
  const list = data.watchedPairs.length ? data.watchedPairs.slice(0, 20).join(", ") : "None";
  return {
    text: [
      "🪙 Pair Management",
      "",
      `Active Pairs: ${data.watchedPairs.length}`,
      `Metadata Source: Hyperliquid`,
      `Last Metadata Refresh: ${formatTimestamp(data.metadata?.fetchedAt || data.metadata)}`,
      "",
      "Pairs:",
      list,
      "",
      "Use this menu to add, remove, or refresh tradable pairs.",
    ].join("\n"),
    extra: inlineKeyboard([
      [button("📋 View Pairs", screenCallback(SCREEN.PAIRS)), button("➕ Add Pair", promptCallback("add_pair", SCREEN.PAIRS)), button("➖ Remove Pair", promptCallback("remove_pair", SCREEN.PAIRS))],
      [button("🔄 Reload Metadata", "pair:reload"), button("🔍 Pair Status", promptCallback("pair_status", SCREEN.PAIRS))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildPositionsScreen(data) {
  const positionLines = data.openTrades.length
    ? data.openTrades.slice(0, 12).map((trade) => `• ${describeTrade(trade)}`)
    : ["No open or pending trades."];

  return {
    text: [
      "📍 Positions",
      "",
      `Open Positions: ${data.activeTradeCount}`,
      `Pending Orders: ${data.pendingOrderCount}`,
      `Protected Trades: ${data.protectedTradeCount}`,
      "",
      ...positionLines,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("📍 Open Positions", screenCallback(SCREEN.POSITIONS)), button("⏳ Pending Orders", screenCallback(SCREEN.POSITIONS))],
      [button("🛡️ Protected Trades", screenCallback(SCREEN.POSITIONS)), button("🔄 Reconcile", screenCallback(SCREEN.RECONCILE))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildClosedScreen(profile) {
  const closedTrades = tradeManager.loadClosedTrades(profile.id, 20);
  const lines = closedTrades.length
    ? closedTrades.map((trade) => `• ${describeTrade(trade)}`)
    : ["No closed trades yet."];
  return {
    text: ["✅ Closed Trades", "", ...lines].join("\n"),
    extra: inlineKeyboard([
      [button("🔄 Refresh", screenCallback(SCREEN.CLOSED))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildPnlScreen(profile, filters = {}) {
  const summary = tradeManager.pnlSummary({
    profileId: profile.id,
    executionMode: filters.executionMode || undefined,
    capitalMode: filters.capitalMode || undefined,
  });
  const labelParts = [];
  if (filters.executionMode) labelParts.push(filters.executionMode);
  if (filters.capitalMode) labelParts.push(filters.capitalMode);
  const label = labelParts.length ? labelParts.join(" + ") : "Overall";

  return {
    text: formatPnlText(summary, label),
    extra: inlineKeyboard([
      [button("📊 Overall", pnlCallback("all", "all")), button("💰 Simple", pnlCallback("all", "simple")), button("📈 Compounding", pnlCallback("all", "compounding"))],
      [button("🔴 Real", pnlCallback("real", "all")), button("🟢 Demo", pnlCallback("demo", "all"))],
      [button("🔴 Real + Simple", pnlCallback("real", "simple")), button("🔴 Real + Compounding", pnlCallback("real", "compounding"))],
      [button("🟢 Demo + Simple", pnlCallback("demo", "simple")), button("🟢 Demo + Compounding", pnlCallback("demo", "compounding"))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildBalanceScreen(data) {
  return {
    text: [
      "💼 Balance",
      "",
      `Execution Mode: ${data.runtime.executionMode}`,
      "",
      "Real Wallet:",
      `• Perp Equity: ${formatNumber(data.capital.accountValue)}`,
      `• Available Margin: ${formatNumber(data.capital.withdrawable)}`,
      `• Withdrawable: ${formatNumber(data.capital.withdrawable)}`,
      "",
      "Demo Wallet:",
      `• Demo Balance: ${formatNumber(data.runtime.demoPerpBalance)}`,
      `• Demo Equity: ${formatNumber(data.runtime.demoPerpBalance)}`,
      `• Demo Net PnL: ${formatNumber(data.pnl.summary.netPnl)}`,
      "",
      "Simple Mode:",
      `• Selected Principal: ${formatNumber(data.capital.selectedPrincipal)}`,
      `• Available Balance: ${formatNumber(data.capital.availableTradableBalance)}`,
      `• Trade Principal: ${formatNumber(data.capital.tradePrincipal)}`,
      `• Slots: ${data.capital.simpleSlots}`,
      `• Slot Size: ${formatNumber(data.capital.slotSize)}`,
      "",
      "Note:",
      "In all modes, profit remains in the trading balance.",
      "The bot only limits next trade sizing to maximum selected principal.",
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🔄 Refresh", screenCallback(SCREEN.BALANCE)), button("📊 Status", screenCallback(SCREEN.STATUS)), button("📈 PnL", screenCallback(SCREEN.PNL))],
      [button("✏️ Set Principal", promptCallback("principal", SCREEN.BALANCE)), button("✏️ Set Demo Balance", promptCallback("demo_balance", SCREEN.BALANCE))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildScanScreen(data) {
  const lastScanSummary = data.lastScanResult || {};
  return {
    text: [
      "🔎 Manual Scan",
      "",
      "Current:",
      `• Active Pairs: ${data.watchedPairs.length}`,
      `• Base TFs: 1m, 5m`,
      `• Capital Mode: ${data.runtime.capitalMode}`,
      `• Execution Mode: ${data.runtime.executionMode}`,
      `• AutoTrade: ${data.runtime.autoTradeEnabled ? "ON" : "OFF"}`,
      "",
      "Last Scan Result:",
      `• Time: ${formatTimestamp(data.lastScan)}`,
      `• Candidates: ${lastScanSummary.candidates || 0}`,
      `• Universe: ${lastScanSummary.scannedUniverse || 0}`,
      `• Error: ${shortText(lastScanSummary.error || "none")}`,
      "",
      "Choose scan type.",
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🔎 Scan Now", "run:scan"), button("🧪 Dry Run Long", "dry:long"), button("🧪 Dry Run Short", "dry:short")],
      [button("🪙 Select Pair", promptCallback("dryrun_pair", SCREEN.SCAN)), button("📊 Last Scan Result", screenCallback(SCREEN.SCAN))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

async function buildDryRunPreview(profile, side, pair = null) {
  const selectedPair =
    String(pair || getDryRunSelection(profile.id).pair || state.getWatchedPairs(profile.id)[0] || "BTCUSDT")
      .trim()
      .toUpperCase();
  const validation = await pairUniverse.validatePair(selectedPair);
  if (!validation.ok) {
    return {
      ok: false,
      reason: validation.reason,
      pair: selectedPair,
    };
  }

  const mids = await require("./hyperliquid").getAllMids();
  const entry = Number(mids?.[validation.coin] || 0);
  if (!entry) {
    return {
      ok: false,
      reason: "missing-market-price",
      pair: validation.pair,
      coin: validation.coin,
    };
  }

  const isLong = String(side || "long").toLowerCase() === "long";
  const target = isLong ? entry * 1.006 : entry * 0.994;
  const stop = isLong ? entry * 0.994 : entry * 1.006;
  const capitalStatus = await tradeManager.getCapitalStatus(profile.id);
  const runtime = capitalStatus.runtime;
  const capital = capitalStatus.capitalSnapshot;
  const principalBase =
    runtime.capitalMode === "SIMPLE" ? Number(capital.slotSize || 0) : Number(capital.currentPrincipal || 0);
  const principalUsed = Math.min(principalBase, Number(capital.withdrawable || principalBase || 0));
  const leverage = Number(runtime.tradeLeverage || config.defaultTradeLeverage || 10);
  const notional = principalUsed * leverage;
  const quantity = entry > 0 ? notional / entry : 0;
  const grossPnl = isLong ? (target - entry) * quantity : (entry - target) * quantity;
  const fees = notional * Number(config.demoFeeRate || 0) * 2;
  const funding = notional * Number(config.demoFundingRatePerHour || 0) * (isLong ? -1 : 1);
  const netPnl = grossPnl - fees + funding;
  const strategy = getStrategyByPair(validation.pair)[0];
  const strategySummary = strategy
    ? `${strategy.direction || "N/A"} | ${strategy.mainSourceTimeframe || "N/A"} | net=${formatNumber(strategy.netPnl || 0)}`
    : "Matched live scan rules";

  return {
    ok: true,
    pair: validation.pair,
    coin: validation.coin,
    side: isLong ? "LONG" : "SHORT",
    baseTf: "1m/5m",
    entry,
    tp: target,
    sl: stop,
    leverage,
    principalUsed,
    grossPnl,
    fees,
    funding,
    netPnl,
    strategySummary,
    link: validation.link,
  };
}

async function buildDryRunScreen(profile, preview = null) {
  const selectedPair = getDryRunSelection(profile.id).pair || state.getWatchedPairs(profile.id)[0] || "BTCUSDT";
  const intro = [
    "🧪 Dry Run Test",
    "",
    "Mode: DEMO TEST",
    `Pair: ${selectedPair}`,
    "",
    "No real order will be placed.",
    "",
    "The bot will simulate:",
    "• signal",
    "• entry",
    "• TP",
    "• SL",
    "• fees",
    "• funding",
    "• net PnL impact",
  ];

  let previewLines = [];
  if (preview?.ok) {
    previewLines = [
      "",
      "🧪 Dry Run Result",
      `Pair: ${preview.pair}`,
      `🌐 Hyperliquid: ${preview.link}`,
      `Side: ${preview.side}`,
      `Base TF: ${preview.baseTf}`,
      `Entry: ${formatNumber(preview.entry)}`,
      `TP: ${formatNumber(preview.tp)}`,
      `SL: ${formatNumber(preview.sl)}`,
      `Leverage: ${preview.leverage}x`,
      "",
      "Simulated:",
      `• Principal Used: ${formatNumber(preview.principalUsed)}`,
      `• Gross PnL: ${formatNumber(preview.grossPnl)}`,
      `• Fees: ${formatNumber(preview.fees)}`,
      `• Funding: ${formatNumber(preview.funding)}`,
      `• Net PnL: ${formatNumber(preview.netPnl)}`,
      "",
      "Strategy:",
      preview.strategySummary,
    ];
  } else if (preview && !preview.ok) {
    previewLines = ["", `Preview unavailable: ${preview.reason}`];
  }

  return {
    text: [...intro, ...previewLines].join("\n"),
    extra: inlineKeyboard([
      [button("▶️ Run Long Test", "dry:long"), button("▶️ Run Short Test", "dry:short")],
      [button("🪙 Choose Pair", promptCallback("dryrun_pair", SCREEN.DRYRUN)), preview?.link ? urlButton("🌐 Open Hyperliquid", preview.link) : button("⬅️ Back", screenCallback(SCREEN.SCAN))],
      [button("⬅️ Back", screenCallback(SCREEN.SCAN))],
    ]),
  };
}

function buildAutomationScreen(data) {
  return {
    text: [
      "🛡️ Automation",
      "",
      `Status: ${data.approvalState}`,
      `Real Trading: ${data.profile.automationStatus === "APPROVED" ? "allowed" : "blocked"}`,
      `Wallet: ${maskAddress(data.profile.masterWalletAddress)}`,
      `Agent Wallet: ${maskAddress(data.profile.agentWalletAddress)}`,
      `Secret: ${data.profile.agentSecretFingerprint ? "configured" : "not configured"}`,
      "",
      "To enable REAL automation:",
      "1. Set wallet address",
      "2. Set Hyperliquid agent/API wallet",
      "3. Submit approval request",
      "4. Admin approval required",
      "",
      "Security rules:",
      "- Do not collect main wallet private keys.",
      "- Only use dedicated Hyperliquid agent/API wallet secret.",
      "- Store secrets encrypted.",
      "- Never show full secret again.",
      "- Admin sees only masked fingerprint, not the raw secret.",
      "- REAL mode remains blocked until approval.",
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🔗 Connect Trading", "auto:connect"), button("👛 Set Wallet", promptCallback("wallet", SCREEN.AUTOMATION))],
      [button("🤖 Set Agent Address", promptCallback("agent_address", SCREEN.AUTOMATION)), button("🔐 Set Agent Secret", promptCallback("agent_secret", SCREEN.AUTOMATION))],
      [button("📨 Submit Approval", "auto:submit"), button("📊 Automation Status", screenCallback(SCREEN.AUTOMATION))],
      [button("🟢 Enable", "auto:enable"), button("🔴 Disable", "auto:disable")],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildReconcileScreen(data, summary = null) {
  const lines = [
    "🔄 Reconciliation",
    "",
    "This compares local bot state with Hyperliquid exchange state.",
    "",
    "It checks:",
    "• open positions",
    "• pending orders",
    "• fills",
    "• TP orders",
    "• SL orders",
    "• local trade records",
  ];
  if (summary) {
    lines.push("", formatReconcileText(summary));
  }
  return {
    text: lines.join("\n"),
    extra: inlineKeyboard([
      [button("▶️ Run Reconcile", "recon:self")],
      [button("📍 Reconcile Positions", "recon:self"), button("⏳ Reconcile Orders", "recon:self")],
      [button("🛡️ Reconcile Protections", "recon:self"), button("📋 Last Result", screenCallback(SCREEN.RECONCILE))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildStrategiesScreen(data) {
  const status = data.strategyStatus;
  return {
    text: [
      "🧠 Strategy Manager",
      "",
      `Stored Strategies: ${status.totalStrategies}`,
      `Active Strategies: ${status.totalStrategies}`,
      `Strategy Cap: ${status.strategyCap}`,
      `Retention Days: ${status.strategyRetentionDays}`,
      `Last Weekly Prune: ${status.lastStrategyPruneAt || "never"}`,
      "",
      `Weekly rule:`,
      `The bot keeps only the top ${status.strategyCap} strategies based on accuracy, net PnL, sample count, drawdown, and recency.`,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("🏆 Top Strategies", "str:top"), button("🧹 Run Prune", "str:prune")],
      [button("📤 Export", "str:export"), button("📥 Import Latest", "str:import_latest")],
      [button("📁 Export Files", "str:exports"), button("⚙️ Set Cap", promptCallback("strategy_cap", SCREEN.STRATEGIES))],
      [button("📅 Set Retention", promptCallback("strategy_retention", SCREEN.STRATEGIES))],
      [button("📊 Strategy Stats", screenCallback(SCREEN.STRATEGIES))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildAdminScreen() {
  const pending = state.listAutomationRequests("PENDING");
  const profiles = state.listProfiles({ includeDisabled: true });
  const approvedUsers = profiles.filter((profile) => profile.automationStatus === "APPROVED");
  const disabledUsers = profiles.filter((profile) => profile.status === "DISABLED" || !profile.walletEnabled);
  const activeRealTraders = profiles.filter(
    (profile) =>
      profile.automationStatus === "APPROVED" &&
      profile.automationEnabled &&
      state.getRuntimeSettings(profile.id).executionMode === "REAL"
  );

  return {
    text: [
      "👑 Admin Panel",
      "",
      "Manage users, approvals, automation, reconciliation, and forced mode controls.",
      "",
      `Pending Approvals: ${pending.length}`,
      `Approved Users: ${approvedUsers.length}`,
      `Disabled Users: ${disabledUsers.length}`,
      `Active Real Traders: ${activeRealTraders.length}`,
    ].join("\n"),
    extra: inlineKeyboard([
      [button("📋 Pending Approvals", screenCallback(SCREEN.APPROVALS)), button("🔄 Reconcile All", "recon:all")],
      [button("👁 View User Config", promptCallback("view_user_config", SCREEN.ADMIN)), button("💰 Force Capital Mode", promptCallback("force_capital_mode", SCREEN.ADMIN))],
      [button("🧪 Force Execution Mode", promptCallback("force_execution_mode", SCREEN.ADMIN))],
      [button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

function buildApprovalsScreen() {
  const pending = state.listAutomationRequests("PENDING").slice(0, 10);
  const textLines = ["📋 Pending Automation Approvals", ""];
  if (!pending.length) {
    textLines.push("No pending approvals.");
  } else {
    for (const request of pending) {
      textLines.push(
        `Request ID: ${request.id}`,
        `User ID: ${request.telegramUserId || "N/A"}`,
        `Username: @${request.username || "unknown"}`,
        `Name: ${request.displayName || request.label || request.profileId}`,
        `Wallet: ${maskAddress(request.masterWalletAddress)}`,
        `Agent Wallet: ${maskAddress(request.agentWalletAddress)}`,
        `Secret Fingerprint: ${request.agentSecretFingerprint || "not-set"}`,
        `Requested At: ${formatTimestamp(request.requestedAt)}`,
        ""
      );
    }
  }

  const rows = pending.flatMap((request) => [
    [
      button(`✅ Approve #${request.id}`, `apr:approve:${request.id}`),
      button(`❌ Reject #${request.id}`, `apr:reject:${request.id}`),
    ],
  ]);
  rows.push([button("⬅️ Back", screenCallback(SCREEN.ADMIN))]);

  return {
    text: textLines.join("\n").trim(),
    extra: inlineKeyboard(rows),
  };
}

function buildHelpScreen() {
  return {
    text: buildHelpText(),
    extra: inlineKeyboard([
      [button("📊 Status", screenCallback(SCREEN.STATUS)), button("⚙️ Trading", screenCallback(SCREEN.TRADING)), button("🪙 Pairs", screenCallback(SCREEN.PAIRS))],
      [button("🛡️ Automation", screenCallback(SCREEN.AUTOMATION)), button("🧠 Strategies", screenCallback(SCREEN.STRATEGIES)), button("⬅️ Back", screenCallback(SCREEN.HOME))],
    ]),
  };
}

async function renderScreen(screen, { profile, filters = {}, dryRunPreview = null, reconcileSummary = null } = {}) {
  const data = await buildDashboard(profile);

  switch (screen) {
    case SCREEN.HOME:
      return buildHomeScreen(data);
    case SCREEN.STATUS:
      return buildStatusScreen(data);
    case SCREEN.TRADING:
      return buildTradingScreen(data);
    case SCREEN.CAPITAL:
      return buildCapitalScreen(data);
    case SCREEN.EXECUTION:
      return buildExecutionScreen(data);
    case SCREEN.PAIRS:
      return buildPairsScreen(data);
    case SCREEN.POSITIONS:
      return buildPositionsScreen(data);
    case SCREEN.CLOSED:
      return buildClosedScreen(profile);
    case SCREEN.PNL:
      return buildPnlScreen(profile, filters);
    case SCREEN.BALANCE:
      return buildBalanceScreen(data);
    case SCREEN.SCAN:
      return buildScanScreen(data);
    case SCREEN.DRYRUN:
      return buildDryRunScreen(profile, dryRunPreview);
    case SCREEN.AUTOMATION:
      return buildAutomationScreen(data);
    case SCREEN.RECONCILE:
      return buildReconcileScreen(data, reconcileSummary);
    case SCREEN.STRATEGIES:
      return buildStrategiesScreen(data);
    case SCREEN.ADMIN:
      return buildAdminScreen();
    case SCREEN.APPROVALS:
      return buildApprovalsScreen();
    case SCREEN.HELP:
      return buildHelpScreen();
    default:
      return buildHomeScreen(data);
  }
}

async function showScreen(bot, target, screen, options = {}) {
  const from = target.from || target.message?.from;
  const chat = target.chat || target.message?.chat;
  const userId = String(from?.id || options.userId || "");
  const profile =
    options.profile || state.getOrCreateProfileFromTelegram(from, chat);
  const payload = await renderScreen(screen, {
    profile,
    filters: options.filters,
    dryRunPreview: options.dryRunPreview || null,
    reconcileSummary: options.reconcileSummary || null,
  });
  const sendOptions = {
    disable_web_page_preview: true,
    ...(payload.extra || {}),
  };

  if (target.id && target.message && options.edit !== false) {
    try {
      await bot.editMessageText(payload.text, {
        chat_id: target.message.chat.id,
        message_id: target.message.message_id,
        ...sendOptions,
      });
      armEphemeralDelete(bot, target.message.chat.id, target.message.message_id);
      await answerCallbackSafe(bot, target.id);
      return;
    } catch (error) {
      if (isBenignEditError(error)) {
        await answerCallbackSafe(bot, target.id);
        if (/message is not modified/i.test(String(error.message || ""))) {
          return;
        }
      } else {
        console.error("Screen edit failed:", error.message);
      }
    }
  }

  await sendMessage(bot, chat.id, payload.text, sendOptions, { userId });
}

async function exportStrategies(bot, chatId, { userId = null } = {}) {
  state.ensureDir(config.exportsDir);
  const beforeCount = state.loadStrategiesIndex().length;
  const fileName = `strategies-${Date.now()}.txt`;
  const filePath = path.join(config.exportsDir, fileName);
  fs.writeFileSync(filePath, state.exportStrategiesText(), "utf8");
  const message = await bot.sendDocument(chatId, filePath, {
    caption: `🗂 Strategy export\nFile: ${fileName}\nStrategies: ${beforeCount}`,
  });
  armEphemeralDeleteForMessage(bot, message);
  await sendMessage(
    bot,
    chatId,
    [
      "✅ Strategy export created",
      `File: ${fileName}`,
      `Strategies: ${beforeCount}`,
      `Path: ${filePath}`,
      "",
      "Use `/importstrategies latest` to restore the newest backup later.",
    ].join("\n"),
    inlineKeyboard([
      [button("📥 Import Latest", "str:import_latest"), button("📁 Export Files", "str:exports")],
      [button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))],
    ]),
    { userId }
  );
  return { fileName, filePath, count: beforeCount };
}

async function importStrategiesFromText(
  bot,
  chatId,
  rawText,
  sourceLabel,
  { userId = null, replace = false } = {}
) {
  const beforeCount = state.loadStrategiesIndex().length;
  const result = state.importStrategiesText(rawText, { replace });
  const afterCount = result.strategies.length;
  const netChange = afterCount - beforeCount;

  await sendMessage(
    bot,
    chatId,
    [
      "✅ Strategies imported",
      `Source: ${sourceLabel}`,
      `Mode: ${replace ? "replace" : "merge"}`,
      `Records Parsed: ${result.imported}`,
      `Stored Before: ${beforeCount}`,
      `Stored After: ${afterCount}`,
      `Net Change: ${netChange >= 0 ? "+" : ""}${netChange}`,
    ].join("\n"),
    inlineKeyboard([
      [button("📊 Strategy Stats", screenCallback(SCREEN.STRATEGIES)), button("📁 Export Files", "str:exports")],
      [button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))],
    ]),
    { userId }
  );

  return result;
}

async function importStrategiesFromFile(
  bot,
  chatId,
  fileRecord,
  { userId = null, replace = false } = {}
) {
  if (!fileRecord?.path || !fs.existsSync(fileRecord.path)) {
    await sendMessage(
      bot,
      chatId,
      "No strategy export file found. Use `/exportstrategies` first.",
      inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]]),
      { userId }
    );
    return null;
  }

  const rawText = fs.readFileSync(fileRecord.path, "utf8");
  return importStrategiesFromText(bot, chatId, rawText, fileRecord.name, { userId, replace });
}

async function importStrategiesFromExternalPath(
  bot,
  chatId,
  rawFilePath,
  { userId = null, replace = false } = {}
) {
  const normalizedPath = path.resolve(stripWrappedQuotes(rawFilePath || ""));
  if (!normalizedPath || !fs.existsSync(normalizedPath) || !fs.statSync(normalizedPath).isFile()) {
    await sendInvalidInput(
      bot,
      chatId,
      "External strategy file not found.",
      ["/importstrategyfile /abs/path/file.json", "/importstrategyfile replace /abs/path/file.txt"],
      { userId, backScreen: SCREEN.STRATEGIES }
    );
    return null;
  }

  const rawText = fs.readFileSync(normalizedPath, "utf8");
  return importStrategiesFromText(bot, chatId, rawText, normalizedPath, { userId, replace });
}

async function listStrategyExports(bot, chatId, { userId = null } = {}) {
  const exports = listStrategyExportFiles(10);
  if (!exports.length) {
    await sendMessage(
      bot,
      chatId,
      "No strategy exports found yet. Use `/exportstrategies` to create one.",
      inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]]),
      { userId }
    );
    return [];
  }

  await sendMessage(
    bot,
    chatId,
    [
      "📁 Strategy Exports",
      "",
      ...exports.map(
        (item, index) =>
          `${index + 1}. ${item.name} | ${formatTimestamp(item.modifiedAt)} | ${formatFileSize(item.sizeBytes)}`
      ),
      "",
      "Import the latest backup with `/importstrategies latest`.",
      "Import a specific file with `/importstrategies strategies-1234567890.txt`.",
    ].join("\n"),
    inlineKeyboard([
      [button("📥 Import Latest", "str:import_latest"), button("📤 Export", "str:export")],
      [button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))],
    ]),
    { userId }
  );
  return exports;
}

async function importStrategiesFromDocument(bot, chatId, document, { userId = null, replace = false } = {}) {
  if (!document) {
    await sendMessage(
      bot,
      chatId,
      "Reply to a .txt export with `/importstrategies`, send a .txt file with caption `/importstrategies`, or use `/importstrategies latest`.",
      inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]]),
      { userId }
    );
    return;
  }

  const fileUrl = await bot.getFileLink(document.file_id);
  const response = await axios.get(fileUrl, {
    responseType: "text",
    timeout: 25_000,
  });
  return importStrategiesFromText(
    bot,
    chatId,
    response.data,
    document.file_name || "uploaded file",
    { userId, replace }
  );
}

async function notifyAdmins(bot, text) {
  for (const adminId of config.telegramAdminIds) {
    try {
      await sendMessage(bot, adminId, text, {}, { userId: adminId });
    } catch (error) {
      console.error(`Admin notify failed ${adminId}:`, error.message);
    }
  }
}

function buildAutomationSummary(profile) {
  return [
    `Status: ${profile.automationStatus || "NOT_CONFIGURED"}`,
    `Enabled: ${profile.automationEnabled ? "YES" : "NO"}`,
    `Wallet Enabled: ${profile.walletEnabled ? "YES" : "NO"}`,
    `Master Wallet: ${maskAddress(profile.masterWalletAddress)}`,
    `Agent Wallet: ${maskAddress(profile.agentWalletAddress)}`,
    `Secret Fingerprint: ${profile.agentSecretFingerprint || "not-set"}`,
  ].join("\n");
}

async function maybeDeleteSecretMessage(bot, msg) {
  try {
    if (msg.chat?.type === "private") {
      await bot.deleteMessage(msg.chat.id, msg.message_id);
    }
  } catch (error) {
    // Best effort only.
  }
}

async function sendModeBlocked(bot, chatId, profile, lockState, userId) {
  const payload = buildModeSwitchBlockedScreen(profile, lockState);
  await sendMessage(bot, chatId, payload.text, payload.extra, { userId });
}

async function setCapitalModeGuarded(bot, chatId, profile, userId, mode, { force = false, reason = null } = {}) {
  const lockState = state.getModeLockState(profile.id);
  if (lockState.locked && !force) {
    await sendModeBlocked(bot, chatId, profile, lockState, userId);
    return false;
  }
  const before = state.getRuntimeSettings(profile.id);
  state.setCapitalMode(profile.id, mode);
  state.appendAuditLog({
    profileId: profile.id,
    actorUserId: userId,
    action: force ? "force-capital-mode-change" : "capital-mode-change",
    reason,
    oldValue: { capitalMode: before.capitalMode },
    newValue: { capitalMode: String(mode).toUpperCase() },
  });
  return true;
}

async function setExecutionModeGuarded(bot, chatId, profile, userId, mode, { force = false, reason = null } = {}) {
  const targetMode = String(mode || "").toUpperCase();
  const lockState = state.getModeLockState(profile.id);
  if (lockState.locked && !force) {
    await sendModeBlocked(bot, chatId, profile, lockState, userId);
    return false;
  }

  if (targetMode === "REAL" && profile.automationStatus !== "APPROVED" && !force) {
    await sendMessage(
      bot,
      chatId,
      "❌ Real mode cannot be enabled.\n\nReason: automation-not-approved",
      inlineKeyboard([
        [button("🛡️ Automation", screenCallback(SCREEN.AUTOMATION)), button("⬅️ Back", screenCallback(SCREEN.EXECUTION))],
      ]),
      { userId }
    );
    return false;
  }

  const before = state.getRuntimeSettings(profile.id);
  state.setExecutionMode(profile.id, targetMode);
  state.appendAuditLog({
    profileId: profile.id,
    actorUserId: userId,
    action: force ? "force-execution-mode-change" : "execution-mode-change",
    reason,
    oldValue: { executionMode: before.executionMode },
    newValue: { executionMode: targetMode },
  });
  return true;
}

async function runManualScan(bot, target, profile, callbacks, { edit = false } = {}) {
  const summary = await callbacks.runScan({
    manual: true,
    chatId: target.chat?.id || target.message?.chat.id,
    profileId: profile.id,
  });
  await showScreen(bot, target, SCREEN.SCAN, { profile, edit, reconcileSummary: null });
  await sendMessage(
    bot,
    target.chat?.id || target.message?.chat.id,
    [
      "✅ Scan done",
      `Pairs checked: ${summary.pairsChecked || 0}`,
      `Candidates: ${summary.candidates || 0}`,
      `Learned strategies: ${summary.learnedStrategies || 0}`,
      summary.error ? `Error: ${summary.error}` : null,
    ]
      .filter(Boolean)
      .join("\n"),
    inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.SCAN))]]),
    { userId: target.userId || String(target.from?.id || target.message?.from?.id || "") }
  );
}

async function runReconcile(bot, target, profile, targetProfile = null) {
  const summary = await tradeManager.reconcileProfile((targetProfile || profile).id);
  const screenTarget = targetProfile || profile;
  await showScreen(bot, target, SCREEN.RECONCILE, {
    profile: screenTarget,
    edit: Boolean(target.id),
    reconcileSummary: {
      ...summary,
      lastRun: state.nowIso(),
    },
  });
  return summary;
}

async function reloadPairMetadata(bot, target, profile, callbacks) {
  await pairUniverse.refreshMetadata({ force: true });
  if (callbacks?.runScan) {
    await callbacks.runScan({
      suppressSignals: true,
      forceReloadPairs: true,
      profileId: profile.id,
    });
  }
  await showScreen(bot, target, SCREEN.PAIRS, { profile, edit: Boolean(target.id) });
}

async function submitAutomationRequest(bot, chatId, profile) {
  const result = state.submitAutomationRequest(profile.id, {
    telegramUserId: profile.telegramUserId,
    chatId: profile.chatId,
    username: profile.username,
    displayName: profile.displayName,
    label: profile.label,
    masterWalletAddress: profile.masterWalletAddress,
    agentWalletAddress: profile.agentWalletAddress,
    requestedDefaults: {
      capitalMode: state.getRuntimeSettings(profile.id).capitalMode,
      executionMode: state.getRuntimeSettings(profile.id).executionMode,
    },
  });
  await notifyAdmins(
    bot,
    [
      "🆕 Automation approval request",
      `Request ID: ${result.request?.id || "N/A"}`,
      `Profile: ${profileLabel(profile)}`,
      `Telegram User: ${profile.telegramUserId || "N/A"}`,
      `Master Wallet: ${maskAddress(profile.masterWalletAddress)}`,
      `Agent Wallet: ${maskAddress(profile.agentWalletAddress)}`,
      `Secret Fingerprint: ${profile.agentSecretFingerprint || "not-set"}`,
    ].join("\n")
  );
  return result;
}

async function handlePendingPrompt(bot, msg, profile, callbacks) {
  const pending = getPendingPrompt(profile.id);
  if (!pending || !msg.text) return false;

  const chatId = msg.chat.id;
  const userId = String(msg.from?.id || "");
  const text = String(msg.text || "").trim();

  const successBack = pending.backScreen || SCREEN.HOME;

  switch (pending.kind) {
    case "principal": {
      const value = Number(text);
      if (!Number.isFinite(value) || value <= 0) {
        await sendInvalidInput(bot, chatId, "Invalid principal value.", ["/setprincipal 100"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setBaselinePrincipal(profile.id, value);
      const dashboard = await buildDashboard(profile);
      await sendMessage(
        bot,
        chatId,
        [
          "✅ Principal Updated",
          "",
          `Selected Principal: ${formatNumber(value)} USDT`,
          `Available Balance: ${formatNumber(dashboard.capital.availableTradableBalance)} USDT`,
          `Trade Principal: ${formatNumber(Math.min(value, dashboard.capital.availableTradableBalance || 0))} USDT`,
          "",
          "Formula:",
          "Trade Principal = min(Selected Principal, Available Balance)",
        ].join("\n"),
        inlineKeyboard([
          [button("📊 Capital Status", screenCallback(SCREEN.CAPITAL)), button("🔢 Set Slots", promptCallback("slots", SCREEN.CAPITAL))],
          [button("⬅️ Back", screenCallback(successBack))],
        ]),
        { userId }
      );
      return true;
    }

    case "slots": {
      const value = Number(text);
      if (!Number.isFinite(value) || value < 1) {
        await sendInvalidInput(bot, chatId, "Invalid slot count.", ["/setslots 2"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setSimpleSlots(profile.id, Math.trunc(value));
      const dashboard = await buildDashboard(profile);
      await sendMessage(
        bot,
        chatId,
        [
          "✅ Slots Updated",
          "",
          `Slots: ${dashboard.capital.simpleSlots}`,
          `Trade Principal: ${formatNumber(dashboard.capital.tradePrincipal)} USDT`,
          `Slot Size: ${formatNumber(dashboard.capital.slotSize)} USDT`,
          `Max Active Trades: ${dashboard.capital.simpleSlots}`,
        ].join("\n"),
        inlineKeyboard([
          [button("⬅️ Back", screenCallback(successBack))],
        ]),
        { userId }
      );
      return true;
    }

    case "demo_balance": {
      const value = Number(text);
      if (!Number.isFinite(value) || value <= 0) {
        await sendInvalidInput(bot, chatId, "Invalid demo balance value.", ["/setdemobalance 100"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setDemoBalances(profile.id, {
        perpBalance: value,
        spotBalance: Number(state.getRuntimeSettings(profile.id).demoSpotBalance || 0),
        startingBalance: value,
      });
      await showScreen(bot, msg, SCREEN.EXECUTION, { profile });
      return true;
    }

    case "trade_balance": {
      const value = Number(text);
      if (!Number.isFinite(value) || value <= 0) {
        await sendInvalidInput(bot, chatId, "Invalid trade balance value.", ["/settradebalance 100"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setTradeBalanceTarget(profile.id, value);
      await showScreen(bot, msg, SCREEN.TRADING, { profile });
      return true;
    }

    case "leverage": {
      const value = Number(text);
      if (!Number.isFinite(value) || value < 1 || value > 50) {
        await sendInvalidInput(bot, chatId, "Invalid leverage value.", ["/setleverage 10"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setTradeLeverage(profile.id, Math.trunc(value));
      await showScreen(bot, msg, SCREEN.TRADING, { profile });
      return true;
    }

    case "add_pair": {
      const pair = String(text).trim().toUpperCase();
      const validation = await pairUniverse.validatePair(pair).catch(() => ({ ok: false, reason: "invalid-pair" }));
      if (!validation.ok) {
        await sendInvalidInput(
          bot,
          chatId,
          validation.reason === "not-listed-on-hyperliquid"
            ? "Pair is not listed on Hyperliquid."
            : "Invalid pair format.",
          ["/addpair BTCUSDT"],
          { userId, retryPrompt: pending, backScreen: successBack }
        );
        return true;
      }
      clearPendingPrompt(profile.id);
      state.addWatchedPair(profile.id, validation.pair);
      await sendMessage(
        bot,
        chatId,
        [
          "✅ Pair Added",
          "",
          `Pair: ${validation.pair}`,
          `Hyperliquid Coin: ${validation.coin}`,
          `🌐 Link: ${validation.link}`,
          "",
          "This pair is now active for scanning.",
        ].join("\n"),
        inlineKeyboard([
          [urlButton("🌐 Open Hyperliquid", validation.link)],
          [button("⬅️ Back", screenCallback(SCREEN.PAIRS))],
        ]),
        { userId }
      );
      return true;
    }

    case "remove_pair": {
      const pair = String(text).trim().toUpperCase();
      if (!pairUniverse.formatIsValidPair(pair)) {
        await sendInvalidInput(bot, chatId, "Invalid pair format.", ["/removepair BTCUSDT"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.removeWatchedPair(profile.id, pair);
      await showScreen(bot, msg, SCREEN.PAIRS, { profile });
      return true;
    }

    case "pair_status": {
      const pair = String(text).trim().toUpperCase();
      const validation = await pairUniverse.validatePair(pair).catch(() => ({ ok: false, reason: "invalid-pair" }));
      if (!pairUniverse.formatIsValidPair(pair)) {
        await sendInvalidInput(bot, chatId, "Invalid pair format.", ["/pairstatus BTCUSDT"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      await sendMessage(
        bot,
        chatId,
        [
          `Pair: ${pair}`,
          `Watched: ${state.getWatchedPairs(profile.id).includes(pair) ? "YES" : "NO"}`,
          `Tradable: ${validation.ok ? "YES" : "NO"}`,
          `Reason: ${validation.ok ? "ok" : validation.reason}`,
          validation.link ? `Hyperliquid: ${validation.link}` : null,
        ]
          .filter(Boolean)
          .join("\n"),
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.PAIRS))]]),
        { userId }
      );
      return true;
    }

    case "wallet":
      clearPendingPrompt(profile.id);
      state.updateProfileFields(profile.id, { masterWalletAddress: text });
      await showScreen(bot, msg, SCREEN.AUTOMATION, { profile: state.getProfileById(profile.id, { includeSecret: true }) || profile });
      return true;

    case "agent_address":
      clearPendingPrompt(profile.id);
      state.updateProfileFields(profile.id, { agentWalletAddress: text });
      await showScreen(bot, msg, SCREEN.AUTOMATION, { profile: state.getProfileById(profile.id, { includeSecret: true }) || profile });
      return true;

    case "agent_secret":
      if (msg.chat?.type !== "private") {
        await sendMessage(bot, chatId, "Send the agent secret only in a private chat with the bot.", {}, { userId });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.updateProfileFields(profile.id, { agentSecret: text });
      await maybeDeleteSecretMessage(bot, msg);
      await sendMessage(bot, chatId, "✅ Agent signing secret stored securely.", inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.AUTOMATION))]]), { userId });
      return true;

    case "strategy_cap": {
      const value = Number(text);
      if (!Number.isFinite(value) || value < 1) {
        await sendInvalidInput(bot, chatId, "Invalid strategy cap.", ["/setstrategycap 500"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setStrategyCap(value, profile.id);
      await showScreen(bot, msg, SCREEN.STRATEGIES, { profile });
      return true;
    }

    case "strategy_retention": {
      const value = Number(text);
      if (!Number.isFinite(value) || value < 1) {
        await sendInvalidInput(bot, chatId, "Invalid strategy retention days.", ["/setstrategyretentiondays 7"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      state.setStrategyRetentionDays(value, profile.id);
      await showScreen(bot, msg, SCREEN.STRATEGIES, { profile });
      return true;
    }

    case "dryrun_pair": {
      const pair = String(text).trim().toUpperCase();
      const validation = await pairUniverse.validatePair(pair).catch(() => ({ ok: false, reason: "invalid-pair" }));
      if (!validation.ok) {
        await sendInvalidInput(bot, chatId, "Invalid pair format.", ["/dryrunpair BTCUSDT long"], {
          userId,
          retryPrompt: pending,
          backScreen: successBack,
        });
        return true;
      }
      clearPendingPrompt(profile.id);
      setDryRunSelection(profile.id, { pair: validation.pair });
      await showScreen(bot, msg, SCREEN.DRYRUN, { profile });
      return true;
    }

    case "view_user_config": {
      if (!isAdmin(userId)) {
        await sendMessage(bot, chatId, "Admin only.", {}, { userId });
        return true;
      }
      clearPendingPrompt(profile.id);
      const target = state.findProfile(text);
      await sendMessage(
        bot,
        chatId,
        target ? `👤 User Config\n${buildAutomationSummary(target)}` : "User profile not found.",
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.ADMIN))]]),
        { userId }
      );
      return true;
    }

    case "force_capital_mode": {
      if (!isAdmin(userId)) {
        await sendMessage(bot, chatId, "Admin only.", {}, { userId });
        return true;
      }
      const [ref, modeRaw] = text.split(/\s+/, 2);
      const target = state.findProfile(ref);
      const mode = String(modeRaw || "").toLowerCase();
      if (!target || !["simple", "compounding"].includes(mode)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid force capital mode input.",
          ["/forcesetcapitalmode 123456789 simple"],
          { userId, retryPrompt: pending, backScreen: successBack }
        );
        return true;
      }
      clearPendingPrompt(profile.id);
      if (!config.forceModeChangeEnabled) {
        await sendMessage(bot, chatId, "Force mode change is disabled in config.", {}, { userId });
        return true;
      }
      await setCapitalModeGuarded(bot, chatId, target, userId, mode, { force: true, reason: "admin-force" });
      await showScreen(bot, msg, SCREEN.ADMIN, { profile });
      return true;
    }

    case "force_execution_mode": {
      if (!isAdmin(userId)) {
        await sendMessage(bot, chatId, "Admin only.", {}, { userId });
        return true;
      }
      const [ref, modeRaw] = text.split(/\s+/, 2);
      const target = state.findProfile(ref);
      const mode = String(modeRaw || "").toLowerCase();
      if (!target || !["real", "demo"].includes(mode)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid force execution mode input.",
          ["/forcesetexecutionmode 123456789 demo"],
          { userId, retryPrompt: pending, backScreen: successBack }
        );
        return true;
      }
      clearPendingPrompt(profile.id);
      if (!config.forceModeChangeEnabled) {
        await sendMessage(bot, chatId, "Force mode change is disabled in config.", {}, { userId });
        return true;
      }
      await setExecutionModeGuarded(bot, chatId, target, userId, mode, { force: true, reason: "admin-force" });
      await showScreen(bot, msg, SCREEN.ADMIN, { profile });
      return true;
    }

    case "reconcile_user": {
      if (!isAdmin(userId)) {
        await sendMessage(bot, chatId, "Admin only.", {}, { userId });
        return true;
      }
      clearPendingPrompt(profile.id);
      const target = state.findProfile(text);
      if (!target) {
        await sendMessage(bot, chatId, "User profile not found.", {}, { userId });
        return true;
      }
      await runReconcile(bot, msg, profile, target);
      return true;
    }

    default:
      return false;
  }
}

async function handleCommand(bot, msg, parsed, callbacks) {
  const chatId = msg.chat.id;
  const userId = String(msg.from?.id || "");
  const profile = state.getOrCreateProfileFromTelegram(msg.from, msg.chat);
  const args = parsed.args;
  const argsText = parsed.argsText;
  const sendToUser = (text, extra = {}) => sendMessage(bot, chatId, text, extra, { userId });

  await ensureMenuAnchor(bot, chatId, userId, {
    force: parsed.command === "start" || parsed.command === "menu",
    replyToMessageId: msg.message_id,
  });

  const requireAdmin = async () => {
    if (isAdmin(userId)) return true;
    await sendToUser("Admin only.");
    return false;
  };

  switch (parsed.command) {
    case "start":
    case "menu":
      await ensureMainMenu(bot, chatId, userId);
      await showScreen(bot, msg, SCREEN.HOME, { profile });
      return;

    case "help":
      await showScreen(bot, msg, SCREEN.HELP, { profile });
      return;

    case "status":
      await showScreen(bot, msg, SCREEN.STATUS, { profile });
      return;

    case "capitalstatus":
      await showScreen(bot, msg, SCREEN.CAPITAL, { profile });
      return;

    case "sweepstatus":
      await sendToUser(
        [
          "💼 Profit Handling",
          `Capital Mode: ${state.getRuntimeSettings(profile.id).capitalMode}`,
          "Profit remains in the trading balance in every mode.",
          "No automatic transfer to spot is performed.",
        ].join("\n"),
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.CAPITAL))]])
      );
      return;

    case "scan":
      await showScreen(bot, msg, SCREEN.SCAN, { profile });
      return;

    case "positions":
      await showScreen(bot, msg, SCREEN.POSITIONS, { profile });
      return;

    case "closed":
      await showScreen(bot, msg, SCREEN.CLOSED, { profile });
      return;

    case "cleardemohistory": {
      const summary = state.clearDemoTradeHistory(profile.id);
      await sendToUser(
        [
          "🧹 Demo Trade History Cleared",
          `Deleted Closed Demo Trades: ${summary.deleted}`,
          `Reset Pair States: ${summary.pairsReset}`,
          "Open trades and balances were not changed.",
        ].join("\n"),
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.CLOSED))]])
      );
      return;
    }

    case "pnl": {
      const filters = {};
      for (const token of args.map((arg) => String(arg).toLowerCase())) {
        if (token === "real" || token === "demo") filters.executionMode = token.toUpperCase();
        if (token === "simple" || token === "compounding") filters.capitalMode = token.toUpperCase();
      }
      if (!Object.keys(filters).length) {
        await showScreen(bot, msg, SCREEN.PNL, { profile });
        return;
      }
      await showScreen(bot, msg, SCREEN.PNL, { profile, filters });
      return;
    }

    case "balance":
    case "realstatus":
    case "demostatus":
      await showScreen(bot, msg, SCREEN.BALANCE, { profile });
      return;

    case "capitalmode":
      await showScreen(bot, msg, SCREEN.CAPITAL, { profile });
      return;

    case "setcapitalmode": {
      const mode = String(args[0] || "").toLowerCase();
      if (!["simple", "compounding"].includes(mode)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid capital mode.",
          ["/setcapitalmode simple", "/setcapitalmode compounding"],
          { userId, backScreen: SCREEN.CAPITAL }
        );
        return;
      }
      const changed = await setCapitalModeGuarded(bot, chatId, profile, userId, mode);
      if (changed) {
        await showScreen(bot, msg, SCREEN.CAPITAL, { profile });
      }
      return;
    }

    case "executionmode":
      await showScreen(bot, msg, SCREEN.EXECUTION, { profile });
      return;

    case "setexecutionmode": {
      const mode = String(args[0] || "").toLowerCase();
      if (!["real", "demo"].includes(mode)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid execution mode.",
          ["/setexecutionmode real", "/setexecutionmode demo"],
          { userId, backScreen: SCREEN.EXECUTION }
        );
        return;
      }
      const changed = await setExecutionModeGuarded(bot, chatId, profile, userId, mode);
      if (changed) {
        await showScreen(bot, msg, SCREEN.EXECUTION, { profile });
      }
      return;
    }

    case "principal":
      await showScreen(bot, msg, SCREEN.CAPITAL, { profile });
      return;

    case "setprincipal":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "principal", SCREEN.CAPITAL);
        return;
      }
      setPendingPrompt(profile.id, { kind: "principal", backScreen: SCREEN.CAPITAL });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "slots":
      await showScreen(bot, msg, SCREEN.CAPITAL, { profile });
      return;

    case "setslots":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "slots", SCREEN.CAPITAL);
        return;
      }
      setPendingPrompt(profile.id, { kind: "slots", backScreen: SCREEN.CAPITAL });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "tradebalance":
      await showScreen(bot, msg, SCREEN.TRADING, { profile });
      return;

    case "settradebalance":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "trade_balance", SCREEN.TRADING);
        return;
      }
      setPendingPrompt(profile.id, { kind: "trade_balance", backScreen: SCREEN.TRADING });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "leverage":
      await showScreen(bot, msg, SCREEN.TRADING, { profile });
      return;

    case "setleverage":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "leverage", SCREEN.TRADING);
        return;
      }
      setPendingPrompt(profile.id, { kind: "leverage", backScreen: SCREEN.TRADING });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "demobalance":
      await showScreen(bot, msg, SCREEN.EXECUTION, { profile });
      return;

    case "setdemobalance":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "demo_balance", SCREEN.EXECUTION);
        return;
      }
      setPendingPrompt(profile.id, { kind: "demo_balance", backScreen: SCREEN.EXECUTION });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "autotrade": {
      if (!args.length) {
        await showScreen(bot, msg, SCREEN.TRADING, { profile });
        return;
      }
      const value = String(args[0] || "").toLowerCase();
      if (!["on", "off"].includes(value)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid auto trade value.",
          ["/autotrade on", "/autotrade off"],
          { userId, backScreen: SCREEN.TRADING }
        );
        return;
      }
      state.setAutoTradeEnabled(profile.id, value === "on");
      await showScreen(bot, msg, SCREEN.TRADING, { profile });
      return;
    }

    case "pairs":
      await showScreen(bot, msg, SCREEN.PAIRS, { profile });
      return;

    case "addpair":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "add_pair", SCREEN.PAIRS);
        return;
      }
      setPendingPrompt(profile.id, { kind: "add_pair", backScreen: SCREEN.PAIRS });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "removepair":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "remove_pair", SCREEN.PAIRS);
        return;
      }
      setPendingPrompt(profile.id, { kind: "remove_pair", backScreen: SCREEN.PAIRS });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "reloadpairs":
      await reloadPairMetadata(bot, msg, profile, callbacks);
      return;

    case "pairstatus":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "pair_status", SCREEN.PAIRS);
        return;
      }
      setPendingPrompt(profile.id, { kind: "pair_status", backScreen: SCREEN.PAIRS });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "reconcile":
      if (!args.length || args[0] === "me") {
        await showScreen(bot, msg, SCREEN.RECONCILE, { profile });
        return;
      }
      if (args[0] === "all") {
        if (!(await requireAdmin())) return;
        const summaries = await tradeManager.reconcileAllProfiles();
        for (const summary of summaries) {
          await sendToUser(buildReconciliationCompleteMessage(summary));
        }
        return;
      }
      if (args[0] === "user") {
        if (!(await requireAdmin())) return;
        const target = state.findProfile(args[1]);
        if (!target) {
          await sendToUser("User profile not found.");
          return;
        }
        await runReconcile(bot, msg, profile, target);
        return;
      }
      await showScreen(bot, msg, SCREEN.RECONCILE, { profile });
      return;

    case "reconcileall":
      if (!(await requireAdmin())) return;
      for (const summary of await tradeManager.reconcileAllProfiles()) {
        await sendToUser(buildReconciliationCompleteMessage(summary));
      }
      return;

    case "connecttrading":
      await showScreen(bot, msg, SCREEN.AUTOMATION, { profile });
      return;

    case "setwallet":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "wallet", SCREEN.AUTOMATION);
        return;
      }
      setPendingPrompt(profile.id, { kind: "wallet", backScreen: SCREEN.AUTOMATION });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "setagentaddress":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "agent_address", SCREEN.AUTOMATION);
        return;
      }
      setPendingPrompt(profile.id, { kind: "agent_address", backScreen: SCREEN.AUTOMATION });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "setagentprivatekey":
      if (msg.chat?.type !== "private") {
        await sendToUser("Send /setagentprivatekey only in a private chat with the bot.");
        return;
      }
      if (!argsText) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "agent_secret", SCREEN.AUTOMATION);
        return;
      }
      setPendingPrompt(profile.id, { kind: "agent_secret", backScreen: SCREEN.AUTOMATION });
      await handlePendingPrompt(bot, { ...msg, text: argsText }, profile, callbacks);
      return;

    case "submitautomationrequest": {
      const result = await submitAutomationRequest(bot, chatId, profile);
      await sendToUser(`✅ Automation request submitted.\nRequest ID: ${result.request?.id || "N/A"}`);
      return;
    }

    case "automationstatus":
      await showScreen(bot, msg, SCREEN.AUTOMATION, { profile });
      return;

    case "disableautomation": {
      const updated = state.setAutomationEnabled(profile.id, false, userId);
      await sendToUser(buildAutomationDisabledMessage(updated || profile));
      return;
    }

    case "enableautomation": {
      if (profile.automationStatus !== "APPROVED") {
        await sendToUser("automation-not-approved");
        return;
      }
      const updated = state.setAutomationEnabled(profile.id, true, userId);
      await sendToUser(buildAutomationApprovedMessage(updated || profile));
      return;
    }

    case "admin":
      if (!(await requireAdmin())) return;
      await showScreen(bot, msg, SCREEN.ADMIN, { profile });
      return;

    case "pendingapprovals":
      if (!(await requireAdmin())) return;
      await showScreen(bot, msg, SCREEN.APPROVALS, { profile });
      return;

    case "approveautomation": {
      if (!(await requireAdmin())) return;
      const id = Number(args[0]);
      if (!Number.isFinite(id)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Missing approval request ID.",
          ["/approveautomation 1042"],
          { userId, backScreen: SCREEN.APPROVALS }
        );
        return;
      }
      const updated = state.updateAutomationRequestStatus(id, "APPROVED", { actorUserId: userId });
      if (!updated) {
        await sendToUser("Request not found.");
        return;
      }
      await sendToUser(buildAutomationApprovedMessage(updated));
      if (updated.chatId) {
        await sendMessage(bot, updated.chatId, buildAutomationApprovedMessage(updated));
      }
      return;
    }

    case "rejectautomation": {
      if (!(await requireAdmin())) return;
      const id = Number(args[0]);
      const reason = args.slice(1).join(" ") || "rejected";
      if (!Number.isFinite(id)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Missing rejection request ID.",
          ["/rejectautomation 1042 duplicate request"],
          { userId, backScreen: SCREEN.APPROVALS }
        );
        return;
      }
      const updated = state.updateAutomationRequestStatus(id, "REJECTED", {
        actorUserId: userId,
        reason,
      });
      await sendToUser(updated ? `Rejected request ${id}` : "Request not found.");
      return;
    }

    case "removeautomation": {
      if (!(await requireAdmin())) return;
      const target = state.findProfile(args[0]);
      if (!target) {
        await sendToUser("User profile not found.");
        return;
      }
      state.removeAutomationProfile(target.id, userId);
      await sendToUser(`Removed automation profile ${target.id}`);
      return;
    }

    case "disablewallet":
    case "enablewallet": {
      if (!(await requireAdmin())) return;
      const target = state.findProfile(args[0]);
      if (!target) {
        await sendToUser("User profile not found.");
        return;
      }
      state.setWalletEnabled(target.id, parsed.command === "enablewallet", userId);
      await sendToUser(
        `${parsed.command === "enablewallet" ? "Enabled" : "Disabled"} wallet ${target.id}`
      );
      return;
    }

    case "listautomationusers": {
      if (!(await requireAdmin())) return;
      const profiles = state
        .listProfiles({ includeDisabled: true })
        .filter((item) => item.automationStatus && item.automationStatus !== "NOT_CONFIGURED");
      await sendToUser(
        profiles.length
          ? profiles
              .map(
                (item) =>
                  `${item.id} | ${item.automationStatus} | enabled=${item.automationEnabled ? "yes" : "no"} | wallet=${maskAddress(item.masterWalletAddress)}`
              )
              .join("\n")
          : "No automation users."
      );
      return;
    }

    case "viewuserconfig": {
      if (!(await requireAdmin())) return;
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "view_user_config", SCREEN.ADMIN);
        return;
      }
      const target = state.findProfile(args[0]);
      if (!target) {
        await sendToUser("User profile not found.");
        return;
      }
      await sendToUser(`👤 User Config\n${buildAutomationSummary(target)}`);
      return;
    }

    case "forcesetcapitalmode": {
      if (!(await requireAdmin())) return;
      const target = state.findProfile(args[0]);
      const mode = String(args[1] || "").toLowerCase();
      if (!target || !["simple", "compounding"].includes(mode)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid force capital mode command.",
          ["/forcesetcapitalmode 123456789 simple"],
          { userId, backScreen: SCREEN.ADMIN }
        );
        return;
      }
      if (!config.forceModeChangeEnabled) {
        await sendToUser("Force mode change is disabled in config.");
        return;
      }
      const changed = await setCapitalModeGuarded(bot, chatId, target, userId, mode, {
        force: true,
        reason: "admin-force",
      });
      if (changed) {
        await sendToUser(`Forced capital mode for ${target.id} -> ${mode.toUpperCase()}`);
      }
      return;
    }

    case "forcesetexecutionmode": {
      if (!(await requireAdmin())) return;
      const target = state.findProfile(args[0]);
      const mode = String(args[1] || "").toLowerCase();
      if (!target || !["real", "demo"].includes(mode)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid force execution mode command.",
          ["/forcesetexecutionmode 123456789 demo"],
          { userId, backScreen: SCREEN.ADMIN }
        );
        return;
      }
      if (!config.forceModeChangeEnabled) {
        await sendToUser("Force mode change is disabled in config.");
        return;
      }
      const changed = await setExecutionModeGuarded(bot, chatId, target, userId, mode, {
        force: true,
        reason: "admin-force",
      });
      if (changed) {
        await sendToUser(`Forced execution mode for ${target.id} -> ${mode.toUpperCase()}`);
      }
      return;
    }

    case "strategystatus":
      await showScreen(bot, msg, SCREEN.STRATEGIES, { profile });
      return;

    case "strategytop": {
      const top = strategyMaintenance.getTopStrategies(10, profile.id);
      await sendToUser(
        top.length
          ? top
              .map(
                (item, index) =>
                  `${index + 1}. ${item.strategy.pair} ${item.strategy.direction} ${item.strategy.mainSourceTimeframe} | score=${item.weightedScore} | trades=${item.total} | net=${item.netPnl}`
              )
              .join("\n")
          : "No ranked strategies.",
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]])
      );
      return;
    }

    case "strategyprune": {
      const summary = strategyMaintenance.pruneStrategies({ profileId: profile.id });
      await sendToUser(
        [
          "🧹 Strategy Prune Complete",
          `Before: ${summary.totalBefore}`,
          `Kept: ${summary.kept}`,
          `Removed: ${summary.removed}`,
          `Archive: ${summary.archivePath}`,
        ].join("\n"),
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]])
      );
      return;
    }

    case "exportstrategies":
      if (!(await requireAdmin())) return;
      await exportStrategies(bot, chatId, { userId });
      return;

    case "strategyexports":
      if (!(await requireAdmin())) return;
      await listStrategyExports(bot, chatId, { userId });
      return;

    case "importstrategies": {
      if (!(await requireAdmin())) return;
      const optionTokens = args.map((arg) => String(arg).trim()).filter(Boolean);
      const replace = optionTokens.some((arg) => arg.toLowerCase() === "replace");
      const selector =
        optionTokens.find((arg) => {
          const lower = arg.toLowerCase();
          return lower !== "replace" && lower !== "merge";
        }) || "latest";

      if (msg.reply_to_message?.document || msg.document) {
        await importStrategiesFromDocument(
          bot,
          chatId,
          msg.reply_to_message?.document || msg.document || null,
          { userId, replace }
        );
        return;
      }

      const fileRecord = resolveStrategyExportFile(selector);
      if (!fileRecord) {
        await sendInvalidInput(
          bot,
          chatId,
          "Strategy export file not found.",
          ["/importstrategies latest", "/importstrategies strategies-1234567890.txt"],
          { userId, backScreen: SCREEN.STRATEGIES }
        );
        return;
      }

      await importStrategiesFromFile(bot, chatId, fileRecord, { userId, replace });
      return;
    }

    case "importstrategyfile": {
      if (!(await requireAdmin())) return;
      const { replace, filePath } = parseImportPathArgs(argsText);
      if (!filePath) {
        await sendInvalidInput(
          bot,
          chatId,
          "Missing external strategy file path.",
          ["/importstrategyfile /abs/path/file.json", "/importstrategyfile replace /abs/path/file.txt"],
          { userId, backScreen: SCREEN.STRATEGIES }
        );
        return;
      }

      await importStrategiesFromExternalPath(bot, chatId, filePath, { userId, replace });
      return;
    }

    case "strategyretention":
      await showScreen(bot, msg, SCREEN.STRATEGIES, { profile });
      return;

    case "setstrategyretentiondays":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "strategy_retention", SCREEN.STRATEGIES);
        return;
      }
      setPendingPrompt(profile.id, { kind: "strategy_retention", backScreen: SCREEN.STRATEGIES });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "setstrategycap":
      if (!args.length) {
        await sendPrompt(bot, { ...msg, bot, userId }, profile, "strategy_cap", SCREEN.STRATEGIES);
        return;
      }
      setPendingPrompt(profile.id, { kind: "strategy_cap", backScreen: SCREEN.STRATEGIES });
      await handlePendingPrompt(bot, { ...msg, text: args[0] }, profile, callbacks);
      return;

    case "dryrun":
      await showScreen(bot, msg, SCREEN.DRYRUN, { profile });
      return;

    case "dryrunlong": {
      const preview = await buildDryRunPreview(profile, "long");
      await showScreen(bot, msg, SCREEN.DRYRUN, { profile, dryRunPreview: preview });
      return;
    }

    case "dryrunshort": {
      const preview = await buildDryRunPreview(profile, "short");
      await showScreen(bot, msg, SCREEN.DRYRUN, { profile, dryRunPreview: preview });
      return;
    }

    case "dryrunpair": {
      const pair = String(args[0] || "").trim().toUpperCase();
      const side = String(args[1] || "").toLowerCase();
      if (!pair || !["long", "short"].includes(side)) {
        await sendInvalidInput(
          bot,
          chatId,
          "Invalid dry-run command.",
          ["/dryrunpair BTCUSDT long", "/dryrunpair BTCUSDT short"],
          { userId, backScreen: SCREEN.DRYRUN }
        );
        return;
      }
      const preview = await buildDryRunPreview(profile, side, pair);
      await showScreen(bot, msg, SCREEN.DRYRUN, { profile, dryRunPreview: preview });
      return;
    }

    case "signals": {
      const activeSignals = Object.values(state.getActiveSignals(profile.id));
      await sendToUser(
        activeSignals.length
          ? activeSignals
              .slice(0, 20)
              .map(
                (signal) =>
                  `${signal.pair} ${signal.side} | ${signal.baseTimeframe} | score=${signal.score} | msg=${signal.messageId || "n/a"}`
              )
              .join("\n")
          : "No active signal threads."
      );
      return;
    }

    case "cooldowns": {
      const rows = state
        .listPairStates(profile.id)
        .filter(Boolean)
        .sort((a, b) => String(a.pair || "").localeCompare(String(b.pair || "")));
      await sendToUser(
        rows.length
          ? rows
              .slice(0, 50)
              .map(
                (row) =>
                  `${row.pair} | lastSide=${row.lastSide || "n/a"} | lastEntry=${row.lastEntryPrice || "n/a"} | repeatLevel=${row.repeatLevel ?? "n/a"} | cooldownUntil=${row.cooldownUntil || "none"}`
              )
              .join("\n")
          : "No pair repeat-state recorded yet."
      );
      return;
    }

    case "strategies": {
      const index = loadStrategiesIndex();
      await sendToUser(`🧠 Saved learned strategies: ${index.length}`);
      return;
    }

    case "strategylist": {
      const index = loadStrategiesIndex().slice(0, 100);
      if (!index.length) {
        await sendToUser("No saved strategies yet.");
        return;
      }
      for (const chunk of splitLongMessage(
        index
          .map(
            (strategy) =>
              `${strategy.pair} | ${strategy.direction} | ${strategy.eventTime} | mainTF=${strategy.mainSourceTimeframe || "n/a"}`
          )
          .join("\n")
      )) {
        await sendToUser(chunk);
      }
      return;
    }

    case "strategy": {
      const pair = String(args[0] || "").trim().toUpperCase();
      if (!pair) {
        await sendToUser("Use: /strategy BTCUSDT");
        return;
      }
      const strategies = getStrategyByPair(pair);
      await sendToUser(
        strategies.length
          ? strategies
              .slice(0, 20)
              .map(
                (strategy) =>
                  `${strategy.pair} | ${strategy.direction} | ${strategy.eventTime} | tf=${strategy.mainSourceTimeframe || "n/a"}`
              )
              .join("\n")
          : `No strategy saved for ${pair}`
      );
      return;
    }

    case "rebuildstrategies": {
      const rebuilt = rebuildStrategiesIndexFromFiles();
      await sendToUser(`♻️ Legacy strategy JSON import complete.\nTotal strategies stored: ${rebuilt.length}`);
      return;
    }

    default:
      await sendToUser("Unknown command. Use /help");
  }
}

async function handleCallbackQuery(bot, query, callbacks) {
  const data = String(query.data || "");
  const userId = String(query.from?.id || "");
  const chatId = query.message?.chat?.id;
  const profile = state.getOrCreateProfileFromTelegram(query.from, query.message?.chat);
  const callbackTarget = {
    ...query,
    bot,
    userId,
  };

  if (chatId) {
    await ensureMenuAnchor(bot, chatId, userId);
  }

  if (data.startsWith("scr:")) {
    await showScreen(bot, callbackTarget, data.slice(4), { profile });
    return;
  }

  if (data.startsWith("prm:")) {
    const [, kind, backScreen] = data.split(":");
    await sendPrompt(bot, callbackTarget, profile, kind, backScreen || SCREEN.HOME, true);
    return;
  }

  if (data.startsWith("pnl:")) {
    const [, executionMode, capitalMode] = data.split(":");
    await showScreen(bot, callbackTarget, SCREEN.PNL, {
      profile,
      filters: {
        executionMode: executionMode === "all" ? undefined : String(executionMode || "").toUpperCase(),
        capitalMode: capitalMode === "all" ? undefined : String(capitalMode || "").toUpperCase(),
      },
    });
    return;
  }

  if (data === "set:auto:on" || data === "set:auto:off") {
    state.setAutoTradeEnabled(profile.id, data.endsWith(":on"));
    await showScreen(bot, callbackTarget, SCREEN.TRADING, { profile });
    return;
  }

  if (data === "set:capital:simple" || data === "set:capital:compounding") {
    const mode = data.endsWith(":simple") ? "simple" : "compounding";
    const changed = await setCapitalModeGuarded(bot, chatId, profile, userId, mode);
    if (changed) {
      await showScreen(bot, callbackTarget, SCREEN.CAPITAL, { profile });
    } else {
      await answerCallbackSafe(bot, query.id);
    }
    return;
  }

  if (data === "set:execution:real" || data === "set:execution:demo") {
    const mode = data.endsWith(":real") ? "real" : "demo";
    const changed = await setExecutionModeGuarded(bot, chatId, profile, userId, mode);
    if (changed) {
      await showScreen(bot, callbackTarget, SCREEN.EXECUTION, { profile });
    } else {
      await answerCallbackSafe(bot, query.id);
    }
    return;
  }

  if (data === "pair:reload") {
    await reloadPairMetadata(bot, callbackTarget, profile, callbacks);
    return;
  }

  if (data === "run:scan") {
    await answerCallbackSafe(bot, query.id, "Running scan...");
    await runManualScan(bot, callbackTarget, profile, callbacks, { edit: false });
    return;
  }

  if (data === "dry:long" || data === "dry:short") {
    const side = data.endsWith(":long") ? "long" : "short";
    const preview = await buildDryRunPreview(profile, side);
    await showScreen(bot, callbackTarget, SCREEN.DRYRUN, { profile, dryRunPreview: preview });
    return;
  }

  if (data === "auto:connect") {
    await showScreen(bot, callbackTarget, SCREEN.AUTOMATION, { profile });
    return;
  }

  if (data === "auto:submit") {
    const result = await submitAutomationRequest(bot, chatId, profile);
    await answerCallbackSafe(bot, query.id, `Request ${result.request?.id || "submitted"}`);
    await showScreen(bot, callbackTarget, SCREEN.AUTOMATION, { profile: state.getProfileById(profile.id, { includeSecret: true }) || profile });
    return;
  }

  if (data === "auto:enable") {
    if (profile.automationStatus !== "APPROVED") {
      await answerCallbackSafe(bot, query.id, "automation-not-approved");
      return;
    }
    state.setAutomationEnabled(profile.id, true, userId);
    await showScreen(bot, callbackTarget, SCREEN.AUTOMATION, { profile: state.getProfileById(profile.id, { includeSecret: true }) || profile });
    return;
  }

  if (data === "auto:disable") {
    state.setAutomationEnabled(profile.id, false, userId);
    await showScreen(bot, callbackTarget, SCREEN.AUTOMATION, { profile: state.getProfileById(profile.id, { includeSecret: true }) || profile });
    return;
  }

  if (data === "recon:self") {
    await answerCallbackSafe(bot, query.id, "Running reconcile...");
    const summary = await tradeManager.reconcileProfile(profile.id);
    await showScreen(bot, callbackTarget, SCREEN.RECONCILE, {
      profile,
      reconcileSummary: {
        ...summary,
        lastRun: state.nowIso(),
      },
    });
    return;
  }

  if (data === "recon:all") {
    if (!isAdmin(userId)) {
      await answerCallbackSafe(bot, query.id, "Admin only.");
      return;
    }
    const summaries = await tradeManager.reconcileAllProfiles();
    await answerCallbackSafe(bot, query.id, `Reconciled ${summaries.length} profiles`);
    for (const summary of summaries) {
      await sendMessage(bot, chatId, buildReconciliationCompleteMessage(summary), {}, { userId });
    }
    return;
  }

  if (data === "str:top") {
    const top = strategyMaintenance.getTopStrategies(10, profile.id);
    await sendMessage(
      bot,
      chatId,
      top.length
        ? top
            .map(
              (item, index) =>
                `${index + 1}. ${item.strategy.pair} ${item.strategy.direction} ${item.strategy.mainSourceTimeframe} | score=${item.weightedScore} | trades=${item.total} | net=${item.netPnl}`
            )
            .join("\n")
        : "No ranked strategies.",
      inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]]),
      { userId }
    );
    await answerCallbackSafe(bot, query.id);
    return;
  }

  if (data === "str:prune") {
    const summary = strategyMaintenance.pruneStrategies({ profileId: profile.id });
    await sendMessage(
      bot,
      chatId,
      [
        "🧹 Strategy Prune Complete",
        `Before: ${summary.totalBefore}`,
        `Kept: ${summary.kept}`,
        `Removed: ${summary.removed}`,
        `Archive: ${summary.archivePath}`,
      ].join("\n"),
      inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]]),
      { userId }
    );
    await answerCallbackSafe(bot, query.id);
    return;
  }

  if (data === "str:export") {
    if (!isAdmin(userId)) {
      await answerCallbackSafe(bot, query.id, "Admin only.");
      return;
    }
    await answerCallbackSafe(bot, query.id, "Exporting...");
    await exportStrategies(bot, chatId, { userId });
    return;
  }

  if (data === "str:exports") {
    if (!isAdmin(userId)) {
      await answerCallbackSafe(bot, query.id, "Admin only.");
      return;
    }
    await answerCallbackSafe(bot, query.id);
    await listStrategyExports(bot, chatId, { userId });
    return;
  }

  if (data === "str:import_latest") {
    if (!isAdmin(userId)) {
      await answerCallbackSafe(bot, query.id, "Admin only.");
      return;
    }
    const fileRecord = resolveStrategyExportFile("latest");
    if (!fileRecord) {
      await answerCallbackSafe(bot, query.id, "No export file found");
      await sendMessage(
        bot,
        chatId,
        "No strategy export file found. Use `/exportstrategies` first.",
        inlineKeyboard([[button("⬅️ Back", screenCallback(SCREEN.STRATEGIES))]]),
        { userId }
      );
      return;
    }
    await answerCallbackSafe(bot, query.id, `Importing ${fileRecord.name}`);
    await importStrategiesFromFile(bot, chatId, fileRecord, { userId });
    return;
  }

  if (data.startsWith("apr:")) {
    if (!isAdmin(userId)) {
      await answerCallbackSafe(bot, query.id, "Admin only.");
      return;
    }
    const [, action, rawId] = data.split(":");
    const id = Number(rawId);
    if (!Number.isFinite(id)) {
      await answerCallbackSafe(bot, query.id, "Invalid request ID");
      return;
    }
    if (action === "approve") {
      const updated = state.updateAutomationRequestStatus(id, "APPROVED", { actorUserId: userId });
      if (updated?.chatId) {
        await sendMessage(bot, updated.chatId, buildAutomationApprovedMessage(updated));
      }
      await answerCallbackSafe(bot, query.id, updated ? "Approved" : "Request not found");
      await showScreen(bot, callbackTarget, SCREEN.APPROVALS, { profile });
      return;
    }
    if (action === "reject") {
      state.updateAutomationRequestStatus(id, "REJECTED", {
        actorUserId: userId,
        reason: "rejected-from-telegram",
      });
      await answerCallbackSafe(bot, query.id, "Rejected");
      await showScreen(bot, callbackTarget, SCREEN.APPROVALS, { profile });
      return;
    }
  }

  await answerCallbackSafe(bot, query.id);
}

function registerHandlers(bot, callbacks) {
  if (!bot) return;

  bot.on("message", async (msg) => {
    try {
      if (!msg.text) return;
      const profile = state.getOrCreateProfileFromTelegram(msg.from, msg.chat);
      const parsed = parseCommand(msg.text);
      if (parsed) {
        await handleCommand(bot, msg, parsed, callbacks);
        armEphemeralDeleteForMessage(bot, msg);
        return;
      }

      const menuAction = resolveMenuTextAction(msg.text);
      if (menuAction?.type === "screen") {
        await showScreen(bot, msg, menuAction.screen, { profile });
        armEphemeralDeleteForMessage(bot, msg);
        return;
      }

      const pendingHandled = await handlePendingPrompt(bot, msg, profile, callbacks);
      if (pendingHandled) {
        armEphemeralDeleteForMessage(bot, msg);
      }
      return;
    } catch (error) {
      console.error("Telegram command handler error:", error);
      state.setSnapshot("system:lastError", {
        timestamp: state.nowIso(),
        message: error.message,
        scope: "telegram-message",
      });
      await sendMessage(bot, msg.chat.id, `Command failed: ${error.message}`, {}, {
        userId: String(msg.from?.id || ""),
      });
      armEphemeralDeleteForMessage(bot, msg);
    }
  });

  bot.on("callback_query", async (query) => {
    try {
      await handleCallbackQuery(bot, query, callbacks);
    } catch (error) {
      console.error("Telegram callback handler error:", error);
      state.setSnapshot("system:lastError", {
        timestamp: state.nowIso(),
        message: error.message,
        scope: "telegram-callback",
      });
      await answerCallbackSafe(bot, query.id, "Action failed");
      if (query.message?.chat?.id) {
        await sendMessage(bot, query.message.chat.id, `Action failed: ${error.message}`, {}, {
          userId: String(query.from?.id || ""),
        });
      }
    }
  });

  bot.on("document", async (msg) => {
    try {
      const caption = String(msg.caption || "").trim().toLowerCase();
      if (!caption.startsWith("/importstrategies")) return;
      await importStrategiesFromDocument(bot, msg.chat.id, msg.document);
      armEphemeralDeleteForMessage(bot, msg);
    } catch (error) {
      console.error("Telegram document handler error:", error);
      await sendMessage(bot, msg.chat.id, `Import failed: ${error.message}`, {}, {
        userId: String(msg.from?.id || ""),
      });
      armEphemeralDeleteForMessage(bot, msg);
    }
  });

  bot.on("polling_error", (error) => {
    if (/ETIMEDOUT|ECONNRESET|socket hang up/i.test(String(error?.message || ""))) {
      console.warn("Telegram polling warning:", error?.message || "polling-timeout");
      return;
    }
    console.error("Telegram polling error:", {
      code: error?.code || null,
      message: error?.message || null,
      responseBody: error?.response?.body || error?.response?.data || null,
      stack: error?.stack || null,
    });
    state.setSnapshot("system:lastError", {
      timestamp: state.nowIso(),
      message: error?.message || "polling-error",
      scope: "telegram-polling",
    });
  });
}

module.exports = {
  createBot,
  setupCommands,
  registerHandlers,
  buildHelpText,
};
