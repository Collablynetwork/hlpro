const { round } = require("./indicators");
const pairUniverse = require("./pairUniverse");

function formatPrice(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  if (Math.abs(num) >= 1000) return num.toFixed(2);
  if (Math.abs(num) >= 1) return num.toFixed(4);
  return num.toFixed(6);
}

function formatScore(value) {
  const num = Number(value);
  return Number.isFinite(num) ? round(num, 2) : "N/A";
}

function formatPct(value, digits = 2) {
  const num = Number(value);
  return Number.isFinite(num) ? `${round(num, digits)}%` : "N/A";
}

function formatUsd(value) {
  const num = Number(value);
  return Number.isFinite(num) ? `$${round(num, 6)}` : "N/A";
}

function formatQty(value) {
  const num = Number(value);
  return Number.isFinite(num) ? String(round(num, 8)) : "N/A";
}

function sideEmoji(side) {
  return String(side || "").toUpperCase() === "SHORT" ? "🔴" : "🟢";
}

function sideWord(side) {
  return String(side || "").toUpperCase() === "SHORT" ? "SHORT" : "LONG";
}

function modeTag(tradeOrContext = {}) {
  const executionMode = String(tradeOrContext.executionMode || "DEMO").toUpperCase();
  const capitalMode = String(tradeOrContext.capitalMode || "SIMPLE").toUpperCase();
  return `${executionMode} | ${capitalMode}`;
}

function hyperliquidLink(pair) {
  return pairUniverse.buildHyperliquidLink(pair);
}

function slotInfo(tradeOrContext = {}) {
  if (String(tradeOrContext.capitalMode || "").toUpperCase() !== "SIMPLE") return null;
  return `🎛 Slots: ${tradeOrContext.simpleSlots || tradeOrContext.slotCount || "N/A"} | Slot Size: ${formatUsd(
    tradeOrContext.slotSize
  )}`;
}

function principalInfo(tradeOrContext = {}) {
  const principal =
    tradeOrContext.principalUsed ??
    tradeOrContext.currentPrincipal ??
    tradeOrContext.tradeBalanceTarget ??
    null;
  return principal != null ? `💼 Principal Used: ${formatUsd(principal)}` : null;
}

function buildPublicSignalMessage(candidate) {
  const side = sideWord(candidate.side);
  const emoji = sideEmoji(side);
  const pair = String(candidate.pair || "").toUpperCase();
  const reasons = Array.isArray(candidate.reasons) ? candidate.reasons.slice(0, 4) : [];

  return [
    `👀 ${side} SIGNAL`,
    `🪙 Pair: ${pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(pair)}`,
    `⏱ Execution TF: ${candidate.baseTimeframe || candidate.baseTf || "N/A"}`,
    `💵 Entry: ${formatPrice(candidate.entryPrice || candidate.entry)}`,
    `🏁 TP: ${formatPrice(candidate.targetPrice || candidate.tp1)}`,
    `🛑 SL: ${formatPrice(candidate.stopLoss || candidate.sl)}`,
    `🧠 Strategy: ${candidate.strategyUsed || candidate.strategySource || "N/A"}`,
    reasons.length ? `✅ Reason: ${reasons.join(" | ")}` : `${emoji} Reason: Matched learned setup`,
    `Status: SIGNAL CREATED`,
  ].join("\n");
}

function buildSignalMessage(candidate, context = {}) {
  const side = sideWord(candidate.side);
  const emoji = sideEmoji(side);
  const supportTfs = candidate.supportTfs || candidate.supportTimeframes || [];
  const reasons = Array.isArray(candidate.reasons) ? candidate.reasons.slice(0, 4) : [];
  const pair = String(candidate.pair || "").toUpperCase();

  return [
    `📡 SIGNAL CREATED [${modeTag(context)}]`,
    `${emoji} ${side}`,
    `🪙 Pair: ${pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(pair)}`,
    `⏱ Base TF: ${candidate.baseTimeframe || candidate.baseTf || "N/A"}`,
    `📚 Support TFs (${supportTfs.length}): ${supportTfs.join(", ") || "N/A"}`,
    `🎯 Score: ${formatScore(candidate.score)}`,
    `💵 Entry: ${formatPrice(candidate.entryPrice || candidate.entry)}`,
    `🏁 TP: ${formatPrice(candidate.targetPrice || candidate.tp1)}`,
    `🛑 SL: ${formatPrice(candidate.stopLoss || candidate.sl)}`,
    context.leverage ? `⚙️ Leverage: ${context.leverage}x` : null,
    slotInfo(context),
    principalInfo(context),
    `🧠 Strategy: ${candidate.strategyUsed || candidate.strategySource || "N/A"}`,
    reasons.length ? `✅ Conditions: ${reasons.join(" | ")}` : "✅ Conditions: Matched learned setup",
  ]
    .filter(Boolean)
    .join("\n");
}

function buildSignalReplyMarkup(candidate) {
  return {
    inline_keyboard: [
      [
        {
          text: "Open on Hyperliquid",
          url: hyperliquidLink(candidate.pair),
        },
      ],
    ],
  };
}

function buildScoreRisingMessage({ pair, baseTf, oldScore, newScore, updates = [] }) {
  return [
    `🚀 Score Increased`,
    `🪙 Pair: ${pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(pair)}`,
    `⏱ TF: ${baseTf || "N/A"}`,
    `📈 Score: ${formatScore(oldScore)} → ${formatScore(newScore)}`,
    updates.length ? `✅ Updates: ${updates.join(" | ")}` : null,
  ]
    .filter(Boolean)
    .join("\n");
}

function buildOrderPlacedMessage(trade, ack) {
  return [
    `📌 ORDER PLACED [${modeTag(trade)}]`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `📍 Side: ${trade.side}`,
    `⏱ Base TF: ${trade.baseTimeframe || "N/A"}`,
    `📚 Support TFs: ${(trade.supportTimeframes || []).join(", ") || "N/A"}`,
    `💵 Entry: ${formatPrice(trade.entryPrice)}`,
    `🏁 TP: ${formatPrice(trade.targetPrice)}`,
    `🛑 SL: ${formatPrice(trade.stopLoss)}`,
    `🧮 Qty: ${formatQty(trade.quantity)}`,
    `💰 Notional: ${formatUsd(trade.plannedNotional)}`,
    `⚙️ Leverage: ${trade.leverage}x`,
    slotInfo(trade),
    principalInfo(trade),
    `🧠 Strategy: ${trade.strategyUsed || trade.strategyBucket || "N/A"}`,
    `🆔 Entry Order: ${ack?.oid || trade.entryOrderId || "N/A"}`,
    `Status: Waiting for fill`,
    `Protection: TP and SL will be placed immediately after entry fill.`,
  ]
    .filter(Boolean)
    .join("\n");
}

function buildEntryFilledMessage(trade) {
  return [
    `✅ ENTRY FILLED [${modeTag(trade)}]`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `📍 Side: ${trade.side}`,
    `💵 Fill Price: ${formatPrice(trade.entryPrice)}`,
    `🧮 Size: ${formatQty(trade.filledQuantity || trade.quantity)}`,
    `Status: Placing TP and SL now.`,
    slotInfo(trade),
    principalInfo(trade),
  ]
    .filter(Boolean)
    .join("\n");
}

function buildProtectiveOrdersPlacedMessage(trade) {
  return [
    `🛡️ TP/SL MONITORING STARTED [${modeTag(trade)}]`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `🏁 TP: ${formatPrice(trade.targetPrice)} | ID: ${trade.tpOrderId || "N/A"}`,
    `🛑 SL: ${formatPrice(trade.stopLoss)} | ID: ${trade.slOrderId || "N/A"}`,
    `TP/SL monitoring is now active.`,
  ].join("\n");
}

function buildPublicTargetHitMessage(signal) {
  return [
    `🎯 TARGET ACHIEVED`,
    `🪙 Pair: ${signal.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(signal.pair)}`,
    `📍 Side: ${signal.side}`,
    `⏱ Execution TF: ${signal.baseTimeframe || signal.baseTf || "N/A"}`,
    `💵 Entry: ${formatPrice(signal.entry || signal.entryPrice)}`,
    `🏁 TP: ${formatPrice(signal.tp || signal.targetPrice)}`,
    `🛑 SL: ${formatPrice(signal.sl || signal.stopLoss)}`,
    `🧠 Strategy: ${signal.strategyUsed || signal.strategySource || "N/A"}`,
    `Status: PUBLIC SIGNAL TARGET HIT`,
  ].join("\n");
}

function buildPublicStopHitMessage(signal) {
  return [
    `🛑 STOP LOSS HIT`,
    `🪙 Pair: ${signal.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(signal.pair)}`,
    `📍 Side: ${signal.side}`,
    `⏱ Execution TF: ${signal.baseTimeframe || signal.baseTf || "N/A"}`,
    `💵 Entry: ${formatPrice(signal.entry || signal.entryPrice)}`,
    `🏁 TP: ${formatPrice(signal.tp || signal.targetPrice)}`,
    `🛑 SL: ${formatPrice(signal.sl || signal.stopLoss)}`,
    `🧠 Strategy: ${signal.strategyUsed || signal.strategySource || "N/A"}`,
    `Status: PUBLIC SIGNAL STOP HIT`,
  ].join("\n");
}

function buildTargetHitMessage(trade) {
  return [
    `🎯 TARGET HIT [${modeTag(trade)}]`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `📍 Side: ${trade.side}`,
    `💵 Entry: ${formatPrice(trade.entryPrice)}`,
    `🏁 Exit: ${formatPrice(trade.exitPrice || trade.targetPrice)}`,
    `💹 Gross PnL: ${formatUsd(trade.grossPnl)} | Net PnL: ${formatUsd(trade.netPnl || trade.realizedPnl)}`,
    `💸 Fees: ${formatUsd(trade.fees)} | Funding: ${formatUsd(trade.fundingImpact)}`,
  ].join("\n");
}

function buildStopHitMessage(trade) {
  return [
    `🛑 STOP HIT [${modeTag(trade)}]`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `📍 Side: ${trade.side}`,
    `💵 Entry: ${formatPrice(trade.entryPrice)}`,
    `🚪 Exit: ${formatPrice(trade.exitPrice || trade.stopLoss)}`,
    `💹 Gross PnL: ${formatUsd(trade.grossPnl)} | Net PnL: ${formatUsd(trade.netPnl || trade.realizedPnl)}`,
    `💸 Fees: ${formatUsd(trade.fees)} | Funding: ${formatUsd(trade.fundingImpact)}`,
  ].join("\n");
}

function buildTradeSkippedMessage(event) {
  const candidate = event.candidate || {};
  return [
    `⛔ TRADE SKIPPED`,
    `🪙 Pair: ${candidate.pair || event.trade?.pair || "N/A"}`,
    `🌐 Hyperliquid: ${candidate.pair || event.trade?.pair ? hyperliquidLink(candidate.pair || event.trade?.pair) : "N/A"}`,
    `📍 Side: ${candidate.side || event.trade?.side || "N/A"}`,
    `ℹ️ Reason: ${event.reason || "No reason provided"}`,
    event.cooldownUntil ? `⏳ Cooldown Until: ${event.cooldownUntil}` : null,
  ]
    .filter(Boolean)
    .join("\n");
}

function buildOrderRejectedMessage(event) {
  const trade = event.trade || {};
  const candidate = event.candidate || {};
  const pair = trade.pair || candidate.pair || "N/A";
  return [
    `❌ ORDER REJECTED`,
    `🪙 Pair: ${pair}`,
    `🌐 Hyperliquid: ${pair !== "N/A" ? hyperliquidLink(pair) : "N/A"}`,
    `📍 Side: ${trade.side || candidate.side || "N/A"}`,
    `ℹ️ Reason: ${event.reason || trade.exitReason || event.ack?.error || "Unknown rejection"}`,
  ].join("\n");
}

function buildProfitSweptMessage(event) {
  return [
    `💼 PROFIT RETAINED IN TRADING BALANCE`,
    `🪙 Pair: ${event.trade?.pair || "N/A"}`,
    `🌐 Hyperliquid: ${event.trade?.pair ? hyperliquidLink(event.trade.pair) : "N/A"}`,
    `💰 Amount: ${formatUsd(event.amount)}`,
    `📦 No transfer to spot was performed`,
  ].join("\n");
}

function buildEntryCanceledBeforeFillMessage(trade) {
  return [
    `⚠️ ENTRY CANCELED BEFORE FILL`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `📍 Side: ${trade.side}`,
    ``,
    `Reason:`,
    `Target was reached before entry order was filled.`,
    ``,
    `Status:`,
    `Pending entry canceled safely.`,
  ].join("\n");
}

function buildEntryTimeoutMessage(trade) {
  return [
    `⌛ ENTRY TIMEOUT`,
    `🪙 Pair: ${trade.pair}`,
    `🌐 Hyperliquid: ${hyperliquidLink(trade.pair)}`,
    `📍 Side: ${trade.side}`,
    `ℹ️ Reason: entry-timeout`,
  ].join("\n");
}

function buildModeSwitchBlockedMessage(payload) {
  return [
    `🔒 MODE SWITCH BLOCKED`,
    `🪪 Profile: ${payload.profileLabel || payload.profileId || "N/A"}`,
    `ℹ️ Reason: ${payload.reason || "mode-switch-locked"}`,
    payload.tradeCount != null ? `📂 Blocking Trades: ${payload.tradeCount}` : null,
  ]
    .filter(Boolean)
    .join("\n");
}

function buildReconciliationCompleteMessage(summary) {
  return [
    `🧰 RECONCILIATION COMPLETE`,
    `🪪 Profile: ${summary.profileId || "N/A"}`,
    `✅ Reconciled Trades: ${summary.reconciledTrades || 0}`,
    `🛡 Missing Protections Repaired: ${summary.missingProtectionsRepaired || 0}`,
    `🧹 Stale Orders Canceled: ${summary.staleOrdersCanceled || 0}`,
    `🧷 Orphan Records Found: ${summary.orphanRecordsFound || 0}`,
    (summary.unresolved || []).length
      ? `⚠️ Unresolved: ${summary.unresolved.join(" | ")}`
      : `⚠️ Unresolved: none`,
  ].join("\n");
}

function buildAutomationApprovedMessage(profile) {
  return [
    `✅ AUTOMATION APPROVED`,
    `🪪 Profile: ${profile.label || profile.displayName || profile.id}`,
    `🧾 Status: ${profile.automationStatus || "APPROVED"}`,
  ].join("\n");
}

function buildAutomationDisabledMessage(profile) {
  return [
    `⛔ AUTOMATION DISABLED`,
    `🪪 Profile: ${profile.label || profile.displayName || profile.id}`,
    `🧾 Status: ${profile.automationStatus || "DISABLED"}`,
  ].join("\n");
}

module.exports = {
  buildPublicSignalMessage,
  buildSignalMessage,
  buildSignalReplyMarkup,
  buildScoreRisingMessage,
  buildOrderPlacedMessage,
  buildEntryFilledMessage,
  buildProtectiveOrdersPlacedMessage,
  buildPublicTargetHitMessage,
  buildPublicStopHitMessage,
  buildTargetHitMessage,
  buildStopHitMessage,
  buildTradeSkippedMessage,
  buildOrderRejectedMessage,
  buildProfitSweptMessage,
  buildEntryCanceledBeforeFillMessage,
  buildEntryTimeoutMessage,
  buildModeSwitchBlockedMessage,
  buildReconciliationCompleteMessage,
  buildAutomationApprovedMessage,
  buildAutomationDisabledMessage,
};
