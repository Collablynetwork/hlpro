const config = require("./config");
const state = require("./state");
const tradeManager = require("./dryrun");
const {
  buildSignalMessage,
  buildSignalReplyMarkup,
  buildScoreRisingMessage,
  buildOrderPlacedMessage,
  buildEntryFilledMessage,
  buildProtectiveOrdersPlacedMessage,
  buildTargetHitMessage,
  buildStopHitMessage,
  buildTradeSkippedMessage,
  buildOrderRejectedMessage,
  buildProfitSweptMessage,
  buildEntryCanceledBeforeFillMessage,
  buildEntryTimeoutMessage,
  buildReconciliationCompleteMessage,
} = require("./telegramMessageBuilder");

function getBand(score) {
  const value = Number(score || 0);
  if (value >= config.alertThreshold) return "alert";
  if (value > config.notifyMinScore) return "strong";
  if (value >= config.watchThreshold) return "watch";
  return "low";
}

function bandRank(band) {
  return { low: 0, watch: 1, strong: 2, alert: 3 }[band] ?? 0;
}

function normalizeCandidate(candidate) {
  if (!candidate) return null;

  return {
    ...candidate,
    pair: String(candidate.pair || candidate.symbol || "").toUpperCase(),
    side:
      String(candidate.side || candidate.direction || "LONG").toUpperCase() === "SHORT"
        ? "SHORT"
        : "LONG",
    baseTimeframe: candidate.baseTimeframe || candidate.baseTf || "N/A",
    supportTfs:
      candidate.supportTfs ||
      candidate.supportTimeframes ||
      candidate.supportingTimeframes ||
      candidate.validationTfs ||
      [],
  };
}

function buildSignalKey(candidate) {
  return [
    String(candidate.pair).toUpperCase(),
    String(candidate.side).toUpperCase(),
    String(candidate.baseTimeframe || candidate.baseTf || "N/A"),
  ].join("|");
}

function uniqueStrings(values) {
  return [...new Set((values || []).map((value) => String(value).trim()).filter(Boolean))];
}

function chooseStopAndTargets(side, entry, currentFeatures = {}) {
  const atr = Number(currentFeatures.atr14 || 0);
  const minRisk = Math.max(entry * 0.003, atr * 1.1, entry * 0.0015);
  const longSupport = Number(currentFeatures.support);
  const shortResistance = Number(currentFeatures.resistance);
  const recentLow = Number(currentFeatures.recentLow20);

  let sl;

  if (side === "LONG") {
    const stopCandidate =
      Number.isFinite(longSupport) && longSupport > 0 && longSupport < entry
        ? longSupport
        : entry - minRisk;
    sl = Math.min(stopCandidate, entry - Math.max(minRisk * 0.5, entry * 0.001));
    const risk = Math.max(entry - sl, minRisk);
    return {
      systemTp1: entry + risk,
      systemSl: sl,
      riskReward: risk > 0 ? 1 : null,
    };
  }

  const stopCandidate =
    Number.isFinite(shortResistance) && shortResistance > entry
      ? shortResistance
      : entry + minRisk;
  sl = Math.max(stopCandidate, entry + Math.max(minRisk * 0.5, entry * 0.001));
  const risk = Math.max(sl - entry, minRisk);

  return {
    systemTp1: Number.isFinite(recentLow) ? Math.min(entry - risk, recentLow) : entry - risk,
    systemSl: sl,
    riskReward: risk > 0 ? 1 : null,
  };
}

function buildSignalCandidate(matchResult) {
  if (!matchResult) return null;
  const score = Number(matchResult.score);
  if (!Number.isFinite(score)) return null;
  if (score <= Number(config.notifyMinScore || 80)) return null;

  const side =
    String(matchResult.side || matchResult.direction || "LONG").toUpperCase() === "SHORT"
      ? "SHORT"
      : "LONG";
  const baseTimeframe = matchResult.baseTimeframe || matchResult.baseTf || "N/A";
  if (!config.allowedBaseTimeframes.includes(baseTimeframe)) return null;

  const currentFeatures = matchResult.current?.features || {};
  const entry = Number(
    matchResult.entry ?? matchResult.entryPrice ?? currentFeatures.currentClose ?? 0
  );
  if (!Number.isFinite(entry) || entry <= 0) return null;

  const supportTfsRaw =
    matchResult.supportTfs ||
    matchResult.supportTimeframes ||
    matchResult.supportingTimeframes ||
    matchResult.validationTfs ||
    [];
  const supportTfs = uniqueStrings([baseTimeframe, ...supportTfsRaw]);
  if (supportTfs.length < Number(config.minSupportCount || 3)) return null;

  const generated = chooseStopAndTargets(side, entry, currentFeatures);
  const targetPrice = Number(matchResult.tp1 ?? generated.systemTp1);
  const stopLoss = Number(matchResult.sl ?? matchResult.stopLoss ?? generated.systemSl);
  const strategySourcePair =
    matchResult.strategySourcePair || matchResult.sourcePair || matchResult.strategy?.pair || "N/A";
  const strategySourceTimeframe =
    matchResult.strategySourceTimeframe ||
    matchResult.sourceTimeframe ||
    matchResult.strategy?.mainSourceTimeframe ||
    "N/A";
  const strategyUsed = `${strategySourcePair} ${strategySourceTimeframe}`.trim();

  return {
    pair: String(matchResult.pair || "").toUpperCase(),
    side,
    direction: side,
    score,
    entry,
    entryPrice: entry,
    currentPrice: Number(matchResult.currentPrice ?? currentFeatures.currentClose ?? entry),
    targetPrice,
    stopLoss,
    tp1: targetPrice,
    sl: stopLoss,
    baseTimeframe,
    baseTf: baseTimeframe,
    supportTfs,
    supportTimeframes: supportTfs,
    reasons: matchResult.reasons || [],
    strategySourcePair,
    strategySourceTimeframe,
    strategySource: strategyUsed,
    strategyUsed,
    similarityScore: Number(matchResult.similarityScore || score),
    riskReward: Number(matchResult.riskReward ?? generated.riskReward),
    regimeSupportScore: matchResult.regimeSupportScore ?? null,
  };
}

async function sendNewSignal(bot, chatId, candidate, options = {}) {
  if (!bot || !chatId) return null;

  const runtime = state.getRuntimeSettings(options.profileId || state.DEFAULT_PROFILE_ID);
  const capitalStatus = await tradeManager.getCapitalStatus(options.profileId || state.DEFAULT_PROFILE_ID).catch(
    () => null
  );
  const capital = capitalStatus?.capitalSnapshot || {};
  const text = buildSignalMessage(candidate, {
    capitalMode: runtime.capitalMode,
    executionMode: runtime.executionMode,
    leverage: runtime.tradeLeverage,
    simpleSlots: capital.simpleSlots || runtime.simpleSlots,
    slotSize: runtime.capitalMode === "SIMPLE" ? capital.slotSize : null,
    principalUsed:
      runtime.capitalMode === "SIMPLE"
        ? capital.slotSize
        : capital.tradePrincipal || capital.currentPrincipal || runtime.tradeBalanceTarget || runtime.baselinePrincipal || 0,
  });
  const replyMarkup = buildSignalReplyMarkup(candidate);

  const optionsPayload = {
    disable_web_page_preview: true,
  };
  if (replyMarkup) {
    optionsPayload.reply_markup = replyMarkup;
  }

  return bot.sendMessage(chatId, text, optionsPayload);
}

async function sendScoreRise(bot, chatId, previous, current) {
  if (!bot || !chatId || !previous?.messageId) return null;

  const text = buildScoreRisingMessage({
    pair: current.pair,
    baseTf: current.baseTimeframe,
    oldScore: previous.score,
    newScore: current.score,
    updates: current.reasons?.slice(0, 4) || [],
  });

  return bot.sendMessage(chatId, text, {
    reply_to_message_id: previous.messageId,
  });
}

function dedupeCandidates(candidates) {
  const byKey = new Map();
  for (const raw of candidates || []) {
    const candidate = normalizeCandidate(raw);
    if (!candidate) continue;
    const key = buildSignalKey(candidate);
    const existing = byKey.get(key);
    if (!existing || Number(candidate.score || 0) > Number(existing.score || 0)) {
      byKey.set(key, candidate);
    }
  }
  return [...byKey.values()].sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
}

async function dispatchSignals(bot, chatId, candidates, options = {}) {
  const profileId = options.profileId || state.DEFAULT_PROFILE_ID;
  const deduped = dedupeCandidates(candidates);
  if (!deduped.length) return [];

  const activeSignals = state.getActiveSignals(profileId);
  const results = [];
  let dirty = false;

  for (const candidate of deduped) {
    const signalKey = buildSignalKey(candidate);
    const band = getBand(candidate.score);
    const previous = activeSignals[signalKey];

    if (!previous) {
      const sent = await sendNewSignal(bot, chatId, candidate, { profileId });
      const candidateWithMessage = {
        ...candidate,
        signalKey,
        signalMessageId: sent?.message_id || null,
      };
      const registered = await tradeManager.registerSignal(candidateWithMessage, {
        profileId,
        signalMessageId: sent?.message_id || null,
      });
      const skipped = registered.events?.some((event) => event.type === "TRADE_SKIPPED");

      if (registered.trade && !skipped) {
        state.setSnapshot(`profile:${profileId}:lastSignal`, {
          timestamp: new Date().toISOString(),
          pair: candidate.pair,
          side: candidate.side,
          baseTimeframe: candidate.baseTimeframe,
          score: candidate.score,
          signalKey,
        });
        tradeManager.attachSignalMessage(
          registered.trade.id || registered.trade.signalId,
          sent?.message_id || null,
          signalKey
        );
        activeSignals[signalKey] = {
          ...candidate,
          band,
          messageId: sent?.message_id || null,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };
        dirty = true;
      }

      if (registered.events?.length) {
        await dispatchTradeUpdates(bot, chatId, registered.events, { profileId });
      }

      results.push({ type: "new", key: signalKey, candidate });
      continue;
    }

    const scoreRise = Number(candidate.score || 0) - Number(previous.score || 0);
    const raisedBand = bandRank(band) > bandRank(previous.band);

    if (scoreRise > 0 && (raisedBand || scoreRise >= config.scoreRiseThreshold)) {
      await sendScoreRise(bot, chatId, previous, candidate);
      activeSignals[signalKey] = {
        ...previous,
        ...candidate,
        band,
        updatedAt: new Date().toISOString(),
      };
      dirty = true;
      results.push({ type: "rise", key: signalKey, candidate });
      continue;
    }

    activeSignals[signalKey] = {
      ...previous,
      ...candidate,
      band: previous.band || band,
      updatedAt: new Date().toISOString(),
    };
    dirty = true;
  }

  if (dirty) {
    state.saveActiveSignals(profileId, activeSignals);
  }

  return results;
}

function eventMessage(update) {
  switch (update.type) {
    case "ORDER_PLACED":
      return buildOrderPlacedMessage(update.trade, update.ack);
    case "ENTRY_FILLED":
      return buildEntryFilledMessage(update.trade);
    case "PROTECTIVE_ORDERS_PLACED":
      return buildProtectiveOrdersPlacedMessage(update.trade);
    case "TARGET_HIT":
      return buildTargetHitMessage(update.trade);
    case "STOP_HIT":
      return buildStopHitMessage(update.trade);
    case "TRADE_SKIPPED":
      return buildTradeSkippedMessage(update);
    case "ORDER_REJECTED":
      return buildOrderRejectedMessage(update);
    case "PROFIT_SWEPT":
      return buildProfitSweptMessage(update);
    case "ENTRY_CANCELED_BEFORE_FILL":
      return buildEntryCanceledBeforeFillMessage(update.trade);
    case "ENTRY_TIMEOUT":
      return buildEntryTimeoutMessage(update.trade);
    case "RECONCILIATION_COMPLETE":
      return buildReconciliationCompleteMessage(update.summary || update);
    default:
      return "";
  }
}

function isTerminalEvent(update) {
  return [
    "TARGET_HIT",
    "STOP_HIT",
    "ORDER_REJECTED",
    "ENTRY_CANCELED_BEFORE_FILL",
    "ENTRY_TIMEOUT",
  ].includes(update.type);
}

async function dispatchTradeUpdates(bot, chatId, updates, options = {}) {
  const profileId = options.profileId || state.DEFAULT_PROFILE_ID;
  if (!bot || !chatId || !Array.isArray(updates) || !updates.length) return [];

  const activeSignals = state.getActiveSignals(profileId);
  const sent = [];
  let dirty = false;

  for (const update of updates) {
    const trade = update.trade || {};
    const replyTo =
      trade.signalMessageId || trade.messageId || update.candidate?.signalMessageId || null;
    const text = eventMessage(update);

    if (!text) continue;

    const message = await bot.sendMessage(
      chatId,
      text,
      replyTo ? { reply_to_message_id: replyTo } : {}
    );
    sent.push(message);

    const signalKey = trade.signalKey || update.candidate?.signalKey || null;
    if (signalKey && activeSignals[signalKey] && isTerminalEvent(update)) {
      delete activeSignals[signalKey];
      dirty = true;
    }
  }

  if (dirty) {
    state.saveActiveSignals(profileId, activeSignals);
  }

  return sent;
}

module.exports = {
  buildSignalCandidate,
  dispatchSignals,
  dispatchTradeUpdates,
  buildSignalKey,
};
