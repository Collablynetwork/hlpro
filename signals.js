const config = require("./config");
const state = require("./state");
const tradeManager = require("./dryrun");
const hyperliquid = require("./hyperliquid");
const pairUniverse = require("./pairUniverse");
const {
  buildPublicSignalMessage,
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

function toNumOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function buildSignalCandidate(matchResult) {
  if (!matchResult) return null;
  const score = Number(matchResult.score || 0);
  if (!Number.isFinite(score)) return null;

  if (!(score > Number(config.notifyAboveScore || config.notifyMinScore || 80))) {
    return null;
  }

  const side =
    String(matchResult.side || matchResult.direction || "LONG").toUpperCase() === "SHORT"
      ? "SHORT"
      : "LONG";
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

    entry: toNumOrNull(matchResult.entry ?? matchResult.entryPrice),
    entryPrice: toNumOrNull(matchResult.entry ?? matchResult.entryPrice),
    currentPrice: toNumOrNull(matchResult.currentPrice),

    sl: toNumOrNull(matchResult.sl ?? matchResult.stopLoss),
    stopLoss: toNumOrNull(matchResult.sl ?? matchResult.stopLoss),

    tp1: toNumOrNull(matchResult.tp1),
    tp2: toNumOrNull(matchResult.tp2),
    tp3: toNumOrNull(matchResult.tp3),

    baseTimeframe: matchResult.baseTimeframe || matchResult.baseTf || "N/A",
    baseTf: matchResult.baseTimeframe || matchResult.baseTf || "N/A",
    supportTfs:
      matchResult.supportTfs ||
      matchResult.supportingTimeframes ||
      matchResult.supportTimeframes ||
      matchResult.validationTfs ||
      [],
    supportTimeframes:
      matchResult.supportTfs ||
      matchResult.supportingTimeframes ||
      matchResult.supportTimeframes ||
      matchResult.validationTfs ||
      [],
    reasons: matchResult.reasons || [],
    strategySourcePair,
    strategySourceTimeframe,
    strategySource: strategyUsed,
    strategyUsed,
    similarityScore: Number(matchResult.similarityScore || score),
    riskReward: matchResult.riskReward || null,
    regimeSupportScore: matchResult.regimeSupportScore ?? null,
    discoveryType: "watched",
  };
}

async function sendNewSignal(bot, chatId, candidate, options = {}) {
  if (!bot || !chatId) return null;

  const text = buildPublicSignalMessage(candidate);
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

function mergeActiveSignal(previous, candidate, band) {
  return {
    ...previous,
    ...candidate,
    band: previous?.band || band,
    updatedAt: new Date().toISOString(),
  };
}

function resolveReplyMessageId(update, activeSignals = {}) {
  const trade = update.trade || {};
  if (trade.signalMessageId || trade.messageId || update.candidate?.signalMessageId) {
    return trade.signalMessageId || trade.messageId || update.candidate?.signalMessageId || null;
  }

  const signalKey = trade.signalKey || update.candidate?.signalKey || null;
  if (signalKey && activeSignals[signalKey]?.messageId) {
    return activeSignals[signalKey].messageId;
  }

  const pair = String(trade.pair || update.candidate?.pair || "").toUpperCase();
  if (!pair) return null;
  const fallback = Object.values(activeSignals).find(
    (entry) => String(entry?.pair || "").toUpperCase() === pair && entry?.messageId
  );
  return fallback?.messageId || null;
}

function resolvePublicChatId(chatId = null) {
  const configured = String(config.telegramChatId || "").trim();
  if (configured) return configured;
  const fallback = String(chatId || "").trim();
  return fallback && fallback.startsWith("-") ? fallback : null;
}

function logPrivateDeliveryIssue(profileId, message, extra = {}) {
  const payload = {
    timestamp: state.nowIso(),
    message,
    ...extra,
  };
  state.setSnapshot(`profile:${profileId}:lastPrivateDeliveryError`, payload);
  console.warn(message, extra?.error || "");
}

function isPrivateDeliveryError(error) {
  return /chat not found|bot was blocked by the user|bot can't initiate conversation|forbidden|user is deactivated/i.test(
    String(error?.message || "")
  );
}

async function sendUserTradeUpdate(bot, profileId, text, options = {}) {
  if (!bot || !text) return null;
  const chatId = options.chatId || state.getPrivateChatId(profileId);
  if (!chatId) {
    logPrivateDeliveryIssue(
      profileId,
      "Cannot send private message to user because user has not started the bot.",
      { scope: options.scope || "user-trade-update" }
    );
    return null;
  }

  try {
    return await bot.sendMessage(chatId, text);
  } catch (error) {
    if (isPrivateDeliveryError(error)) {
      logPrivateDeliveryIssue(
        profileId,
        "Cannot send private message to user because user has not started the bot.",
        {
          scope: options.scope || "user-trade-update",
          error: error.message,
        }
      );
      return null;
    }
    throw error;
  }
}

function buildPublicSignalRecord(candidate, publicChatId, messageId) {
  const signalKey = buildSignalKey(candidate);
  return {
    ...candidate,
    signalId: `${signalKey}:${Date.now()}`,
    signalKey,
    signalMessageId: messageId || null,
    groupChatId: publicChatId,
    groupMessageId: messageId || null,
    status: "ACTIVE",
    createdAt: state.nowIso(),
    updatedAt: state.nowIso(),
  };
}

async function dispatchPublicSignal(bot, chatId, candidates) {
  const publicChatId = resolvePublicChatId(chatId);
  const deduped = dedupeCandidates(candidates);
  if (!bot || !publicChatId || !deduped.length) {
    return {
      dispatched: false,
      reason: deduped.length ? "missing-public-chat" : "no-candidates",
      publicSignal: state.getPublicSignalState(publicChatId) || null,
    };
  }

  const activePublicSignal = state.getPublicSignalState(publicChatId);
  if (activePublicSignal?.status === "ACTIVE") {
    return {
      dispatched: false,
      reason: "active-public-signal",
      publicSignal: activePublicSignal,
    };
  }

  const candidate = deduped[0];
  const sent = await sendNewSignal(bot, publicChatId, candidate);
  const publicSignal = state.setPublicSignalState(
    publicChatId,
    buildPublicSignalRecord(candidate, publicChatId, sent?.message_id || null)
  );
  state.setSnapshot("system:lastPublicSignal", publicSignal);

  return {
    dispatched: true,
    candidate,
    publicSignal,
    message: sent,
  };
}

async function registerSignalForProfile(bot, publicSignal, options = {}) {
  const profileId = options.profileId || state.DEFAULT_PROFILE_ID;
  const candidate = normalizeCandidate(publicSignal);
  const signalKey = buildSignalKey(candidate);
  const preview = await tradeManager.previewSignalRegistration(candidate, {
    profileId,
    signalMessageId: publicSignal.groupMessageId || publicSignal.signalMessageId || null,
  });

  if (!preview.ok) {
    if (preview.events?.length) {
      await dispatchTradeUpdates(bot, null, preview.events, { profileId });
    }
    return {
      profileId,
      trade: preview.trade || null,
      skipped: true,
      reason: preview.reason || preview.events?.[0]?.reason || "trade-skipped",
      events: preview.events || [],
    };
  }

  const registered = await tradeManager.registerSignal(
    {
      ...candidate,
      signalKey,
      signalMessageId: publicSignal.groupMessageId || null,
    },
    {
      profileId,
      signalMessageId: publicSignal.groupMessageId || null,
      prepared: {
        ...preview,
        signalMessageId: publicSignal.groupMessageId || preview.signalMessageId || null,
      },
    }
  );
  const skipped = registered.events?.some((event) => event.type === "TRADE_SKIPPED");

  if (registered.trade && !skipped) {
    state.setSnapshot(`profile:${profileId}:lastSignal`, {
      timestamp: state.nowIso(),
      pair: candidate.pair,
      side: candidate.side,
      baseTimeframe: candidate.baseTimeframe,
      score: candidate.score,
      signalKey,
      publicSignalId: publicSignal.signalId,
      publicGroupMessageId: publicSignal.groupMessageId || null,
    });
    tradeManager.attachSignalMessage(
      registered.trade.id || registered.trade.signalId,
      publicSignal.groupMessageId || null,
      signalKey
    );

    const activeSignals = state.getActiveSignals(profileId);
    activeSignals[signalKey] = {
      ...candidate,
      groupMessageId: publicSignal.groupMessageId || null,
      messageId: publicSignal.groupMessageId || null,
      createdAt: state.nowIso(),
      updatedAt: state.nowIso(),
    };
    state.saveActiveSignals(profileId, activeSignals);
  }

  if (registered.events?.length) {
    await dispatchTradeUpdates(bot, null, registered.events, { profileId });
  }

  return {
    profileId,
    trade: registered.trade || null,
    skipped,
    reason: skipped
      ? registered.events?.find((event) => event.type === "TRADE_SKIPPED")?.reason || "trade-skipped"
      : null,
    events: registered.events || [],
  };
}

async function sendPublicSignalResult(bot, publicSignal, kind) {
  const publicChatId = resolvePublicChatId(publicSignal?.groupChatId);
  if (!bot || !publicChatId || !publicSignal) return null;

  const text =
    kind === "tp" ? buildPublicTargetHitMessage(publicSignal) : buildPublicStopHitMessage(publicSignal);
  const replyOptions = publicSignal.groupMessageId
    ? { reply_to_message_id: publicSignal.groupMessageId }
    : {};

  try {
    return await bot.sendMessage(publicChatId, text, replyOptions);
  } catch (error) {
    if (
      publicSignal.groupMessageId &&
      /reply message not found|message to reply not found/i.test(String(error.message || ""))
    ) {
      return bot.sendMessage(publicChatId, text);
    }
    throw error;
  }
}

async function syncPublicSignalStatus(bot, options = {}) {
  const publicChatId = resolvePublicChatId(options.chatId);
  if (!bot || !publicChatId) return null;

  const publicSignal = state.getPublicSignalState(publicChatId);
  if (!publicSignal || publicSignal.status !== "ACTIVE") {
    return publicSignal;
  }

  const allMids = options.allMids || (await hyperliquid.getAllMids());
  const coin = String(publicSignal.coin || pairUniverse.coinFromPair(publicSignal.pair) || "").toUpperCase();
  const marketPrice = Number(allMids?.[coin] || 0);
  const targetPrice = Number(publicSignal.tp);
  const stopPrice = Number(publicSignal.sl);
  if (!Number.isFinite(marketPrice) || marketPrice <= 0) {
    return publicSignal;
  }
  if (!Number.isFinite(targetPrice) || targetPrice <= 0 || !Number.isFinite(stopPrice) || stopPrice <= 0) {
    return publicSignal;
  }

  const hitTarget =
    publicSignal.side === "LONG"
      ? marketPrice >= targetPrice
      : marketPrice <= targetPrice;
  const hitStop =
    publicSignal.side === "LONG"
      ? marketPrice <= stopPrice
      : marketPrice >= stopPrice;

  if (!hitTarget && !hitStop) {
    return publicSignal;
  }

  const outcomeKind = hitTarget ? "tp" : "sl";
  const resultMessage = await sendPublicSignalResult(bot, publicSignal, outcomeKind);
  const closed = state.closePublicSignalState(
    publicChatId,
    outcomeKind === "tp" ? "TP_HIT" : "SL_HIT",
    {
      resultMessageId: resultMessage?.message_id || null,
      updatedAt: state.nowIso(),
      closedAt: state.nowIso(),
    }
  );
  state.setSnapshot("system:lastPublicSignal", closed);
  return closed;
}

async function dispatchSignals(bot, chatId, candidates, options = {}) {
  const profileId = options.profileId || state.DEFAULT_PROFILE_ID;
  const deduped = dedupeCandidates(candidates);
  if (!deduped.length) return [];

  const activeSignals = state.getActiveSignals(profileId);
  const signalBudget = tradeManager.getSignalDispatchBudget(profileId);
  const activePairs = new Set(signalBudget.activePairs || []);
  const reservedPairs = new Set(signalBudget.activePairs || []);
  const results = [];
  let dirty = false;

  for (const candidate of deduped) {
    const signalKey = buildSignalKey(candidate);
    const pair = String(candidate.pair || "").toUpperCase();
    const band = getBand(candidate.score);
    const previous = activeSignals[signalKey] || null;

    if (previous && !activePairs.has(pair)) {
      delete activeSignals[signalKey];
      dirty = true;
    }

    if (reservedPairs.has(pair)) {
      if (previous) {
        activeSignals[signalKey] = mergeActiveSignal(previous, candidate, band);
        dirty = true;
      }
      results.push({ type: "suppressed", key: signalKey, candidate, reason: "pair-already-open" });
      continue;
    }

    const preview = await tradeManager.previewSignalRegistration(candidate, { profileId });
    if (!preview.ok) {
      results.push({
        type: "suppressed",
        key: signalKey,
        candidate,
        reason: preview.reason || preview.events?.[0]?.reason || "trade-skipped",
      });
      continue;
    }

    const sent = await sendNewSignal(bot, chatId, candidate, { profileId });
    reservedPairs.add(pair);

    const candidateWithMessage = {
      ...candidate,
      signalKey,
      signalMessageId: sent?.message_id || null,
    };
    const registered = await tradeManager.registerSignal(candidateWithMessage, {
      profileId,
      signalMessageId: sent?.message_id || null,
      prepared: {
        ...preview,
        signalMessageId: sent?.message_id || preview.signalMessageId || null,
      },
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
      activePairs.add(pair);
      dirty = true;
    }

    if (registered.events?.length) {
      await dispatchTradeUpdates(bot, chatId, registered.events, { profileId });
    }

    results.push({
      type: skipped ? "suppressed" : "new",
      key: signalKey,
      candidate,
      reason: skipped ? registered.events?.find((event) => event.type === "TRADE_SKIPPED")?.reason : null,
    });
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
  if (!bot || !Array.isArray(updates) || !updates.length) return [];

  const activeSignals = state.getActiveSignals(profileId);
  const privateChatId = state.getPrivateChatId(profileId);
  const sent = [];
  let dirty = false;

  if (!privateChatId) {
    logPrivateDeliveryIssue(
      profileId,
      "Cannot send private message to user because user has not started the bot.",
      { scope: "user-trade-update-batch" }
    );
  }

  for (const update of updates) {
    const trade = update.trade || {};
    const text = eventMessage(update);

    if (!text) continue;

    if (privateChatId) {
      try {
        const message = await sendUserTradeUpdate(bot, profileId, text, {
          scope: update.type || "user-trade-update",
          chatId: privateChatId,
        });
        if (message) {
          sent.push(message);
        }
      } catch (error) {
        console.error(`Private trade update failed for ${profileId}:`, error.message);
      }
    }

    const signalKey = trade.signalKey || update.candidate?.signalKey || null;
    if (isTerminalEvent(update)) {
      if (signalKey && activeSignals[signalKey]) {
        delete activeSignals[signalKey];
        dirty = true;
      }

      const pair = String(trade.pair || update.candidate?.pair || "").toUpperCase();
      if (pair) {
        for (const [key, value] of Object.entries(activeSignals)) {
          if (String(value?.pair || "").toUpperCase() === pair) {
            delete activeSignals[key];
            dirty = true;
          }
        }
      }
    }
  }

  if (dirty) {
    state.saveActiveSignals(profileId, activeSignals);
  }

  return sent;
}

module.exports = {
  buildSignalCandidate,
  dispatchPublicSignal,
  registerSignalForProfile,
  syncPublicSignalStatus,
  dispatchSignals,
  dispatchTradeUpdates,
  buildSignalKey,
};
