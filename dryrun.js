const crypto = require("crypto");
const config = require("./config");
const state = require("./state");
const hyperliquid = require("./hyperliquid");
const pairUniverse = require("./pairUniverse");
const binance = require("./binance");
const { round } = require("./indicators");

const ONE_HOUR_MS = 60 * 60 * 1000;
const TWO_HOURS_MS = 2 * ONE_HOUR_MS;
const MIN_NOTIONAL_USD = 10;
const ACTIVE_STATUSES = new Set([
  "ENTRY_PENDING",
  "ENTRY_PLACED",
  "PARTIALLY_FILLED",
  "OPEN",
  "PROTECTED",
  "EXIT_PENDING",
  "RECONCILING",
]);
const TERMINAL_ORDER_STATUSES = new Set([
  "filled",
  "canceled",
  "triggered",
  "rejected",
  "marginCanceled",
  "vaultWithdrawalCanceled",
  "openInterestCapCanceled",
  "selfTradeCanceled",
  "reduceOnlyCanceled",
  "siblingFilledCanceled",
  "delistedCanceled",
  "liquidatedCanceled",
  "scheduledCancel",
  "tickRejected",
  "minTradeNtlRejected",
  "perpMarginRejected",
  "reduceOnlyRejected",
  "badAloPxRejected",
  "iocCancelRejected",
  "badTriggerPxRejected",
  "marketOrderNoLiquidityRejected",
  "positionIncreaseAtOpenInterestCapRejected",
  "positionFlipAtOpenInterestCapRejected",
  "tooAggressiveAtOpenInterestCapRejected",
  "openInterestIncreaseRejected",
  "insufficientSpotBalanceRejected",
  "oracleRejected",
  "perpMaxPositionRejected",
]);

function uniqueStrings(values) {
  return [...new Set((values || []).map((value) => String(value).trim()).filter(Boolean))];
}

function loadOpenPositions(profileId = null) {
  return state.listOpenTrades(profileId);
}

function saveOpenPositions(profileId = null) {
  return state.listOpenTrades(profileId);
}

function loadClosedTrades(profileOrLimit = 0, maybeLimit = 0) {
  if (typeof profileOrLimit === "string") {
    return state.listClosedTradesByProfile(profileOrLimit, maybeLimit);
  }
  return state.listClosedTrades(profileOrLimit);
}

function saveClosedTrades(profileOrLimit = 0, maybeLimit = 0) {
  return loadClosedTrades(profileOrLimit, maybeLimit);
}

function computeSignedPct(side, entry, exitPrice) {
  if (!Number.isFinite(entry) || entry <= 0 || !Number.isFinite(exitPrice)) return 0;
  if (String(side).toUpperCase() === "SHORT") {
    return ((entry - exitPrice) / entry) * 100;
  }
  return ((exitPrice - entry) / entry) * 100;
}

function computeSignedAmount(side, entry, exitPrice, quantity) {
  if (!Number.isFinite(entry) || entry <= 0 || !Number.isFinite(exitPrice) || !Number.isFinite(quantity)) {
    return 0;
  }
  const move = String(side).toUpperCase() === "SHORT" ? entry - exitPrice : exitPrice - entry;
  return round(move * quantity, 6);
}

function getSignalKey(signal) {
  return String(
    signal.signalKey ||
      [signal.pair, signal.side, signal.baseTimeframe]
        .map((value) => String(value || "").toUpperCase())
        .join("|")
  );
}

function createCloid() {
  return `0x${crypto.randomBytes(16).toString("hex")}`;
}

function buildIdempotencyKey(candidate, profileId) {
  const hash = crypto.createHash("sha256");
  hash.update(
    JSON.stringify({
      profileId,
      pair: String(candidate.pair || "").toUpperCase(),
      side: String(candidate.side || "").toUpperCase(),
      baseTimeframe: String(candidate.baseTimeframe || candidate.baseTf || ""),
      targetPrice: Number(candidate.targetPrice || candidate.tp1 || 0),
      stopLoss: Number(candidate.stopLoss || candidate.sl || 0),
    })
  );
  return hash.digest("hex");
}

function parseOrderStatuses(rawResponse) {
  const statuses =
    rawResponse?.response?.data?.statuses ||
    rawResponse?.data?.statuses ||
    rawResponse?.statuses ||
    [];

  return statuses.map((status) => {
    if (status?.filled) {
      return {
        kind: "filled",
        oid: Number(status.filled.oid),
        totalSz: Number(status.filled.totalSz || 0),
        avgPx: Number(status.filled.avgPx || 0),
        raw: status,
      };
    }
    if (status?.resting) {
      return {
        kind: "resting",
        oid: Number(status.resting.oid),
        raw: status,
      };
    }
    if (status?.error) {
      return {
        kind: "error",
        error: String(status.error),
        raw: status,
      };
    }
    if (typeof status === "string") {
      return {
        kind: "text",
        value: status,
        raw: status,
      };
    }
    return {
      kind: "unknown",
      raw: status,
    };
  });
}

function normalizeOrderStatus(orderStatus) {
  if (!orderStatus) return "unknown";
  return String(orderStatus.status || orderStatus.order?.status || orderStatus).trim();
}

function roundDown(value, decimals) {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** Math.max(0, Number(decimals || 0));
  return Math.floor(value * factor) / factor;
}

function buildFillKey(fill) {
  return [
    fill.hash || "nohash",
    fill.oid,
    fill.time,
    fill.tid || "notid",
    fill.px,
    fill.sz,
  ].join("|");
}

function normalizeFill(fill) {
  return {
    oid: Number(fill.oid),
    px: Number(fill.px || 0),
    sz: Number(fill.sz || 0),
    time: Number(fill.time || Date.now()),
    dir: fill.dir || "",
    hash: fill.hash || null,
    tid: fill.tid || null,
    fee: Number(fill.fee || 0),
    feeToken: fill.feeToken || "USDC",
    closedPnl: Number(fill.closedPnl || 0),
  };
}

function getTradeStatus(trade) {
  return String(trade.status || "").toUpperCase();
}

function isTradeOpen(trade) {
  return ACTIVE_STATUSES.has(getTradeStatus(trade));
}

function getAllTrades(profileId = null) {
  return state.listAllTrades(profileId);
}

function getProfileAuth(profileId) {
  return state.getProfileExecutionCredentials(profileId) || {};
}

function getAccountValue(userState) {
  return Number(
    userState?.marginSummary?.accountValue ||
      userState?.crossMarginSummary?.accountValue ||
      userState?.withdrawable ||
      0
  );
}

function getWithdrawable(userState) {
  return Number(userState?.withdrawable || 0);
}

function getOpenPositionSizeMap(userState) {
  const positions = Array.isArray(userState?.assetPositions) ? userState.assetPositions : [];
  const map = new Map();
  for (const row of positions) {
    const position = row?.position || row;
    const coin = String(position?.coin || position?.name || "").toUpperCase();
    const size = Number(position?.szi || position?.sz || 0);
    if (!coin || !size) continue;
    map.set(coin, size);
  }
  return map;
}

function normalizeOpenOrders(openOrders) {
  return (openOrders || []).map((row) => ({
    oid: Number(row.oid || row.orderId || 0),
    coin: String(row.coin || row.name || "").toUpperCase(),
    cloid: row.cloid || row.clientOrderId || null,
    isTrigger: Boolean(row.isTrigger || row.triggerPx || row.triggerCondition || row.orderType?.trigger),
    reduceOnly: Boolean(row.reduceOnly || row.reduce_only),
    side: row.side || row.dir || null,
    raw: row,
  }));
}

function isProfileApprovedForReal(profile) {
  return Boolean(
    profile &&
      profile.status === "ACTIVE" &&
      profile.walletEnabled &&
      profile.automationEnabled &&
      profile.automationStatus === "APPROVED"
  );
}

function estimateFundingImpact(trade, closeTimestampMs) {
  const openMs = new Date(trade.filledAt || trade.openedAt || Date.now()).getTime();
  const elapsedHours = Math.max(0, (closeTimestampMs - openMs) / (60 * 60 * 1000));
  const positionNotional = Number(trade.positionNotional || trade.plannedNotional || 0);
  const configuredRate = Number(config.demoFundingRatePerHour || 0);
  const sideSign = String(trade.side || "").toUpperCase() === "SHORT" ? 1 : -1;
  return round(positionNotional * configuredRate * elapsedHours * sideSign, 6);
}

function demoFeeForNotional(notional) {
  return round(Math.abs(Number(notional || 0)) * Number(config.demoFeeRate || 0), 6);
}

async function getMarketPriceForPair(pair, allMids = null) {
  const coin = pairUniverse.coinFromPair(pair);
  if (allMids?.[coin] != null) {
    return Number(allMids[coin]);
  }
  const mids = await hyperliquid.getAllMids();
  return Number(mids?.[coin] || 0);
}

function buildAllocationPlan(candidate, profileId = state.DEFAULT_PROFILE_ID) {
  const pair = String(candidate.pair || "").toUpperCase();
  const side = String(candidate.side || "LONG").toUpperCase();
  const entryPrice = Number(candidate.entryPrice || candidate.entry || 0);
  const pairState = state.getPairState(profileId, pair) || {};
  const now = Date.now();
  const lastClosedAtMs = pairState.lastClosedAt ? new Date(pairState.lastClosedAt).getTime() : 0;
  const withinHour = lastClosedAtMs > 0 && now - lastClosedAtMs <= ONE_HOUR_MS;
  const sameSide = String(pairState.lastSide || "").toUpperCase() === side;
  const higherEntry = entryPrice > Number(pairState.lastEntryPrice || 0);
  const reverseOrLower = !sameSide || !higherEntry;
  const cooldownUntilMs = pairState.cooldownUntil
    ? new Date(pairState.cooldownUntil).getTime()
    : 0;
  const inCooldown = cooldownUntilMs > now;

  if (!withinHour || reverseOrLower) {
    return {
      accept: true,
      allocationPct: 1,
      repeatLevel: 0,
      reason: reverseOrLower ? "reverse-or-lower-entry" : "fresh-window",
      cooldownUntil: null,
    };
  }

  if (inCooldown) {
    return {
      accept: false,
      allocationPct: 0,
      repeatLevel: Number(pairState.repeatLevel || 2),
      reason: "pair-cooldown",
      cooldownUntil: pairState.cooldownUntil,
    };
  }

  const previousRepeatLevel = Number(pairState.repeatLevel || 0);
  const nextRepeatLevel = previousRepeatLevel + 1;

  if (nextRepeatLevel === 1) {
    return {
      accept: true,
      allocationPct: 0.25,
      repeatLevel: 1,
      reason: "repeat-higher-entry-25",
      cooldownUntil: null,
    };
  }

  if (nextRepeatLevel === 2) {
    return {
      accept: true,
      allocationPct: 0.1,
      repeatLevel: 2,
      reason: "repeat-higher-entry-10",
      cooldownUntil: null,
    };
  }

  const cooldownUntil = new Date(now + TWO_HOURS_MS).toISOString();
  state.savePairState(profileId, pair, {
    ...pairState,
    pair,
    profileId,
    lastSide: pairState.lastSide || side,
    lastEntryPrice: Number(pairState.lastEntryPrice || entryPrice),
    lastClosedAt: pairState.lastClosedAt || new Date(now).toISOString(),
    repeatLevel: Number(pairState.repeatLevel || 2),
    cooldownUntil,
  });

  return {
    accept: false,
    allocationPct: 0,
    repeatLevel: Number(pairState.repeatLevel || 2),
    reason: "repeat-higher-entry-blocked-2h",
    cooldownUntil,
  };
}

function getActiveTradeCount(profileId) {
  return loadOpenPositions(profileId).filter((trade) => isTradeOpen(trade)).length;
}

function reservedDemoMargin(profileId) {
  return loadOpenPositions(profileId)
    .filter((trade) => trade.executionMode === "DEMO" && isTradeOpen(trade))
    .reduce((sum, trade) => sum + Number(trade.plannedMargin || 0), 0);
}

async function buildCapitalSnapshot(profileId, options = {}) {
  const runtime = state.getRuntimeSettings(profileId);
  const executionMode = options.executionMode || runtime.executionMode;
  const openTrades = loadOpenPositions(profileId).filter((trade) => isTradeOpen(trade));
  const activeTradeCount = openTrades.length;

  let accountValue = 0;
  let withdrawable = 0;
  let userState = options.userState || null;

  if (executionMode === "REAL") {
    if (!userState) {
      userState = await hyperliquid.getUserState(getProfileAuth(profileId));
    }
    accountValue = getAccountValue(userState);
    withdrawable = getWithdrawable(userState);
  } else {
    accountValue = Number(runtime.demoPerpBalance || 0);
    withdrawable = Math.max(0, accountValue - reservedDemoMargin(profileId));
  }

  const selectedPrincipal = Number(runtime.baselinePrincipal || runtime.tradeBalanceTarget || 0);
  const capitalMode = state.normalizeCapitalMode(runtime.capitalMode);
  const simpleSlots = Math.max(1, Number(runtime.simpleSlots || 1));
  const availableTradableBalance = Math.max(0, accountValue);
  const currentPrincipal =
    capitalMode === "SIMPLE"
      ? Math.min(
          selectedPrincipal > 0 ? selectedPrincipal : availableTradableBalance,
          availableTradableBalance
        )
      : availableTradableBalance;
  const slotSize = capitalMode === "SIMPLE" ? currentPrincipal / simpleSlots : currentPrincipal;
  const maxActiveTrades = capitalMode === "COMPOUNDING" ? 1 : simpleSlots;
  const freeSlots = Math.max(0, maxActiveTrades - activeTradeCount);

  return {
    profileId,
    executionMode,
    capitalMode,
    accountValue: round(accountValue, 6),
    withdrawable: round(withdrawable, 6),
    availableTradableBalance: round(availableTradableBalance, 6),
    currentPrincipal: round(currentPrincipal, 6),
    tradePrincipal: round(currentPrincipal, 6),
    baselinePrincipal: round(selectedPrincipal, 6),
    selectedPrincipal: round(selectedPrincipal, 6),
    simpleSlots,
    slotSize: round(slotSize, 6),
    activeTradeCount,
    openSlots: activeTradeCount,
    freeSlots,
    maxActiveTrades,
    userState,
  };
}

function buildSizingContext(candidate, capitalSnapshot, runtime, marketPrice, szDecimals, allocationPlan) {
  const leverage = Number(runtime.tradeLeverage || config.defaultTradeLeverage || 10);
  const capitalMode = capitalSnapshot.capitalMode;
  const basePrincipal =
    capitalMode === "SIMPLE" ? capitalSnapshot.slotSize : capitalSnapshot.currentPrincipal;
  const principalUsed = Math.min(
    basePrincipal * Number(allocationPlan.allocationPct || 0),
    capitalSnapshot.withdrawable
  );
  const positionNotional = principalUsed * leverage;
  const quantity = roundDown(positionNotional / marketPrice, szDecimals);

  return {
    leverage,
    tradeBalanceTarget: Number(runtime.tradeBalanceTarget || 0),
    capitalMode,
    executionMode: capitalSnapshot.executionMode,
    tradeAccountValue: capitalSnapshot.accountValue,
    withdrawable: capitalSnapshot.withdrawable,
    availableTradableBalance: capitalSnapshot.availableTradableBalance,
    allocationPct: allocationPlan.allocationPct,
    repeatLevel: allocationPlan.repeatLevel,
    currentPrincipal: capitalSnapshot.currentPrincipal,
    simpleSlots: capitalSnapshot.simpleSlots,
    slotSize: capitalSnapshot.slotSize,
    principalUsed: round(principalUsed, 6),
    marginToUse: round(principalUsed, 6),
    positionNotional: round(positionNotional, 6),
    quantity,
  };
}

function createTrade(candidate, params) {
  const now = state.nowIso();
  return {
    id: `${params.profileId}-${candidate.pair}-${candidate.side}-${candidate.baseTimeframe}-${Date.now()}`,
    signalId: `${candidate.pair}-${candidate.side}-${candidate.baseTimeframe}-${Date.now()}`,
    signalKey: getSignalKey(candidate),
    idempotencyKey: params.idempotencyKey || buildIdempotencyKey(candidate, params.profileId),
    profileId: params.profileId,
    userProfileId: params.profileId,
    pair: String(candidate.pair || "").toUpperCase(),
    coin: String(params.coin || "").toUpperCase(),
    side: String(candidate.side || "LONG").toUpperCase(),
    baseTimeframe: candidate.baseTimeframe,
    supportTimeframes: uniqueStrings(candidate.supportTimeframes || candidate.supportTfs || []),
    supportTimeframeCount: uniqueStrings(candidate.supportTimeframes || candidate.supportTfs || []).length,
    score: Number(candidate.score || 0),
    strategyUsed:
      candidate.strategyUsed ||
      candidate.strategySource ||
      `${candidate.strategySourcePair || "N/A"} ${candidate.strategySourceTimeframe || ""}`.trim(),
    strategyBucket:
      candidate.strategyUsed ||
      `${String(candidate.pair || "").toUpperCase()}|${String(candidate.side || "").toUpperCase()}|${candidate.baseTimeframe || candidate.baseTf || "N/A"}`,
    reasons: candidate.reasons || [],
    referenceEntryPrice: Number(candidate.entryPrice || candidate.entry || 0),
    entryPrice: Number(params.marketPrice || candidate.entryPrice || candidate.entry || 0),
    targetPrice: Number(candidate.targetPrice || candidate.tp1 || candidate.target2Price || 0),
    stopLoss: Number(candidate.stopLoss || candidate.sl || candidate.sl2Price || 0),
    leverage: Number(params.leverage || 0),
    tradeBalanceTarget: Number(params.tradeBalanceTarget || 0),
    currentPrincipal: Number(params.currentPrincipal || 0),
    tradeAccountValue: Number(params.tradeAccountValue || 0),
    withdrawableAtEntry: Number(params.withdrawable || 0),
    principalUsed: Number(params.principalUsed || 0),
    slotSize: Number(params.slotSize || 0),
    simpleSlots: Number(params.simpleSlots || 1),
    allocationPct: Number(params.allocationPct || 0),
    repeatLevel: Number(params.repeatLevel || 0),
    quantity: Number(params.quantity || 0),
    plannedMargin: Number(params.marginToUse || 0),
    plannedNotional: Number(params.positionNotional || 0),
    positionNotional: Number(params.positionNotional || 0),
    capitalMode: params.capitalMode,
    executionMode: params.executionMode,
    signalMessageId: params.signalMessageId || null,
    messageId: params.signalMessageId || null,
    entryCloid: params.entryCloid || createCloid(),
    tpCloid: params.tpCloid || createCloid(),
    slCloid: params.slCloid || createCloid(),
    entryOrderId: null,
    tpOrderId: null,
    slOrderId: null,
    entryOrderStatus: "pending",
    tpOrderStatus: null,
    slOrderStatus: null,
    protectedQuantity: 0,
    entryExchangeResponse: null,
    protectiveExchangeResponse: null,
    entryFills: [],
    exitFills: [],
    filledQuantity: 0,
    exitQuantity: 0,
    exitPrice: null,
    grossPnl: 0,
    netPnl: 0,
    fees: 0,
    fundingImpact: 0,
    entryFee: 0,
    exitFee: 0,
    realizedPnl: 0,
    realizedPnlPct: 0,
    profitSweptAmount: 0,
    profitSweepResponse: null,
    exitReason: null,
    lastReconcileStatus: null,
    entryFilledNotified: false,
    protectiveOrdersPlaced: false,
    protectiveOrdersPlacedAt: null,
    status: "ENTRY_PENDING",
    phase: "ENTRY_PENDING",
    openedAt: now,
    entryTimeoutAt: new Date(Date.now() + Number(params.entryTimeoutMs || config.defaultEntryTimeoutMs)).toISOString(),
    filledAt: null,
    closedAt: null,
    createdAt: now,
    updatedAt: now,
  };
}

function appendEntryFills(trade, fills) {
  let changed = false;
  const existingKeys = new Set((trade.entryFills || []).map(buildFillKey));

  for (const fill of fills) {
    const normalized = normalizeFill(fill);
    const fillKey = buildFillKey(normalized);
    if (existingKeys.has(fillKey)) continue;
    trade.entryFills.push(normalized);
    existingKeys.add(fillKey);
    changed = true;
  }

  if (!changed) return false;

  const totalSize = trade.entryFills.reduce((sum, fill) => sum + Number(fill.sz || 0), 0);
  const totalNotional = trade.entryFills.reduce(
    (sum, fill) => sum + Number(fill.sz || 0) * Number(fill.px || 0),
    0
  );

  trade.filledQuantity = round(totalSize, 8);
  trade.entryPrice = totalSize > 0 ? round(totalNotional / totalSize, 8) : trade.entryPrice;
  trade.entryOrderId = trade.entryOrderId || Number(trade.entryFills[0]?.oid || 0);
  trade.entryFee = round(
    trade.entryFills.reduce((sum, fill) => sum + Number(fill.fee || 0), 0),
    6
  );
  trade.fees = round(Number(trade.entryFee || 0) + Number(trade.exitFee || 0), 6);
  trade.entryOrderStatus = totalSize < Number(trade.quantity || 0) ? "partial" : "filled";
  trade.status = totalSize < Number(trade.quantity || 0) ? "PARTIALLY_FILLED" : "OPEN";
  trade.phase = trade.status;
  trade.filledAt =
    trade.filledAt || new Date(Math.max(...trade.entryFills.map((fill) => fill.time))).toISOString();
  trade.updatedAt = state.nowIso();
  return true;
}

function finalizeClosedTrade(trade, exitReason) {
  trade.exitReason = exitReason;
  trade.status = "CLOSED";
  trade.phase = "CLOSED";
  trade.realizedPnl = round(Number(trade.netPnl || 0), 6);
  trade.realizedPnlPct = round(computeSignedPct(trade.side, trade.entryPrice, trade.exitPrice), 6);
  trade.closedAt = trade.closedAt || state.nowIso();
  trade.updatedAt = state.nowIso();
}

function appendExitFills(trade, fills, exitReason) {
  let changed = false;
  const existingKeys = new Set((trade.exitFills || []).map(buildFillKey));

  for (const fill of fills) {
    const normalized = normalizeFill(fill);
    const fillKey = buildFillKey(normalized);
    if (existingKeys.has(fillKey)) continue;
    trade.exitFills.push(normalized);
    existingKeys.add(fillKey);
    changed = true;
  }

  if (!changed) return false;

  const totalSize = trade.exitFills.reduce((sum, fill) => sum + Number(fill.sz || 0), 0);
  const totalNotional = trade.exitFills.reduce(
    (sum, fill) => sum + Number(fill.sz || 0) * Number(fill.px || 0),
    0
  );
  const exitPrice = totalSize > 0 ? totalNotional / totalSize : Number(trade.exitPrice || 0);

  trade.exitQuantity = round(totalSize, 8);
  trade.exitPrice = round(exitPrice, 8);
  trade.grossPnl = computeSignedAmount(trade.side, trade.entryPrice, exitPrice, totalSize);
  trade.exitFee = round(
    trade.exitFills.reduce((sum, fill) => sum + Number(fill.fee || 0), 0),
    6
  );
  trade.fees = round(Number(trade.entryFee || 0) + Number(trade.exitFee || 0), 6);
  trade.fundingImpact = round(
    Number(trade.fundingImpact || 0) +
      trade.exitFills.reduce((sum, fill) => sum + Number(fill.funding || 0), 0),
    6
  );
  trade.netPnl = round(Number(trade.grossPnl || 0) - Number(trade.fees || 0) + Number(trade.fundingImpact || 0), 6);
  finalizeClosedTrade(trade, exitReason);
  return true;
}

function updatePairStateAfterClose(profileId, trade) {
  state.savePairState(profileId, trade.pair, {
    profileId,
    pair: trade.pair,
    lastTradeId: trade.id,
    lastClosedAt: trade.closedAt || state.nowIso(),
    lastSide: trade.side,
    lastEntryPrice: Number(trade.entryPrice || trade.referenceEntryPrice || 0),
    repeatLevel: Number(trade.repeatLevel || 0),
    cooldownUntil: null,
    lastExitReason: trade.exitReason || null,
  });
}

async function ensureProtectiveOrders(trade, auth) {
  if (!trade?.filledQuantity || trade.executionMode !== "REAL") return [];
  if (!auth || !hyperliquid.isConfigured(auth)) return [];

  if (
    trade.protectiveOrdersPlaced &&
    Math.abs(Number(trade.protectedQuantity || 0) - Number(trade.filledQuantity || 0)) < 1e-8
  ) {
    return [];
  }

  if (trade.tpOrderId || trade.slOrderId) {
    const stale = [trade.tpOrderId, trade.slOrderId].filter(Boolean).map(Number);
    if (stale.length) {
      try {
        await hyperliquid.cancelOrders({ coin: trade.coin, oids: stale }, auth);
      } catch (error) {
        // Reconciliation will retry if exchange already cleared the orders.
      }
    }
  }

  const exitIsBuy = trade.side === "SHORT";
  const response = await hyperliquid.placeTpSl(
    {
      coin: trade.coin,
      isBuy: exitIsBuy,
      size: trade.filledQuantity,
      targetPx: trade.targetPrice,
      stopPx: trade.stopLoss,
      tpCloid: trade.tpCloid,
      slCloid: trade.slCloid,
      grouping: "positionTpsl",
    },
    auth
  );

  const statuses = parseOrderStatuses(response);
  const tpAck = statuses[0] || null;
  const slAck = statuses[1] || null;

  trade.protectiveExchangeResponse = response;
  trade.protectiveOrdersPlaced = true;
  trade.protectiveOrdersPlacedAt = state.nowIso();
  trade.protectedQuantity = Number(trade.filledQuantity || 0);
  trade.tpOrderId = tpAck?.oid || trade.tpOrderId;
  trade.slOrderId = slAck?.oid || trade.slOrderId;
  trade.tpOrderStatus = tpAck?.kind || trade.tpOrderStatus;
  trade.slOrderStatus = slAck?.kind || trade.slOrderStatus;
  trade.status = "PROTECTED";
  trade.phase = "PROTECTED";
  trade.updatedAt = state.nowIso();
  state.saveTrade(trade);

  return [
    {
      type: "PROTECTIVE_ORDERS_PLACED",
      trade: { ...trade },
      tpAck,
      slAck,
    },
  ];
}

function buildSyntheticFill({ oid, price, size, side, fee = 0, closedPnl = 0 }) {
  return {
    oid,
    px: price,
    sz: size,
    time: Date.now(),
    dir: side,
    hash: null,
    tid: null,
    fee,
    feeToken: "USDC",
    closedPnl,
  };
}

function updateDemoLedgerAfterClose(profileId, trade) {
  const runtime = state.getRuntimeSettings(profileId);
  const nextPerpBalance = round(Number(runtime.demoPerpBalance || 0) + Number(trade.netPnl || 0), 6);
  state.setDemoBalances(profileId, {
    perpBalance: nextPerpBalance,
    spotBalance: Number(runtime.demoSpotBalance || 0),
  });
  return 0;
}

async function maybeSweepProfitToSpot(profileId, auth) {
  return null;
}

function applyDemoOpen(trade) {
  const entryFee = demoFeeForNotional(Number(trade.plannedNotional || 0));
  appendEntryFills(trade, [
    buildSyntheticFill({
      oid: Number(Date.now()),
      price: trade.entryPrice,
      size: trade.quantity,
      side: trade.side === "LONG" ? "Open Long" : "Open Short",
      fee: entryFee,
    }),
  ]);
  trade.entryOrderStatus = "filled";
  trade.protectiveOrdersPlaced = true;
  trade.protectiveOrdersPlacedAt = state.nowIso();
  trade.protectedQuantity = Number(trade.filledQuantity || 0);
  trade.tpOrderStatus = "demo";
  trade.slOrderStatus = "demo";
  trade.status = "PROTECTED";
  trade.phase = "PROTECTED";
  state.saveTrade(trade);

  return [
    {
      type: "ORDER_PLACED",
      trade: { ...trade },
      ack: { oid: "DEMO", kind: "filled" },
    },
    {
      type: "ENTRY_FILLED",
      trade: { ...trade },
    },
    {
      type: "PROTECTIVE_ORDERS_PLACED",
      trade: { ...trade },
      tpAck: { oid: "DEMO-TP", kind: "demo" },
      slAck: { oid: "DEMO-SL", kind: "demo" },
    },
  ];
}

function findExistingTrackedSignal(profileId, pair, side, baseTimeframe) {
  return loadOpenPositions(profileId).find(
    (trade) =>
      trade.pair === String(pair || "").toUpperCase() &&
      trade.side === String(side || "").toUpperCase() &&
      trade.baseTimeframe === baseTimeframe
  );
}

function canOpenNewSignal(profileId, pair) {
  if (!pair) return true;
  return !state.findOpenTradeByPair(profileId, pair);
}

function buildSkippedSignalResult(candidate, reason, extra = {}) {
  return {
    ok: false,
    trade: extra.trade || null,
    reason,
    events: [
      {
        type: "TRADE_SKIPPED",
        trade: extra.trade || undefined,
        candidate,
        reason,
        ...extra,
      },
    ],
  };
}

function getSignalDispatchBudget(profileId) {
  const runtime = state.getRuntimeSettings(profileId);
  const activeTrades = loadOpenPositions(profileId).filter((trade) => isTradeOpen(trade));
  const maxActiveTrades =
    state.normalizeCapitalMode(runtime.capitalMode) === "COMPOUNDING"
      ? 1
      : Math.max(1, Number(runtime.simpleSlots || 1));

  return {
    profileId,
    capitalMode: state.normalizeCapitalMode(runtime.capitalMode),
    activeTradeCount: activeTrades.length,
    activePairs: uniqueStrings(activeTrades.map((trade) => trade.pair)),
    maxActiveTrades,
    freeSlots: Math.max(0, maxActiveTrades - activeTrades.length),
  };
}

async function previewSignalRegistration(candidate, options = {}) {
  const profileId = options.profileId || state.DEFAULT_PROFILE_ID;
  const signalKey = getSignalKey(candidate);
  const pair = String(candidate.pair || "").toUpperCase();
  const side = String(candidate.side || "LONG").toUpperCase();
  const signalMessageId = options.signalMessageId || candidate.signalMessageId || null;
  const profile = state.getProfileById(profileId, { includeSecret: true }) || { id: profileId };
  const runtime = state.getRuntimeSettings(profileId);

  const existing = state.findOpenTradeByPair(profileId, pair);
  if (existing) {
    return buildSkippedSignalResult(candidate, "pair-already-open", {
      trade: existing,
    });
  }

  if (!runtime.autoTradeEnabled) {
    return buildSkippedSignalResult(candidate, "autotrade-disabled");
  }

  const pairValidation = await pairUniverse.validatePair(pair).catch(() => ({
    ok: false,
    reason: "invalid-pair",
  }));
  if (!pairValidation.ok) {
    return buildSkippedSignalResult(
      candidate,
      pairValidation.reason === "not-listed-on-hyperliquid"
        ? "not-listed-on-hyperliquid"
        : "invalid-pair"
    );
  }

  let auth = null;
  if (runtime.executionMode === "REAL") {
    if (!isProfileApprovedForReal(profile)) {
      return buildSkippedSignalResult(candidate, "automation-not-approved");
    }
    auth = getProfileAuth(profileId);
    if (!hyperliquid.isConfigured(auth)) {
      return buildSkippedSignalResult(candidate, "secret-not-configured");
    }
  }

  const capitalSnapshot = await buildCapitalSnapshot(profileId, {
    executionMode: runtime.executionMode,
  });
  if (
    capitalSnapshot.freeSlots <= 0 ||
    capitalSnapshot.activeTradeCount >= capitalSnapshot.maxActiveTrades
  ) {
    return buildSkippedSignalResult(candidate, "slot-limit-reached");
  }

  const allocationPlan = buildAllocationPlan(candidate, profileId);
  if (!allocationPlan.accept) {
    return buildSkippedSignalResult(candidate, allocationPlan.reason, {
      cooldownUntil: allocationPlan.cooldownUntil || null,
    });
  }

  const allMids = options.allMids || (await hyperliquid.getAllMids());
  const marketPrice = Number(
    allMids?.[pairValidation.coin] || candidate.entryPrice || candidate.entry || 0
  );
  if (!Number.isFinite(marketPrice) || marketPrice <= 0) {
    return buildSkippedSignalResult(candidate, "missing-market-price");
  }

  const sizing = buildSizingContext(
    candidate,
    capitalSnapshot,
    runtime,
    marketPrice,
    pairValidation.meta?.szDecimals || 0,
    allocationPlan
  );

  if (!Number.isFinite(sizing.principalUsed) || sizing.principalUsed <= 0) {
    return buildSkippedSignalResult(
      candidate,
      capitalSnapshot.withdrawable <= 0 ? "insufficient-margin" : "insufficient-size"
    );
  }

  if (sizing.quantity <= 0 || sizing.positionNotional < MIN_NOTIONAL_USD) {
    return buildSkippedSignalResult(candidate, "insufficient-size", {
      plannedNotional: sizing.positionNotional,
    });
  }

  return {
    ok: true,
    profileId,
    profile,
    runtime,
    signalKey,
    pair,
    side,
    signalMessageId,
    pairValidation,
    capitalSnapshot,
    allocationPlan,
    marketPrice,
    sizing,
    auth,
    allMids,
  };
}

async function registerSignal(candidate, options = {}) {
  const prepared = options.prepared || (await previewSignalRegistration(candidate, options));
  if (!prepared.ok) {
    return {
      trade: prepared.trade || null,
      events: prepared.events || [],
    };
  }

  const profileId = prepared.profileId;
  const pair = prepared.pair;
  const side = prepared.side;
  const signalMessageId =
    options.signalMessageId || prepared.signalMessageId || candidate.signalMessageId || null;
  const runtime = prepared.runtime;
  const pairValidation = prepared.pairValidation;
  const sizing = prepared.sizing;
  const marketPrice = prepared.marketPrice;

  const trade = createTrade(candidate, {
    ...sizing,
    coin: pairValidation.coin,
    profileId,
    marketPrice,
    signalMessageId,
    entryTimeoutMs: runtime.entryTimeoutMs,
    idempotencyKey: buildIdempotencyKey(candidate, profileId),
  });
  state.saveTrade(trade);

  if (runtime.executionMode === "DEMO") {
    return {
      trade,
      events: applyDemoOpen(trade),
    };
  }

  const auth = prepared.auth || getProfileAuth(profileId);

  try {
    const entryResponse = await hyperliquid.placeEntry(
      {
        coin: pairValidation.coin,
        isBuy: side === "LONG",
        size: trade.quantity,
        leverage: trade.leverage,
        slippage: config.hyperliquidSlippage,
        cloid: trade.entryCloid,
      },
      auth
    );

    const statuses = parseOrderStatuses(entryResponse?.order || entryResponse);
    const primary = statuses[0] || null;

    trade.entryExchangeResponse = entryResponse;
    trade.entryOrderId = primary?.oid || trade.entryOrderId;
    trade.entryOrderStatus = primary?.kind || "unknown";
    trade.phase = primary?.kind === "filled" ? "OPEN" : "ENTRY_PLACED";
    trade.status = primary?.kind === "filled" ? "OPEN" : "ENTRY_PLACED";
    trade.updatedAt = state.nowIso();

    const events = [
      {
        type: primary?.kind === "error" ? "ORDER_REJECTED" : "ORDER_PLACED",
        trade: { ...trade },
        candidate,
        ack: primary,
      },
    ];

    if (primary?.kind === "error") {
      trade.status = "REJECTED";
      trade.phase = "REJECTED";
      trade.exitReason = primary.error || "entry-order-rejected";
      trade.closedAt = state.nowIso();
      state.saveTrade(trade);
      return { trade, events };
    }

    if (primary?.kind === "filled") {
      appendEntryFills(trade, [
        buildSyntheticFill({
          oid: primary.oid,
          price: primary.avgPx,
          size: primary.totalSz,
          side: side === "LONG" ? "Open Long" : "Open Short",
        }),
      ]);
      state.saveTrade(trade);
      events.push({
        type: "ENTRY_FILLED",
        trade: { ...trade },
      });
      const protectionEvents = await ensureProtectiveOrders(trade, auth);
      return {
        trade,
        events: [...events, ...protectionEvents],
      };
    }

    state.saveTrade(trade);
    return { trade, events };
  } catch (error) {
    trade.status = "REJECTED";
    trade.phase = "REJECTED";
    trade.exitReason = "entry-order-rejected";
    trade.lastError = error.message;
    trade.closedAt = state.nowIso();
    state.saveTrade(trade);
    return {
      trade,
      events: [
        {
          type: "ORDER_REJECTED",
          trade: { ...trade },
          candidate,
          reason: error.message,
        },
      ],
    };
  }
}

async function queryOrderStatusSafe(trade, auth) {
  try {
    return await hyperliquid.getOrderStatus(
      {
        oid: trade.entryOrderId,
        cloid: trade.entryCloid,
      },
      auth
    );
  } catch (error) {
    return { status: "error", error: error.message };
  }
}

async function maybeCancelPendingEntryBeforeFill(trade, marketPrice, auth) {
  if (!trade.entryOrderId || trade.filledAt) return null;
  const targetReached =
    trade.side === "LONG"
      ? Number(marketPrice || 0) >= Number(trade.targetPrice || 0)
      : Number(marketPrice || 0) <= Number(trade.targetPrice || 0);
  if (!targetReached) return null;

  try {
    await hyperliquid.cancelOrders(
      {
        coin: trade.coin,
        oids: [Number(trade.entryOrderId)],
      },
      auth
    );
  } catch (error) {
    const latest = await queryOrderStatusSafe(trade, auth);
    const status = normalizeOrderStatus(latest);
    if (status === "filled") {
      return null;
    }
  }

  trade.entryOrderStatus = "canceled";
  trade.status = "CANCELED";
  trade.phase = "CANCELED";
  trade.exitReason = "target-reached-before-entry-fill";
  trade.closedAt = state.nowIso();
  trade.updatedAt = state.nowIso();
  state.saveTrade(trade);

  return {
    type: "ENTRY_CANCELED_BEFORE_FILL",
    trade: { ...trade },
    reason: "target-reached-before-entry-fill",
  };
}

async function maybeCancelEntryOnTimeout(trade, auth) {
  if (!trade.entryOrderId || trade.filledAt) return null;
  const timeoutAt = new Date(trade.entryTimeoutAt || 0).getTime();
  if (!timeoutAt || timeoutAt > Date.now()) return null;

  try {
    await hyperliquid.cancelOrders(
      {
        coin: trade.coin,
        oids: [Number(trade.entryOrderId)],
      },
      auth
    );
  } catch (error) {
    const latest = await queryOrderStatusSafe(trade, auth);
    if (normalizeOrderStatus(latest) === "filled") {
      return null;
    }
  }

  trade.entryOrderStatus = "canceled";
  trade.status = "CANCELED";
  trade.phase = "CANCELED";
  trade.exitReason = "entry-timeout";
  trade.closedAt = state.nowIso();
  trade.updatedAt = state.nowIso();
  state.saveTrade(trade);

  return {
    type: "ENTRY_TIMEOUT",
    trade: { ...trade },
    reason: "entry-timeout",
  };
}

async function processEntryStatusFallback(trade, auth) {
  if (!trade.entryOrderId && !trade.entryCloid) return null;
  const orderStatus = await queryOrderStatusSafe(trade, auth);
  const status = normalizeOrderStatus(orderStatus);

  if (status === "open") {
    return null;
  }

  if (status === "filled") {
    trade.entryOrderStatus = status;
    state.saveTrade(trade);
    return {
      type: "ENTRY_AWAITING_FILLS",
      trade: { ...trade },
    };
  }

  if (TERMINAL_ORDER_STATUSES.has(status)) {
    trade.entryOrderStatus = status;
    trade.status = status === "canceled" || status.endsWith("Canceled") ? "CANCELED" : "REJECTED";
    trade.phase = trade.status;
    trade.exitReason = status === "canceled" ? "entry-order-canceled" : "entry-order-rejected";
    trade.closedAt = trade.closedAt || state.nowIso();
    state.saveTrade(trade);
    return {
      type: trade.status === "CANCELED" ? "ENTRY_CANCELED" : "ORDER_REJECTED",
      trade: { ...trade },
      reason: trade.exitReason,
    };
  }

  return null;
}

async function syncTradeFills(profileId, trades, auth) {
  if (!trades.length) return [];

  state.cleanupProcessedFills(profileId);
  const settings = state.getRuntimeSettings(profileId);
  const lookbackStart = Math.max(
    Number(settings.lastFillSyncAt || 0) - 60_000,
    Date.now() - Number(config.fillLookbackMs || 6 * 60 * 60 * 1000)
  );
  const fills = await hyperliquid.getUserFillsByTime(
    {
      startTime: lookbackStart,
      endTime: Date.now(),
      aggregateByTime: false,
    },
    auth
  );

  const relevantTrades = new Map(trades.map((trade) => [trade.id, trade]));
  const oidLookup = new Map();

  for (const trade of trades) {
    if (trade.entryOrderId) oidLookup.set(Number(trade.entryOrderId), { tradeId: trade.id, kind: "entry" });
    if (trade.tpOrderId) oidLookup.set(Number(trade.tpOrderId), { tradeId: trade.id, kind: "tp" });
    if (trade.slOrderId) oidLookup.set(Number(trade.slOrderId), { tradeId: trade.id, kind: "sl" });
  }

  const grouped = new Map();
  let maxFillTime = Number(settings.lastFillSyncAt || 0);

  for (const fill of fills || []) {
    const oid = Number(fill.oid);
    const match = oidLookup.get(oid);
    if (!match) continue;
    maxFillTime = Math.max(maxFillTime, Number(fill.time || 0));

    const fillKey = buildFillKey(fill);
    if (state.hasProcessedFill(profileId, fillKey)) continue;

    const trade = relevantTrades.get(match.tradeId);
    if (!trade) continue;

    const bucketKey = `${match.tradeId}:${match.kind}`;
    const bucket = grouped.get(bucketKey) || {
      trade,
      kind: match.kind,
      fills: [],
      fillKeys: [],
    };
    bucket.fills.push(fill);
    bucket.fillKeys.push(fillKey);
    grouped.set(bucketKey, bucket);
  }

  const events = [];

  for (const bucket of grouped.values()) {
    const trade = bucket.trade;

    if (bucket.kind === "entry") {
      const previousSize = Number(trade.filledQuantity || 0);
      const appended = appendEntryFills(trade, bucket.fills);
      if (appended) {
        state.saveTrade(trade);
        if (!previousSize) {
          events.push({
            type: "ENTRY_FILLED",
            trade: { ...trade },
          });
        } else if (Number(trade.filledQuantity || 0) > previousSize) {
          events.push({
            type: "ENTRY_FILLED",
            trade: { ...trade },
          });
        }
        const protectionEvents = await ensureProtectiveOrders(trade, auth);
        events.push(...protectionEvents);
      }
    } else {
      const exitReason = bucket.kind === "tp" ? "tp-hit" : "sl-hit";
      const appended = appendExitFills(trade, bucket.fills, exitReason);
      if (appended) {
        updatePairStateAfterClose(profileId, trade);
        state.saveTrade(trade);
        events.push({
          type: bucket.kind === "tp" ? "TARGET_HIT" : "STOP_HIT",
          trade: { ...trade },
        });
      }
    }

    for (const fillKey of bucket.fillKeys) {
      state.markProcessedFill(profileId, fillKey, {
        tradeId: trade.id,
        kind: bucket.kind,
      });
    }
  }

  state.setLastFillSyncAt(profileId, Math.max(maxFillTime, Date.now()));
  return events;
}

async function closeDemoTrade(profileId, trade, exitPrice, exitReason) {
  const closeTimestampMs = Date.now();
  const exitNotional = Number(exitPrice || 0) * Number(trade.filledQuantity || trade.quantity || 0);
  const exitFee = demoFeeForNotional(exitNotional);
  trade.exitFills.push(
    buildSyntheticFill({
      oid: Number(Date.now()),
      price: exitPrice,
      size: trade.filledQuantity || trade.quantity,
      side: exitReason === "tp-hit" ? "Close TP" : "Close SL",
      fee: exitFee,
    })
  );
  trade.exitQuantity = Number(trade.filledQuantity || trade.quantity || 0);
  trade.exitPrice = round(exitPrice, 8);
  trade.grossPnl = computeSignedAmount(
    trade.side,
    trade.entryPrice,
    trade.exitPrice,
    Number(trade.exitQuantity || 0)
  );
  trade.exitFee = exitFee;
  trade.fundingImpact = estimateFundingImpact(trade, closeTimestampMs);
  trade.fees = round(Number(trade.entryFee || 0) + Number(trade.exitFee || 0), 6);
  trade.netPnl = round(Number(trade.grossPnl || 0) - Number(trade.fees || 0) + Number(trade.fundingImpact || 0), 6);
  finalizeClosedTrade(trade, exitReason);
  const demoSweepAmount = updateDemoLedgerAfterClose(profileId, trade);
  updatePairStateAfterClose(profileId, trade);
  state.saveTrade(trade);
  return {
    type: exitReason === "tp-hit" ? "TARGET_HIT" : "STOP_HIT",
    trade: { ...trade },
    demoSweepAmount,
  };
}

async function syncDemoProfileTrades(profileId, trades, allMids) {
  const events = [];
  for (const trade of trades) {
    if (!isTradeOpen(trade)) continue;
    const marketPrice = Number(allMids?.[trade.coin] || 0);
    if (!Number.isFinite(marketPrice) || marketPrice <= 0) continue;

    const hitTarget =
      trade.side === "LONG"
        ? marketPrice >= Number(trade.targetPrice || 0)
        : marketPrice <= Number(trade.targetPrice || 0);
    const hitStop =
      trade.side === "LONG"
        ? marketPrice <= Number(trade.stopLoss || 0)
        : marketPrice >= Number(trade.stopLoss || 0);

    if (hitTarget) {
      events.push(await closeDemoTrade(profileId, trade, Number(trade.targetPrice || marketPrice), "tp-hit"));
      continue;
    }
    if (hitStop) {
      events.push(await closeDemoTrade(profileId, trade, Number(trade.stopLoss || marketPrice), "sl-hit"));
    }
  }
  return events;
}

async function syncRealProfileTrades(profileId, trades) {
  const auth = getProfileAuth(profileId);
  if (!hyperliquid.isConfigured(auth)) return [];

  const events = [];
  const allMids = await hyperliquid.getAllMids();
  const fillEvents = await syncTradeFills(profileId, trades, auth);
  events.push(...fillEvents);

  for (const trade of state.listOpenTrades(profileId)) {
    if (trade.executionMode !== "REAL") continue;

    if ((trade.status === "ENTRY_PLACED" || trade.status === "ENTRY_PENDING") && !trade.filledAt) {
      const price = Number(allMids?.[trade.coin] || 0);
      const targetCancel = await maybeCancelPendingEntryBeforeFill(trade, price, auth);
      if (targetCancel) {
        events.push(targetCancel);
        continue;
      }
      const timeoutCancel = await maybeCancelEntryOnTimeout(trade, auth);
      if (timeoutCancel) {
        events.push(timeoutCancel);
        continue;
      }
      const fallbackEvent = await processEntryStatusFallback(trade, auth);
      if (fallbackEvent) events.push(fallbackEvent);
    }

    if ((trade.status === "OPEN" || trade.status === "PARTIALLY_FILLED" || trade.status === "PROTECTED") && trade.filledAt) {
      const protectionEvents = await ensureProtectiveOrders(trade, auth);
      events.push(...protectionEvents);
    }
  }

  return events;
}

async function syncTrades(options = {}) {
  const profileId = options.profileId || null;
  const openTrades = loadOpenPositions(profileId);
  if (!openTrades.length) return [];

  const byProfile = new Map();
  for (const trade of openTrades) {
    const key = trade.profileId || state.DEFAULT_PROFILE_ID;
    const bucket = byProfile.get(key) || [];
    bucket.push(trade);
    byProfile.set(key, bucket);
  }

  const allMids = await hyperliquid.getAllMids().catch(() => null);
  const events = [];

  for (const [currentProfileId, trades] of byProfile.entries()) {
    const demoTrades = trades.filter((trade) => trade.executionMode === "DEMO");
    const realTrades = trades.filter((trade) => trade.executionMode === "REAL");

    if (demoTrades.length) {
      events.push(...(await syncDemoProfileTrades(currentProfileId, demoTrades, allMids)));
    }
    if (realTrades.length) {
      events.push(...(await syncRealProfileTrades(currentProfileId, realTrades)));
    }
  }

  return events;
}

function computeDrawdown(trades) {
  let equity = 0;
  let peak = 0;
  let maxDrawdown = 0;
  for (const trade of trades) {
    equity += Number(trade.netPnl || trade.realizedPnl || 0);
    peak = Math.max(peak, equity);
    maxDrawdown = Math.min(maxDrawdown, equity - peak);
  }
  return round(Math.abs(maxDrawdown), 6);
}

function summarizeTrades(trades) {
  const ordered = [...trades].sort((a, b) => {
    const aTime = new Date(a.closedAt || a.updatedAt || a.createdAt || 0).getTime();
    const bTime = new Date(b.closedAt || b.updatedAt || b.createdAt || 0).getTime();
    return aTime - bTime;
  });
  const totalTrades = ordered.length;
  const closed = ordered.filter((trade) => state.TERMINAL_TRADE_STATUSES.includes(String(trade.status || "").toUpperCase()));
  const wins = closed.filter((trade) => Number(trade.netPnl || trade.realizedPnl || 0) > 0).length;
  const losses = closed.filter((trade) => Number(trade.netPnl || trade.realizedPnl || 0) < 0).length;
  const rejected = closed.filter((trade) => ["REJECTED", "CANCELED", "ORPHANED"].includes(String(trade.status || "").toUpperCase())).length;
  const grossPnl = round(closed.reduce((sum, trade) => sum + Number(trade.grossPnl || 0), 0), 6);
  const netPnl = round(closed.reduce((sum, trade) => sum + Number(trade.netPnl || trade.realizedPnl || 0), 0), 6);
  const fees = round(closed.reduce((sum, trade) => sum + Number(trade.fees || 0), 0), 6);
  const funding = round(closed.reduce((sum, trade) => sum + Number(trade.fundingImpact || 0), 0), 6);
  const avgPnl = totalTrades ? round(netPnl / totalTrades, 6) : 0;

  const bucketScores = new Map();
  for (const trade of closed) {
    const bucket = trade.strategyBucket || trade.strategyUsed || "unknown";
    bucketScores.set(bucket, Number(bucketScores.get(bucket) || 0) + Number(trade.netPnl || 0));
  }

  let bestStrategyBucket = "N/A";
  let bestStrategyScore = Number.NEGATIVE_INFINITY;
  for (const [bucket, score] of bucketScores.entries()) {
    if (score > bestStrategyScore) {
      bestStrategyBucket = bucket;
      bestStrategyScore = score;
    }
  }

  return {
    totalTrades,
    wins,
    losses,
    rejected,
    winRate: totalTrades ? round((wins / totalTrades) * 100, 2) : 0,
    lossRate: totalTrades ? round((losses / totalTrades) * 100, 2) : 0,
    grossPnl,
    netPnl,
    fees,
    funding,
    avgPnl,
    drawdown: computeDrawdown(closed),
    bestStrategyBucket,
  };
}

function filterTrades(trades, filters = {}) {
  return trades.filter((trade) => {
    if (filters.executionMode && trade.executionMode !== filters.executionMode) return false;
    if (filters.capitalMode && trade.capitalMode !== filters.capitalMode) return false;
    return true;
  });
}

function pnlSummary(options = {}) {
  const profileId = options.profileId || null;
  const executionMode = options.executionMode || null;
  const capitalMode = options.capitalMode || null;
  const openTrades = filterTrades(loadOpenPositions(profileId), { executionMode, capitalMode });
  const closedTrades = filterTrades(
    profileId ? state.listClosedTradesByProfile(profileId) : state.listClosedTrades(),
    { executionMode, capitalMode }
  );
  const allTrades = [...openTrades, ...closedTrades];

  return {
    openCount: openTrades.length,
    closedCount: closedTrades.length,
    openPairs: uniqueStrings(openTrades.map((trade) => trade.pair)),
    realized: round(allTrades.reduce((sum, trade) => sum + Number(trade.netPnl || trade.realizedPnl || 0), 0), 6),
    summary: summarizeTrades(allTrades),
    settings: profileId ? state.getRuntimeSettings(profileId) : null,
    profileId,
    executionMode,
    capitalMode,
  };
}

function pnlModelSummary(options = {}) {
  return summarizeTrades(filterTrades(getAllTrades(options.profileId || null), options));
}

async function getCapitalStatus(profileId) {
  const profile = state.getProfileById(profileId, { includeSecret: true }) || { id: profileId };
  const runtime = state.getRuntimeSettings(profileId);
  const capitalSnapshot = await buildCapitalSnapshot(profileId, {
    executionMode: runtime.executionMode,
  }).catch(() => ({
    capitalMode: runtime.capitalMode,
    executionMode: runtime.executionMode,
    accountValue: runtime.executionMode === "DEMO" ? runtime.demoPerpBalance : 0,
    withdrawable: runtime.executionMode === "DEMO" ? runtime.demoPerpBalance : 0,
    availableTradableBalance: runtime.executionMode === "DEMO" ? runtime.demoPerpBalance : 0,
    currentPrincipal:
      runtime.capitalMode === "SIMPLE"
        ? Math.min(runtime.demoPerpBalance, runtime.baselinePrincipal)
        : runtime.demoPerpBalance,
    tradePrincipal:
      runtime.capitalMode === "SIMPLE"
        ? Math.min(runtime.demoPerpBalance, runtime.baselinePrincipal)
        : runtime.demoPerpBalance,
    baselinePrincipal: runtime.baselinePrincipal,
    selectedPrincipal: runtime.baselinePrincipal,
    simpleSlots: runtime.simpleSlots,
    slotSize:
      runtime.capitalMode === "SIMPLE"
        ? Math.min(runtime.demoPerpBalance, runtime.baselinePrincipal) /
          Math.max(1, runtime.simpleSlots)
        : runtime.demoPerpBalance,
    activeTradeCount: getActiveTradeCount(profileId),
    freeSlots:
      runtime.capitalMode === "COMPOUNDING"
        ? Math.max(0, 1 - getActiveTradeCount(profileId))
        : Math.max(0, runtime.simpleSlots - getActiveTradeCount(profileId)),
    maxActiveTrades: runtime.capitalMode === "COMPOUNDING" ? 1 : runtime.simpleSlots,
  }));

  return {
    profile,
    runtime,
    capitalSnapshot,
    pnl: pnlSummary({ profileId }),
    lastReconcile: state.getRecentReconciliation(profileId),
  };
}

function findProtectionOrder(openOrders, trade) {
  return openOrders.filter((order) => {
    if (order.coin !== trade.coin) return false;
    if (trade.tpOrderId && Number(order.oid) === Number(trade.tpOrderId)) return true;
    if (trade.slOrderId && Number(order.oid) === Number(trade.slOrderId)) return true;
    if (trade.tpCloid && order.cloid === trade.tpCloid) return true;
    if (trade.slCloid && order.cloid === trade.slCloid) return true;
    return false;
  });
}

async function reconcileProfile(profileId, options = {}) {
  const profile = state.getProfileById(profileId, { includeSecret: true });
  if (!profile) {
    return {
      profileId,
      unresolved: ["invalid-user-profile"],
    };
  }

  const summary = {
    profileId,
    reconciledTrades: 0,
    missingProtectionsRepaired: 0,
    staleOrdersCanceled: 0,
    orphanRecordsFound: 0,
    unresolved: [],
  };

  const runtime = state.getRuntimeSettings(profileId);
  if (runtime.executionMode === "DEMO") {
    const demoSummary = {
      ...summary,
      note: "demo-profile",
    };
    state.recordReconciliation(profileId, demoSummary);
    return demoSummary;
  }

  const auth = getProfileAuth(profileId);
  if (!hyperliquid.hasUserContext(auth)) {
    const invalid = {
      ...summary,
      unresolved: ["invalid-user-profile"],
    };
    state.recordReconciliation(profileId, invalid);
    return invalid;
  }

  const [openOrdersRaw, userState] = await Promise.all([
    hyperliquid.getOpenOrders(auth).catch(() => []),
    hyperliquid.getUserState(auth).catch(() => null),
  ]);
  const openOrders = normalizeOpenOrders(openOrdersRaw);
  const openPositionSize = getOpenPositionSizeMap(userState);
  const localTrades = loadOpenPositions(profileId);
  const events = await syncRealProfileTrades(profileId, localTrades);
  summary.reconciledTrades += localTrades.length;

  for (const trade of state.listOpenTrades(profileId)) {
    const positionSize = Math.abs(Number(openPositionSize.get(trade.coin) || 0));
    const protections = findProtectionOrder(openOrders, trade);

    if ((trade.status === "OPEN" || trade.status === "PARTIALLY_FILLED" || trade.status === "PROTECTED") && positionSize > 0 && !protections.length) {
      try {
        const protectionEvents = await ensureProtectiveOrders(trade, auth);
        if (protectionEvents.length) {
          summary.missingProtectionsRepaired += 1;
        }
      } catch (error) {
        summary.unresolved.push(`${trade.pair}: missing protection repair failed`);
      }
    }

    if ((trade.status === "CANCELED" || trade.status === "CLOSED" || trade.status === "REJECTED") && protections.length) {
      try {
        await hyperliquid.cancelOrders(
          {
            coin: trade.coin,
            oids: protections.map((item) => Number(item.oid)).filter(Boolean),
          },
          auth
        );
        summary.staleOrdersCanceled += protections.length;
      } catch (error) {
        summary.unresolved.push(`${trade.pair}: stale order cancel failed`);
      }
    }

    if ((trade.status === "ENTRY_PLACED" || trade.status === "ENTRY_PENDING") && !positionSize && !protections.length && !trade.filledAt) {
      summary.orphanRecordsFound += 1;
      trade.status = "ORPHANED";
      trade.phase = "ORPHANED";
      trade.exitReason = trade.exitReason || "reconciliation-orphan";
      trade.closedAt = trade.closedAt || state.nowIso();
      state.saveTrade(trade);
    }
  }

  state.recordReconciliation(profileId, {
    ...summary,
    events: events.map((event) => event.type),
  });
  return summary;
}

async function reconcileAllProfiles() {
  const profiles = state.listProfiles({ includeDisabled: false });
  const summaries = [];
  for (const profile of profiles) {
    summaries.push(await reconcileProfile(profile.id));
  }
  return summaries;
}

module.exports = {
  loadOpenPositions,
  saveOpenPositions,
  loadClosedTrades,
  saveClosedTrades,
  getAllTrades,
  registerSignal,
  attachSignalMessage(signalId, messageId, signalKey) {
    const trade = state.getTradeById(signalId);
    if (!trade) return null;
    trade.signalMessageId = messageId || trade.signalMessageId;
    trade.messageId = messageId || trade.messageId;
    trade.signalKey = signalKey || trade.signalKey;
    return state.saveTrade(trade);
  },
  syncTrades,
  reconcileProfile,
  reconcileAllProfiles,
  pnlSummary,
  pnlModelSummary,
  getCapitalStatus,
  canOpenNewSignal,
  findExistingTrackedSignal,
  findExistingOpen: findExistingTrackedSignal,
  getSignalDispatchBudget,
  previewSignalRegistration,
  buildAllocationPlan,
};
