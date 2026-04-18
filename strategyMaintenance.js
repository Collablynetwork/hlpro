const fs = require("fs");
const path = require("path");
const config = require("./config");
const state = require("./state");
const { round } = require("./indicators");

function strategyBucketForStrategy(strategy) {
  return [
    String(strategy.pair || "").toUpperCase(),
    String(strategy.direction || "").toUpperCase(),
    String(strategy.mainSourceTimeframe || strategy.timeframe || "N/A"),
  ].join("|");
}

function strategySignature(strategy) {
  return [
    String(strategy.pair || "").toUpperCase(),
    String(strategy.direction || "").toUpperCase(),
    String(strategy.mainSourceTimeframe || strategy.timeframe || "N/A"),
  ].join("|");
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
  return Math.abs(maxDrawdown);
}

function sampleWeight(count) {
  return count / (count + 5);
}

function recencyWeight(strategy, retentionDays) {
  const eventMs = new Date(strategy.eventTime || strategy.detectedAt || Date.now()).getTime();
  if (!Number.isFinite(eventMs)) return 0;
  const ageDays = Math.max(0, (Date.now() - eventMs) / (24 * 60 * 60 * 1000));
  return Math.max(0, 1 - ageDays / Math.max(1, retentionDays));
}

function rankStrategies(strategies, closedTrades, settings) {
  const retentionDays = Math.max(1, Number(settings.strategyRetentionDays || 7));
  return strategies.map((strategy) => {
    const bucket = strategyBucketForStrategy(strategy);
    const matches = closedTrades.filter((trade) => {
      const tradeBucket = String(trade.strategyBucket || "").toUpperCase();
      return (
        tradeBucket === bucket ||
        (
          String(trade.pair || "").toUpperCase() === String(strategy.pair || "").toUpperCase() &&
          String(trade.side || "").toLowerCase() === String(strategy.direction || "").toLowerCase()
        )
      );
    });
    const wins = matches.filter((trade) => Number(trade.netPnl || trade.realizedPnl || 0) > 0).length;
    const total = matches.length;
    const netPnl = matches.reduce((sum, trade) => sum + Number(trade.netPnl || trade.realizedPnl || 0), 0);
    const drawdown = computeDrawdown(matches);
    const riskAdjusted = netPnl / Math.max(1, drawdown || 1);
    const weightedScore =
      sampleWeight(total) * (wins / Math.max(1, total)) * 100 +
      sampleWeight(total) * netPnl * 0.4 +
      sampleWeight(total) * riskAdjusted * 0.2 +
      recencyWeight(strategy, retentionDays) * 20 -
      drawdown * 0.15;

    return {
      strategy,
      signature: strategySignature(strategy),
      bucket,
      total,
      wins,
      netPnl: round(netPnl, 6),
      drawdown: round(drawdown, 6),
      weightedScore: round(weightedScore, 6),
    };
  });
}

function exportPruneArchive(ranked, removed) {
  state.ensureDir(config.exportsDir);
  const filePath = path.join(config.exportsDir, `strategy-prune-${Date.now()}.txt`);
  const payload = {
    generatedAt: new Date().toISOString(),
    kept: ranked.slice(0, 25).map((item) => ({
      id: item.strategy.id,
      pair: item.strategy.pair,
      direction: item.strategy.direction,
      timeframe: item.strategy.mainSourceTimeframe,
      weightedScore: item.weightedScore,
      trades: item.total,
      netPnl: item.netPnl,
    })),
    removed: removed.slice(0, 200).map((item) => ({
      id: item.strategy.id,
      pair: item.strategy.pair,
      direction: item.strategy.direction,
      timeframe: item.strategy.mainSourceTimeframe,
      weightedScore: item.weightedScore,
    })),
  };
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8");
  return filePath;
}

function getStrategyStatus(profileId = state.DEFAULT_PROFILE_ID) {
  const settings = state.getStrategyRuntimeSettings(profileId);
  return {
    totalStrategies: state.loadStrategiesIndex().length,
    strategyCap: Number(settings.strategyCap || config.strategyCap),
    strategyRetentionDays: Number(settings.strategyRetentionDays || config.strategyRetentionDays),
    lastStrategyPruneAt: settings.lastStrategyPruneAt || null,
  };
}

function getTopStrategies(limit = 10, profileId = state.DEFAULT_PROFILE_ID) {
  const settings = state.getStrategyRuntimeSettings(profileId);
  const cutoffMs = Date.now() - Number(settings.strategyRetentionDays || 7) * 24 * 60 * 60 * 1000;
  const trades = state
    .listClosedTrades()
    .filter((trade) => new Date(trade.closedAt || trade.updatedAt || 0).getTime() >= cutoffMs);
  const ranked = rankStrategies(state.loadStrategies(), trades, settings);
  return ranked.sort((a, b) => b.weightedScore - a.weightedScore).slice(0, limit);
}

function pruneStrategies(options = {}) {
  const profileId = options.profileId || state.DEFAULT_PROFILE_ID;
  const settings = state.getStrategyRuntimeSettings(profileId);
  const cap = Math.max(1, Number(settings.strategyCap || config.strategyCap));
  const retentionDays = Math.max(1, Number(settings.strategyRetentionDays || config.strategyRetentionDays));
  const cutoffMs = Date.now() - retentionDays * 24 * 60 * 60 * 1000;
  const strategies = state.loadStrategies();
  const closedTrades = state
    .listClosedTrades()
    .filter((trade) => new Date(trade.closedAt || trade.updatedAt || 0).getTime() >= cutoffMs);
  const ranked = rankStrategies(strategies, closedTrades, settings).sort(
    (a, b) => b.weightedScore - a.weightedScore
  );

  const kept = [];
  const removed = [];
  const perSignatureCount = new Map();
  for (const item of ranked) {
    const signatureCount = Number(perSignatureCount.get(item.signature) || 0);
    if (kept.length < cap && signatureCount < 3) {
      kept.push(item);
      perSignatureCount.set(item.signature, signatureCount + 1);
    } else {
      removed.push(item);
    }
  }

  if (removed.length) {
    state.deleteStrategiesByIds(removed.map((item) => item.strategy.id));
  }
  state.setLastStrategyPruneAt(new Date().toISOString());
  const archivePath = exportPruneArchive(kept, removed);

  return {
    totalBefore: strategies.length,
    kept: kept.length,
    removed: removed.length,
    cap,
    retentionDays,
    archivePath,
    top: kept.slice(0, 20),
  };
}

function shouldRunScheduledPrune(profileId = state.DEFAULT_PROFILE_ID) {
  const status = getStrategyStatus(profileId);
  const lastRunMs = status.lastStrategyPruneAt
    ? new Date(status.lastStrategyPruneAt).getTime()
    : 0;
  return Date.now() - lastRunMs >= Number(config.strategyPruneIntervalMs || 7 * 24 * 60 * 60 * 1000);
}

module.exports = {
  getStrategyStatus,
  getTopStrategies,
  pruneStrategies,
  shouldRunScheduledPrune,
};
