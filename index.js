const config = require("./config");
const state = require("./state");
const binance = require("./binance");
const pairUniverse = require("./pairUniverse");
const { buildFeatureSnapshot, round } = require("./indicators");
const { detectStructure } = require("./structure");
const { buildDynamicWeights } = require("./weights");
const { findRecentPumpLeaders } = require("./pumpDetector");
const {
  learnAndPersist,
  loadStrategies,
  rebuildStrategiesIndexFromFiles,
} = require("./strategyLearner");
const { matchStrategiesForPair } = require("./similarity");
const signalEngine = require("./signals");
const dryrun = require("./dryrun");
const telegram = require("./telegram");
const strategyMaintenance = require("./strategyMaintenance");

state.ensureStorage();

let bot = null;
let bootstrapPromise = null;
let validSymbolsSet = null;
let scanLock = false;
let tradeSyncLock = false;

function ensureBotStarted() {
  if (bot) return bot;

  bot = telegram.createBot({ polling: config.telegramPolling });

  if (bot) {
    telegram.registerHandlers(bot, { runScan, syncTradeUpdates });
  }

  return bot;
}

function timeframeToFlowPeriod(timeframe) {
  const allowed = new Set(config.supportedFlowPeriods);
  if (allowed.has(timeframe)) return timeframe;
  if (["1m", "3m"].includes(timeframe)) return "5m";
  if (timeframe === "8h") return "6h";
  if (timeframe === "3d" || timeframe === "1w") return "1d";
  return "1h";
}

function getActiveScanIntervals() {
  const supported = new Set(config.supportedKlineIntervals || []);
  const configured = Array.isArray(config.scanKlineIntervals)
    ? config.scanKlineIntervals
    : config.supportedKlineIntervals;
  const filtered = configured.filter((timeframe) => supported.has(timeframe));
  return filtered.length ? filtered : config.supportedKlineIntervals;
}

function isRateLimitError(error) {
  return Number(error?.response?.status) === 429 || /\b429\b/.test(String(error?.message || ""));
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let index = 0;

  async function runner() {
    while (true) {
      const current = index;
      index += 1;
      if (current >= items.length) break;
      results[current] = await worker(items[current], current);
    }
  }

  const size = Math.max(1, Math.min(limit || 1, items.length || 1));
  await Promise.all(Array.from({ length: size }, () => runner()));
  return results;
}

function uniqueUpper(values) {
  return [...new Set((values || []).map((value) => String(value).trim().toUpperCase()).filter(Boolean))];
}

function getScanTargets(options = {}) {
  if (options.profileId) {
    const profile = state.getProfileById(options.profileId);
    return profile ? [profile] : [];
  }

  const profiles = state
    .listProfiles({ includeDisabled: false })
    .filter((profile) => profile.status === "ACTIVE" && profile.chatId);
  const userProfiles = profiles.filter((profile) => profile.id !== state.DEFAULT_PROFILE_ID);
  const targetProfiles = userProfiles.length ? userProfiles : profiles;

  if (targetProfiles.length) {
    const byChat = new Map();

    const priority = (profile) => {
      let score = 0;
      if (profile.role === "SYSTEM") score += 100;
      if (profile.automationStatus === "APPROVED") score += 20;
      if (profile.automationEnabled) score += 10;
      return score;
    };

    for (const profile of targetProfiles) {
      const key = String(profile.chatId || "");
      const existing = byChat.get(key);
      if (!existing || priority(profile) > priority(existing)) {
        byChat.set(key, profile);
      }
    }

    return [...byChat.values()];
  }

  const fallback = state.getProfileById(state.DEFAULT_PROFILE_ID);
  return fallback?.chatId ? [fallback] : [];
}

function applyUniverseCap(allPairs, watchedPairs) {
  const cap = Number(config.maxScanPairs || 0);
  const prioritizeWatchedPairs =
    String(config.prioritizeWatchedPairs ?? "true").toLowerCase() === "true";

  if (!cap || cap <= 0 || allPairs.length <= cap) return allPairs;
  if (!prioritizeWatchedPairs) return allPairs.slice(0, cap);

  const watchedSet = new Set(watchedPairs);
  const priority = [];
  const rest = [];

  for (const pair of allPairs) {
    if (watchedSet.has(pair)) priority.push(pair);
    else rest.push(pair);
  }

  return [...priority, ...rest].slice(0, cap);
}

async function resolveScanUniverse(options = {}) {
  const targets = getScanTargets(options);
  const watchedPairs = uniqueUpper(
    targets.flatMap((profile) => state.getWatchedPairs(profile.id))
  );

  if (!validSymbolsSet || options.forceReloadPairs) {
    validSymbolsSet = await pairUniverse.getTradablePairSet({
      force: Boolean(options.forceReloadPairs),
    });
  }

  const watchedValid = uniqueUpper(watchedPairs.filter((pair) => validSymbolsSet.has(pair)));
  const finalPairs = applyUniverseCap(watchedValid, watchedValid);

  return {
    targets,
    watchedPairs,
    watchedValid,
    scanPairs: finalPairs,
  };
}

async function buildTimeframePayload(symbol, timeframe) {
  const candles = await binance.getKlines(symbol, timeframe, config.maxKlinesPerRequest);
  const features = buildFeatureSnapshot(candles);
  const structure = detectStructure(candles);
  const flowPeriod = timeframeToFlowPeriod(timeframe);

  let fundingRate = 0;
  let openInterest = null;
  let openInterestChangePct = 0;
  let takerBuySellRatio = 1;
  let book = null;

  try {
    const [mark, oiNow, oiHist, takerHist, funding, bookTicker] = await Promise.all([
      binance.getMarkPrice(symbol),
      binance.getOpenInterest(symbol),
      binance.getOpenInterestHist(symbol, flowPeriod, 20).catch(() => []),
      binance.getTakerLongShortRatio(symbol, flowPeriod, 20).catch(() => []),
      binance.getFundingRate(symbol, 5).catch(() => []),
      binance.getBookTicker(symbol).catch(() => null),
    ]);

    features.currentClose = Number(mark.markPrice || mark.indexPrice || features.currentClose);
    openInterest = Number(oiNow.openInterest || 0);

    if (oiHist.length >= 2) {
      const start = Number(oiHist[0].sumOpenInterestValue || oiHist[0].sumOpenInterest || 0);
      const end = Number(
        oiHist[oiHist.length - 1].sumOpenInterestValue ||
          oiHist[oiHist.length - 1].sumOpenInterest ||
          0
      );
      openInterestChangePct = start ? ((end - start) / start) * 100 : 0;
    }

    if (takerHist.length) {
      const last = takerHist[takerHist.length - 1];
      takerBuySellRatio = Number(last.buySellRatio || 1);
    }

    if (funding.length) {
      fundingRate = Number(funding[funding.length - 1].fundingRate || 0);
    }

    if (bookTicker) {
      const bid = Number(bookTicker.bidPrice || 0);
      const ask = Number(bookTicker.askPrice || 0);

      book = {
        bid,
        ask,
        spreadPct: bid > 0 ? ((ask - bid) / bid) * 100 : 0,
        bidQty: Number(bookTicker.bidQty || 0),
        askQty: Number(bookTicker.askQty || 0),
      };
    }
  } catch (error) {
    console.error(`Flow build error ${symbol} ${timeframe}:`, error.message);
  }

  return {
    candles,
    features: {
      ...features,
      ...structure,
      openInterest,
      spreadPct: book?.spreadPct ?? null,
    },
    flow: {
      fundingRate,
      openInterest,
      openInterestChangePct: round(openInterestChangePct, 4),
      takerBuySellRatio: round(takerBuySellRatio, 4),
    },
    book,
  };
}

async function buildFeatureStoreForPairs(pairs) {
  const featureStore = {};
  const scanIntervals = config.supportedKlineIntervals || [];
  let rateLimited = false;

  await mapLimit(pairs, Number(config.maxParallelRequests || 2), async (pair) => {
    featureStore[pair] = {};

    for (const timeframe of scanIntervals) {
      if (rateLimited) break;

      try {
        featureStore[pair][timeframe] = await buildTimeframePayload(pair, timeframe);
      } catch (error) {
        const message = error?.message || String(error);
        console.error(`Timeframe build skipped ${pair} ${timeframe}:`, message);
        featureStore[pair][timeframe] = null;

        if (isRateLimitError(error)) {
          rateLimited = true;
          break;
        }
      }
    }
  });

  return featureStore;
}

function recentEventsOnly(events) {
  const threeDaysAgo = Date.now() - 3 * 24 * 60 * 60 * 1000;
  return events.filter((event) => new Date(event.timestamp).getTime() >= threeDaysAgo);
}

function extractRegimeSupport(tfMap, direction) {
  const regimeTfs = ["1d", "3d", "1w"];
  let score = 0;

  for (const tf of regimeTfs) {
    const features = tfMap?.[tf]?.features;
    if (!features) continue;

    if (direction === "long") {
      if (features.trend === "uptrend") score += 0.4;
      if ((features.rangePositionPct ?? 50) > 55) score += 0.2;
      if ((features.diSpread ?? 0) > 0) score += 0.2;
    } else {
      if (features.trend === "downtrend") score += 0.4;
      if ((features.rangePositionPct ?? 50) < 45) score += 0.2;
      if ((features.diSpread ?? 0) < 0) score += 0.2;
    }
  }

  return Math.min(1, score);
}

function buildRegimeSupportDetail(tfMap, direction) {
  const regimeTfs = ["1d", "3d", "1w"];
  const out = {};

  for (const tf of regimeTfs) {
    const features = tfMap?.[tf]?.features || {};
    out[tf] = {
      trend: features.trend ?? null,
      bullishBos: features.bullishBos ?? null,
      bearishBos: features.bearishBos ?? null,
      rangePositionPct: features.rangePositionPct ?? null,
      diSpread: features.diSpread ?? null,
      directionSupported:
        direction === "long"
          ? features.trend === "uptrend" || (features.diSpread ?? 0) > 0
          : features.trend === "downtrend" || (features.diSpread ?? 0) < 0,
    };
  }

  return out;
}

function buildAllTimeframeSnapshot(tfMap) {
  const snapshot = {};

  for (const [timeframe, payload] of Object.entries(tfMap || {})) {
    const features = payload?.features || {};
    const flow = payload?.flow || {};

    snapshot[timeframe] = {
      currentClose: features.currentClose ?? null,
      trend: features.trend ?? null,
      bullishBos: features.bullishBos ?? null,
      bearishBos: features.bearishBos ?? null,
      support: features.support ?? null,
      resistance: features.resistance ?? null,
      bbWidthPercentile: features.bbWidthPercentile ?? null,
      bbWidth: features.bbWidth ?? null,
      bbBasis: features.bbBasis ?? null,
      bbUpper: features.bbUpper ?? null,
      bbLower: features.bbLower ?? null,
      macdLine: features.macdLine ?? null,
      macdSignal: features.macdSignal ?? null,
      macdHistogram: features.macdHistogram ?? null,
      macdHistogramSlope: features.macdHistogramSlope ?? null,
      macdBullCross: features.macdBullCross ?? null,
      macdBearCross: features.macdBearCross ?? null,
      macdAboveZero: features.macdAboveZero ?? null,
      macdBelowZero: features.macdBelowZero ?? null,
      adx: features.adx ?? null,
      adxSlope: features.adxSlope ?? null,
      plusDI: features.plusDI ?? null,
      minusDI: features.minusDI ?? null,
      diSpread: features.diSpread ?? null,
      volumeVsAvg20: features.volumeVsAvg20 ?? null,
      volumeVsAvg50: features.volumeVsAvg50 ?? null,
      quoteVolumeVsAvg20: features.quoteVolumeVsAvg20 ?? null,
      rangePositionPct: features.rangePositionPct ?? null,
      openInterest: flow.openInterest ?? null,
      openInterestChangePct: flow.openInterestChangePct ?? null,
      takerBuySellRatio: flow.takerBuySellRatio ?? null,
      fundingRate: flow.fundingRate ?? null,
    };
  }

  return snapshot;
}

function findSupportingTimeframes(tfMap, baseTimeframe, direction) {
  const map = config.timeframeHierarchyMap || {};
  const stack = map[baseTimeframe] || [baseTimeframe];
  const out = [];

  for (const tf of stack) {
    const features = tfMap?.[tf]?.features;
    if (!features) continue;

    if (direction === "long") {
      const supported =
        features.trend === "uptrend" ||
        features.bullishBos ||
        (features.diSpread ?? 0) > 0 ||
        ((features.macdLine ?? 0) >= (features.macdSignal ?? 0) &&
          (features.macdHistogramSlope ?? 0) >= 0);

      if (supported) out.push(tf);
    } else {
      const supported =
        features.trend === "downtrend" ||
        features.bearishBos ||
        (features.diSpread ?? 0) < 0 ||
        ((features.macdLine ?? 0) <= (features.macdSignal ?? 0) &&
          (features.macdHistogramSlope ?? 0) <= 0);

      if (supported) out.push(tf);
    }
  }

  return [...new Set(out)];
}

function dedupeCandidates(candidates) {
  const map = new Map();

  for (const candidate of candidates) {
    const key = signalEngine.buildSignalKey(candidate);
    const existing = map.get(key);
    if (!existing || Number(candidate.score || 0) > Number(existing.score || 0)) {
      map.set(key, candidate);
    }
  }

  return [...map.values()].sort((a, b) => b.score - a.score);
}

async function maybeRunScheduledPrune() {
  if (!strategyMaintenance.shouldRunScheduledPrune()) return null;
  return strategyMaintenance.pruneStrategies();
}

async function runScan(options = {}) {
  if (scanLock) {
    return {
      skipped: true,
      pairsChecked: 0,
      candidates: 0,
      topCandidates: [],
    };
  }

  scanLock = true;

  try {
    const { targets, watchedPairs, watchedValid, scanPairs } = await resolveScanUniverse(options);
    const scanStartedAt = new Date().toISOString();

    if (!scanPairs.length) {
      const emptySummary = {
        skipped: false,
        pairsChecked: 0,
        candidates: 0,
        watchedPairs: watchedPairs.length,
        watchedValid: watchedValid.length,
        scannedUniverse: 0,
        topCandidates: [],
        profiles: targets.length,
      };
      state.setSnapshot("system:lastScan", {
        timestamp: scanStartedAt,
        profileIds: targets.map((profile) => profile.id),
        summary: emptySummary,
      });
      for (const target of targets) {
        state.setSnapshot(`profile:${target.id}:lastScan`, {
          timestamp: scanStartedAt,
          summary: emptySummary,
        });
      }
      return emptySummary;
    }

    const featureStore = await buildFeatureStoreForPairs(scanPairs);

    let recentLeaders = recentEventsOnly(findRecentPumpLeaders(featureStore));
    recentLeaders = recentLeaders.map((event) => ({
      ...event,
      regimeSupportScore: extractRegimeSupport(featureStore[event.pair], event.direction),
      regimeSupport: buildRegimeSupportDetail(featureStore[event.pair], event.direction),
      allTimeframes: buildAllTimeframeSnapshot(featureStore[event.pair]),
      supportingTimeframes: findSupportingTimeframes(
        featureStore[event.pair],
        event.timeframe,
        event.direction
      ),
    }));

    const learnedWeights = buildDynamicWeights(recentLeaders);
    state.writeJson(config.learnedPumpsPath, recentLeaders);

    const newStrategies = learnAndPersist(recentLeaders, learnedWeights);
    const strategies = loadStrategies();
    const candidates = [];

    for (const pair of scanPairs) {
      const match = matchStrategiesForPair(pair, featureStore[pair], strategies);

      const longSupport = match.long
        ? findSupportingTimeframes(featureStore[pair], match.long.baseTimeframe, "long")
        : [];
      const shortSupport = match.short
        ? findSupportingTimeframes(featureStore[pair], match.short.baseTimeframe, "short")
        : [];

      const longSignal = signalEngine.buildSignalCandidate({
        ...match.long,
        supportTimeframes: longSupport,
      });
      const shortSignal = signalEngine.buildSignalCandidate({
        ...match.short,
        supportTimeframes: shortSupport,
      });

      if (longSignal) {
        longSignal.isWatchedPair = watchedValid.includes(pair);
        longSignal.discoveryType = "watched";
        candidates.push(longSignal);
      }

      if (shortSignal) {
        shortSignal.isWatchedPair = watchedValid.includes(pair);
        shortSignal.discoveryType = "watched";
        candidates.push(shortSignal);
      }
    }

    const topCandidates = dedupeCandidates(candidates).slice(0, 25);

    state.writeJson(config.scoreStatePath, {
      generatedAt: new Date().toISOString(),
      watchedPairs,
      watchedValid,
      scannedUniverse: scanPairs.length,
      candidates: topCandidates,
    });

    if (!options.suppressSignals) {
      for (const target of targets) {
        const profilePairs = new Set(state.getWatchedPairs(target.id));
        const filteredCandidates = topCandidates.filter((candidate) => profilePairs.has(candidate.pair));
        if (!filteredCandidates.length) continue;
        await signalEngine.dispatchSignals(
          bot,
          options.chatId || target.chatId || config.telegramChatId,
          filteredCandidates,
          { profileId: target.id }
        );
      }
    }

    await syncTradeUpdates();
    await maybeRunScheduledPrune().catch((error) => {
      console.error("Scheduled strategy prune failed:", error.message);
    });

    const summary = {
      skipped: false,
      watchedPairs: watchedPairs.length,
      watchedValid: watchedValid.length,
      scannedUniverse: scanPairs.length,
      pairsChecked: scanPairs.length,
      candidates: topCandidates.length,
      learnedStrategies: newStrategies.length,
      topCandidates,
      profiles: targets.length,
    };
    state.setSnapshot("system:lastScan", {
      timestamp: scanStartedAt,
      profileIds: targets.map((profile) => profile.id),
      summary,
    });
    state.setSnapshot("system:lastScanResult", summary);
    for (const target of targets) {
      state.setSnapshot(`profile:${target.id}:lastScan`, {
        timestamp: scanStartedAt,
        summary,
      });
      state.setSnapshot(`profile:${target.id}:lastScanResult`, summary);
    }
    return summary;
  } catch (error) {
    console.error("runScan error:", error);
    const failure = {
      skipped: false,
      pairsChecked: 0,
      candidates: 0,
      topCandidates: [],
      error: error.message,
    };
    state.setSnapshot("system:lastError", {
      timestamp: new Date().toISOString(),
      message: error.message,
      scope: "runScan",
    });
    if (options.profileId) {
      state.setSnapshot(`profile:${options.profileId}:lastError`, {
        timestamp: new Date().toISOString(),
        message: error.message,
        scope: "runScan",
      });
    }
    return failure;
  } finally {
    scanLock = false;
  }
}

async function syncTradeUpdates() {
  if (tradeSyncLock) return [];
  tradeSyncLock = true;

  try {
    const updates = await dryrun.syncTrades();
    if (!updates.length) return [];

    const grouped = new Map();
    for (const update of updates) {
      const profileId = update.trade?.profileId || update.summary?.profileId || state.DEFAULT_PROFILE_ID;
      const bucket = grouped.get(profileId) || [];
      bucket.push(update);
      grouped.set(profileId, bucket);
    }

    for (const [profileId, profileUpdates] of grouped.entries()) {
      const profile = state.getProfileById(profileId) || state.getProfileById(state.DEFAULT_PROFILE_ID);
      const chatId = profile?.chatId || config.telegramChatId;
      if (!chatId) continue;
      await signalEngine.dispatchTradeUpdates(bot, chatId, profileUpdates, { profileId });
    }

    return updates;
  } catch (error) {
    console.error("syncTradeUpdates error:", error.message);
    state.setSnapshot("system:lastError", {
      timestamp: new Date().toISOString(),
      message: error.message,
      scope: "syncTradeUpdates",
    });
    return [];
  } finally {
    tradeSyncLock = false;
  }
}

async function bootstrap() {
  if (bootstrapPromise) return bootstrapPromise;

  bootstrapPromise = (async () => {
    if (typeof rebuildStrategiesIndexFromFiles === "function") {
      rebuildStrategiesIndexFromFiles();
    } else {
      console.warn("rebuildStrategiesIndexFromFiles export missing, skipping index rebuild.");
    }

    const startedBot = ensureBotStarted();

    if (startedBot && typeof telegram.setupCommands === "function") {
      await telegram.setupCommands(startedBot);
      console.log("Telegram command menu registered.");
    }

    try {
      await binance.ping();
      console.log("Connected to Hyperliquid public market data");
    } catch (error) {
      console.error("Hyperliquid market-data ping failed:", error.message);
    }

    try {
      await pairUniverse.refreshMetadata({ force: true });
    } catch (error) {
      console.error("Initial pair metadata refresh failed:", error.message);
    }

    try {
      const reconcileSummaries = await dryrun.reconcileAllProfiles();
      if (reconcileSummaries.length) {
        console.log("Startup reconciliation summaries:", reconcileSummaries);
      }
    } catch (error) {
      console.error("Startup reconciliation failed:", error.message);
    }

    const first = await runScan();
    console.log("Initial scan summary:", first);
    await syncTradeUpdates();

    setInterval(() => {
      runScan().then((summary) => {
        console.log("Scheduled scan summary:", summary);
      });
    }, config.scanEveryMs);

    setInterval(() => {
      syncTradeUpdates().then((updates) => {
        if (updates.length) {
          console.log("Trade sync events:", updates.map((event) => event.type));
        }
      });
    }, config.tradeMonitorMs);

    return { ok: true };
  })();

  try {
    return await bootstrapPromise;
  } catch (error) {
    bootstrapPromise = null;
    throw error;
  }
}

if (require.main === module) {
  bootstrap().catch((error) => {
    console.error("Bootstrap failed:", error);
    process.exit(1);
  });
}

module.exports = {
  runScan,
  bootstrap,
  resolveScanUniverse,
  findSupportingTimeframes,
  syncTradeUpdates,
};
