const fs = require("fs");
const path = require("path");
const config = require("./config");
const state = require("./state");

function safeName(value) {
  return String(value || "").replace(/[^a-zA-Z0-9_-]/g, "_");
}

function normalizeDirection(direction) {
  return String(direction || "").toLowerCase() === "short" ? "short" : "long";
}

function readJsonSafe(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, "utf8");
    if (!raw.trim()) return fallback;
    return JSON.parse(raw);
  } catch (error) {
    return fallback;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function getMainSourceTimeframe(strategyOrEvent) {
  return (
    strategyOrEvent?.mainSourceTimeframe ||
    strategyOrEvent?.timeframe ||
    strategyOrEvent?.fingerprint?.timeframe ||
    strategyOrEvent?.sourceTimeframes?.[0] ||
    null
  );
}

function getSavedTimeframes(strategyOrEvent) {
  if (Array.isArray(strategyOrEvent?.savedTimeframes) && strategyOrEvent.savedTimeframes.length) {
    return strategyOrEvent.savedTimeframes;
  }

  if (strategyOrEvent?.allTimeframes && typeof strategyOrEvent.allTimeframes === "object") {
    return Object.keys(strategyOrEvent.allTimeframes);
  }

  if (Array.isArray(strategyOrEvent?.sourceTimeframes) && strategyOrEvent.sourceTimeframes.length) {
    return strategyOrEvent.sourceTimeframes;
  }

  const tf = getMainSourceTimeframe(strategyOrEvent);
  return tf ? [tf] : [];
}

function buildStrategyFileName(event) {
  const ts = new Date(event.timestamp || Date.now()).toISOString().replace(/[:.]/g, "-");
  const pair = safeName(event.pair || "UNKNOWN");
  const tf = safeName(getMainSourceTimeframe(event) || "unknown");
  const kind = normalizeDirection(event.direction) === "long" ? "bullish_pump" : "bearish_dump";

  return `${ts}_${pair}_${tf}_${kind}.json`;
}

function buildExplanation(event) {
  const f = event.features || {};
  const savedTfs = getSavedTimeframes(event).join(", ") || "none";
  const supportTfs = (event.supportingTimeframes || []).join(", ") || "none";
  const side = normalizeDirection(event.direction) === "long" ? "bullish" : "bearish";

  return [
    `This ${side} strategy was learned from ${event.pair} on ${new Date(event.timestamp).toISOString()}.`,
    `Main source timeframe was ${getMainSourceTimeframe(event) || "n/a"}.`,
    `Saved timeframes: ${savedTfs}.`,
    `Supporting timeframes: ${supportTfs}.`,
    `BB width percentile=${f.bbWidthPercentile ?? "n/a"}, MACD line=${f.macdLine ?? "n/a"}, MACD signal=${f.macdSignal ?? "n/a"}, histogram slope=${f.macdHistogramSlope ?? "n/a"}, ADX=${f.adx ?? "n/a"}, ADX slope=${f.adxSlope ?? "n/a"}, DI spread=${f.diSpread ?? "n/a"}, volume/avg20=${f.volumeVsAvg20 ?? "n/a"}, quote-volume/avg20=${f.quoteVolumeVsAvg20 ?? "n/a"}.`,
    "This record stores trigger conditions, flow state, all timeframe snapshots, and reusable fingerprint data.",
  ].join(" ");
}

function eventToStrategy(event, learnedWeights = {}) {
  const direction = normalizeDirection(event.direction);
  const mainSourceTimeframe = getMainSourceTimeframe(event);
  const savedTimeframes = getSavedTimeframes(event);

  return {
    id: `${event.pair}-${direction}-${mainSourceTimeframe}-${new Date(event.timestamp).getTime()}`,
    pair: event.pair,
    direction,
    detectedAt: nowIso(),
    eventTime: event.timestamp,
    fileName: buildStrategyFileName(event),

    mainSourceTimeframe,
    sourceTimeframes: mainSourceTimeframe ? [mainSourceTimeframe] : [],
    savedTimeframes,
    supportingTimeframes: event.supportingTimeframes || [],

    sourcePumpWindow: event.sourceWindow || null,
    sourceWindow: event.sourceWindow || null,

    indicatorsUsed: [
      "Bollinger Bands",
      "MACD",
      "ADX",
      "DI+/DI-",
      "Volume",
      "Quote Volume",
      "Open Interest",
      "Funding",
      "Taker Buy/Sell Ratio",
      "Structure / BOS",
      "Support / Resistance",
    ],

    prePumpFeatures: event.features || {},
    triggerFeatures: {
      bbWidthPercentile: event.features?.bbWidthPercentile ?? null,
      bbWidth: event.features?.bbWidth ?? null,
      bbBasis: event.features?.bbBasis ?? null,
      bbUpper: event.features?.bbUpper ?? null,
      bbLower: event.features?.bbLower ?? null,

      macdLine: event.features?.macdLine ?? null,
      macdSignal: event.features?.macdSignal ?? null,
      macdHistogram: event.features?.macdHistogram ?? null,
      macdHistogramSlope: event.features?.macdHistogramSlope ?? null,
      macdBullCross: event.features?.macdBullCross ?? null,
      macdBearCross: event.features?.macdBearCross ?? null,
      macdAboveZero: event.features?.macdAboveZero ?? null,
      macdBelowZero: event.features?.macdBelowZero ?? null,
      macdZeroDistancePct: event.features?.macdZeroDistancePct ?? null,

      adx: event.features?.adx ?? null,
      adxSlope: event.features?.adxSlope ?? null,
      plusDI: event.features?.plusDI ?? null,
      minusDI: event.features?.minusDI ?? null,
      diSpread: event.features?.diSpread ?? null,

      volumeVsAvg20: event.features?.volumeVsAvg20 ?? null,
      volumeVsAvg50: event.features?.volumeVsAvg50 ?? null,
      quoteVolumeVsAvg20: event.features?.quoteVolumeVsAvg20 ?? null,

      bullishBos: event.features?.bullishBos ?? null,
      bearishBos: event.features?.bearishBos ?? null,
      trend: event.features?.trend ?? null,
      support: event.features?.support ?? null,
      resistance: event.features?.resistance ?? null,
      rangePositionPct: event.features?.rangePositionPct ?? null,

      triggerPrice: event.features?.triggerPrice ?? null,
      resultPrice: event.features?.resultPrice ?? null,
      triggerTime: event.features?.triggerTime ?? null,
      moveTime: event.features?.moveTime ?? null,
    },

    flowFeatures: {
      fundingRate: event.flow?.fundingRate ?? null,
      openInterest: event.flow?.openInterest ?? null,
      openInterestChangePct: event.flow?.openInterestChangePct ?? null,
      takerBuySellRatio: event.flow?.takerBuySellRatio ?? null,
    },

    higherTimeframeSupport: event.regimeSupport || {},
    regimeSupportScore: event.regimeSupportScore ?? null,

    resultingExpansionPct: event.movePct ?? null,
    postAnalysisSummary: `${event.pair} moved ${event.movePct ?? "n/a"}% on ${mainSourceTimeframe || "n/a"} after the detected setup.`,
    learnedWeights,
    reusableStrategyExplanation: buildExplanation(event),

    fingerprint: {
      timeframe: mainSourceTimeframe,
      direction,
      features: event.features || {},
      flow: event.flow || {},
      supportingTimeframes: event.supportingTimeframes || [],
    },

    allTimeframes: event.allTimeframes || {},
  };
}

function summarizeStrategy(strategy) {
  return state.buildStrategySummary(strategy);
}

function loadStrategiesIndex() {
  return state.loadStrategiesIndex();
}

function saveStrategy(strategy) {
  return state.saveStrategy(strategy);
}

function rebuildStrategiesIndexFromFiles() {
  if (!fs.existsSync(config.strategiesDir)) {
    return loadStrategiesIndex();
  }

  const files = fs
    .readdirSync(config.strategiesDir)
    .filter((file) => file.endsWith(".json") && file !== "index.json");

  for (const fileName of files) {
    const strategy = readJsonSafe(path.join(config.strategiesDir, fileName), null);
    if (!strategy?.id) continue;

    saveStrategy({
      ...strategy,
      fileName: strategy.fileName || fileName,
      mainSourceTimeframe: getMainSourceTimeframe(strategy),
      savedTimeframes: getSavedTimeframes(strategy),
    });
  }

  return loadStrategiesIndex();
}

function learnAndPersist(events, learnedWeights = {}) {
  const existing = new Set(loadStrategiesIndex().map((item) => item.id));
  const saved = [];

  for (const event of events || []) {
    const strategy = eventToStrategy(event, learnedWeights);
    if (!existing.has(strategy.id)) {
      saveStrategy(strategy);
      saved.push(strategy);
      existing.add(strategy.id);
    }
  }

  return saved;
}

function loadStrategies() {
  return state.loadStrategies();
}

function getStrategyByPair(pair) {
  return state.getStrategyByPair(pair);
}

module.exports = {
  buildExplanation,
  eventToStrategy,
  summarizeStrategy,
  loadStrategiesIndex,
  saveStrategy,
  rebuildStrategiesIndexFromFiles,
  learnAndPersist,
  loadStrategies,
  getStrategyByPair,
  getMainSourceTimeframe,
  getSavedTimeframes,
};
